/*
 * pg_sync.c
 *
 */


#include "postgres_fe.h"
#include "lib/stringinfo.h"
#include "common/fe_memutils.h"

#include "libpq-fe.h"

#include "access/transam.h"
#include "libpq/pqformat.h"
#include "pg_logicaldecode.h"
#include "pqexpbuffer.h"
#include "pgsync.h"
#include "nodes/pg_list.h"

#include <time.h>

#ifndef WIN32
#include <zlib.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/time.h>
#endif

#ifndef WIN32

#include <sys/time.h>

typedef struct timeval TimevalStruct;

#define GETTIMEOFDAY(T) gettimeofday(T, NULL)
#define DIFF_MSEC(T, U, res) \
do { \
	res = ((((int) ((T)->tv_sec - (U)->tv_sec)) * 1000000.0 + \
	  ((int) ((T)->tv_usec - (U)->tv_usec))) / 1000.0); \
} while(0)
#else

#include <sys/types.h>
#include <sys/timeb.h>
#include <windows.h>

typedef LARGE_INTEGER TimevalStruct;
#define GETTIMEOFDAY(T) QueryPerformanceCounter(T)
#define DIFF_MSEC(T, U, res) \
do { \
	LARGE_INTEGER frq; 						\
											\
	QueryPerformanceFrequency(&frq); 		\
	res = (double)(((T)->QuadPart - (U)->QuadPart)/(double)frq.QuadPart); \
	res *= 1000; \
} while(0)
#endif

#ifdef WIN32
typedef CRITICAL_SECTION pthread_mutex_t;
typedef HANDLE	ThreadHandle;
typedef DWORD	ThreadId;
typedef unsigned		thid_t;

typedef struct Thread
{
	ThreadHandle		os_handle;
	thid_t				thid;
}Thread;

typedef CRITICAL_SECTION pthread_mutex_t;
typedef DWORD		 pthread_t;
#define pthread_mutex_lock(A)	 (EnterCriticalSection(A),0)
#define pthread_mutex_trylock(A) win_pthread_mutex_trylock((A))
#define pthread_mutex_unlock(A)  (LeaveCriticalSection(A), 0)
#define pthread_mutex_init(A,B)  (InitializeCriticalSection(A),0)
#define pthread_mutex_lock(A)	 (EnterCriticalSection(A),0)
#define pthread_mutex_trylock(A) win_pthread_mutex_trylock((A))
#define pthread_mutex_unlock(A)  (LeaveCriticalSection(A), 0)
#define pthread_mutex_destroy(A) (DeleteCriticalSection(A), 0)


#else
typedef pthread_t	ThreadHandle;
typedef pthread_t	ThreadId;
typedef pthread_t	thid_t;

typedef struct Thread
{
	pthread_t	 os_handle;
} Thread;

#define SIGALRM				14
#endif 

typedef struct ThreadArg
{
	int			id;
	long		count;
	bool		all_ok;
	PGconn		*from;
	PGconn		*to;

	struct Thread_hd *hd;
}ThreadArg;

typedef struct Thread_hd
{
	int			nth;
	struct ThreadArg *th;
	
	const char *snapshot;
	char		*src;
	char		*desc;
	char		*local;

	int			ntask;
	struct Task_hd		*task;
	struct Task_hd		*l_task;
	pthread_mutex_t	t_lock;

	int			ntask_com;
	struct Task_hd		*task_com;
	pthread_mutex_t	t_lock_com;
}Thread_hd;

typedef struct Task_hd
{
	int			id;
	char	   *schemaname;		/* the schema name, or NULL */
	char	   *relname;		/* the relation/sequence name */
	long		count;
	bool		complete;

	struct Task_hd *next;
}Task_hd;

static XLogRecPtr pg_lsn_in(char *lsn);
static PGconn *pglogical_connect(const char *connstring, const char *connname);
static int start_copy_origin_tx(PGconn *conn, const char *snapshot);
static int start_copy_target_tx(PGconn *conn);
static int finish_copy_origin_tx(PGconn *conn);
static int finish_copy_target_tx(PGconn *conn);
static void *copy_table_data(void *arg);
static int ThreadCreate(Thread *th, void *(*start)(void *arg), void *arg);
static bool WaitThreadEnd(int n, Thread *th);
static void ThreadExit(int code);
static PGconn *pglogical_connect_replica(const char *connstring, const char *connname);



/*
 * Ensure slot exists.
 */
char *
ensure_replication_slot_snapshot(PGconn *origin_conn, char *slot_name,
								 XLogRecPtr *lsn)
{
	PGresult	   *res;
	StringInfoData	query;
	char		   *snapshot;
	char			*slsn;

	initStringInfo(&query);

	appendStringInfo(&query, "CREATE_REPLICATION_SLOT \"%s\" LOGICAL %s",
					 slot_name, "ali_decoding");

	res = PQexec(origin_conn, query.data);

	/* TODO: check and handle already existing slot. */
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "could not send replication command \"%s\": status %s: %s\n",
			 query.data,
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));

		return NULL;
	}

	slsn = PQgetvalue(res, 0, 1);
	*lsn = pg_lsn_in(slsn);
	
	snapshot = pstrdup(PQgetvalue(res, 0, 2));

	PQclear(res);

	return snapshot;
}


/*----------------------------------------------------------
 * Formatting and conversion routines.
 *---------------------------------------------------------*/

XLogRecPtr
pg_lsn_in(char *lsn)
{
#define MAXPG_LSNCOMPONENT	8

	char	   *str = lsn;
	int			len1,
				len2;
	uint32		id,
				off;
	XLogRecPtr	result = 0;

	/* Sanity check input format. */
	len1 = strspn(str, "0123456789abcdefABCDEF");
	if (len1 < 1 || len1 > MAXPG_LSNCOMPONENT || str[len1] != '/')
	{
		fprintf(stderr, "invalid input syntax for type pg_lsn: \"%s\"", str);

		return result;
	}
	len2 = strspn(str + len1 + 1, "0123456789abcdefABCDEF");
	if (len2 < 1 || len2 > MAXPG_LSNCOMPONENT || str[len1 + 1 + len2] != '\0')
	{
		fprintf(stderr,"invalid input syntax for type pg_lsn: \"%s\"", str);

		return result;
	}

	/* Decode result. */
	id = (uint32) strtoul(str, NULL, 16);
	off = (uint32) strtoul(str + len1 + 1, NULL, 16);
	result = ((uint64) id << 32) | off;

	return result;
}

/*
 * Transaction management for COPY.
 */
static int
start_copy_origin_tx(PGconn *conn, const char *snapshot)
{
	PGresult	   *res;
	const char	   *setup_query =
		"BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;\n"
		"SET DATESTYLE = ISO;\n"
		"SET INTERVALSTYLE = POSTGRES;\n"
		"SET extra_float_digits TO 3;\n"
		"SET statement_timeout = 0;\n"
		"SET lock_timeout = 0;\n";
	StringInfoData	query;

	initStringInfo(&query);
	appendStringInfoString(&query, setup_query);

	if (snapshot)
		appendStringInfo(&query, "SET TRANSACTION SNAPSHOT '%s';\n", snapshot);

	res = PQexec(conn, query.data);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "BEGIN on origin node failed: %s",
				PQresultErrorMessage(res));
		return 1;
	}

	PQclear(res);
	return 0;
}

static int
start_copy_target_tx(PGconn *conn)
{
	PGresult	   *res;
	const char	   *setup_query =
		"BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;\n"
		"SET session_replication_role = 'replica';\n"
		"SET DATESTYLE = ISO;\n"
		"SET INTERVALSTYLE = POSTGRES;\n"
		"SET extra_float_digits TO 3;\n"
		"SET statement_timeout = 0;\n"
		"SET lock_timeout = 0;\n";

	res = PQexec(conn, setup_query);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "BEGIN on target node failed: %s",
				PQresultErrorMessage(res));
		return 1;
	}

	PQclear(res);
	return 0;
}

static int
finish_copy_origin_tx(PGconn *conn)
{
	PGresult   *res;

	/* Close the  transaction and connection on origin node. */
	res = PQexec(conn, "ROLLBACK");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "ROLLBACK on origin node failed: %s",
				PQresultErrorMessage(res));
		return 1;
	}

	PQclear(res);
	//PQfinish(conn);
	return 0;
}

static int
finish_copy_target_tx(PGconn *conn)
{
	PGresult   *res;

	/* Close the transaction and connection on target node. */
	res = PQexec(conn, "COMMIT");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "COMMIT on target node failed: %s",
				PQresultErrorMessage(res));
		return 1;
	}

	PQclear(res);
	//PQfinish(conn);
	return 0;
}

/*
 * COPY single table over wire.
 */
static void *
copy_table_data(void *arg)
{
	ThreadArg *args = (ThreadArg *)arg;
	Thread_hd *hd = args->hd;
	PGresult   *res1;
	PGresult   *res2;
	int			bytes;
	char	   *copybuf;
	StringInfoData	query;
	char *nspname;
	char *relname;
	Task_hd 	*curr = NULL;
	TimevalStruct before,
					after; 
	double		elapsed_msec = 0;

	PGconn *origin_conn = args->from;
	PGconn *target_conn = args->to;

	origin_conn = pglogical_connect(hd->src, EXTENSION_NAME "_copy");
	if (origin_conn == NULL)
	{
		fprintf(stderr, "init src conn failed: %s", PQerrorMessage(origin_conn));
		return NULL;
	}
	
	target_conn = pglogical_connect(hd->desc, EXTENSION_NAME "_copy");
	if (target_conn == NULL)
	{
		fprintf(stderr, "init desc conn failed: %s", PQerrorMessage(target_conn));
		return NULL;
	}
	
	initStringInfo(&query);
	while(1)
	{
		int			nlist = 0;

		GETTIMEOFDAY(&before);
		pthread_mutex_lock(&hd->t_lock);
		nlist = hd->ntask;
		if (nlist == 1)
		{
		  curr = hd->l_task;
		  hd->l_task = NULL;
		  hd->ntask = 0;
		}
		else if (nlist > 1)
		{
		  Task_hd	*tmp = hd->l_task->next;
		  curr = hd->l_task;
		  hd->l_task = tmp;
		  hd->ntask--;
		}
		else
		{
			curr = NULL;
		}

		pthread_mutex_unlock(&hd->t_lock);

		if(curr == NULL)
		{
			break;
		}

		start_copy_origin_tx(origin_conn, hd->snapshot);
		start_copy_target_tx(target_conn);

		nspname = curr->schemaname;
		relname = curr->relname;

		/* Build COPY TO query. */
		appendStringInfo(&query, "COPY %s.%s TO stdout",
						 PQescapeIdentifier(origin_conn, nspname,
											strlen(nspname)),
						 PQescapeIdentifier(origin_conn, relname,
											strlen(relname)));

		/* Execute COPY TO. */
		res1 = PQexec(origin_conn, query.data);
		if (PQresultStatus(res1) != PGRES_COPY_OUT)
		{
			fprintf(stderr,"table copy failed Query '%s': %s", 
					query.data, PQerrorMessage(origin_conn));
			goto exit;
		}

		/* Build COPY FROM query. */
		resetStringInfo(&query);
		appendStringInfo(&query, "COPY %s.%s FROM stdin",
						 PQescapeIdentifier(target_conn, nspname,
											strlen(nspname)),
						 PQescapeIdentifier(target_conn, relname,
											strlen(relname)));

		/* Execute COPY FROM. */
		res2 = PQexec(target_conn, query.data);
		if (PQresultStatus(res2) != PGRES_COPY_IN)
		{
			fprintf(stderr,"table copy failed Query '%s': %s", 
				query.data, PQerrorMessage(target_conn));
			goto exit;
		}

		while ((bytes = PQgetCopyData(origin_conn, &copybuf, false)) > 0)
		{
			if (PQputCopyData(target_conn, copybuf, bytes) != 1)
			{
				fprintf(stderr,"writing to target table failed destination connection reported: %s",
							 PQerrorMessage(target_conn));
				goto exit;
			}
			args->count++;
			curr->count++;
			PQfreemem(copybuf);
		}

		if (bytes != -1)
		{
			fprintf(stderr,"reading from origin table failed source connection returned %d: %s",
						bytes, PQerrorMessage(origin_conn));
			goto exit;
		}

		/* Send local finish */
		if (PQputCopyEnd(target_conn, NULL) != 1)
		{
			fprintf(stderr,"sending copy-completion to destination connection failed destination connection reported: %s",
						 PQerrorMessage(target_conn));
			goto exit;
		}

		finish_copy_origin_tx(origin_conn);
		finish_copy_target_tx(target_conn);
		curr->complete = true;
		PQclear(res1);
		PQclear(res2);
		resetStringInfo(&query);

		GETTIMEOFDAY(&after);
		DIFF_MSEC(&after, &before, elapsed_msec);
		fprintf(stderr,"thread %d migrate task %d table %s,%s %ld rows complete, time cost %.3f ms\n",
						 args->id, curr->id, nspname, relname, curr->count, elapsed_msec);
	}
	
	args->all_ok = true;

exit:

	PQfinish(origin_conn);
	PQfinish(target_conn);
	ThreadExit(0);
	return NULL;
}

/*
 * Make standard postgres connection, ERROR on failure.
 */
static PGconn *
pglogical_connect(const char *connstring, const char *connname)
{
	PGconn		   *conn;
	StringInfoData	dsn;

	initStringInfo(&dsn);
	appendStringInfo(&dsn,
					"%s fallback_application_name='%s'",
					connstring, connname);

	conn = PQconnectdb(dsn.data);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr,"could not connect to the postgresql server: %s dsn was: %s",
						PQerrorMessage(conn), dsn.data);
		return NULL;
	}

	return conn;
}

static bool
WaitThreadEnd(int n, Thread *th)
{
	ThreadHandle *hanlde = NULL;
	int i;

	hanlde = (ThreadHandle *)malloc(sizeof(ThreadHandle) * n);
	for(i = 0; i < n; i++)
	{
		hanlde[i]=th[i].os_handle;
	}

#ifdef WIN32
	WaitForMultipleObjects(n, hanlde, TRUE, INFINITE);
#else
	for(i = 0; i < n; i++)
		pthread_join(hanlde[i], NULL);
#endif

	free(hanlde);

	return true;
}

static int
ThreadCreate(Thread *th,
				  void *(*start)(void *arg),
				  void *arg)
{
	int		rc = -1;
#ifdef WIN32
	th->os_handle = (HANDLE)_beginthreadex(NULL,
		0,								
		(unsigned(__stdcall*)(void*)) start,
		arg,			
		0,				
		&th->thid);		

	/* error for returned value 0 */
	if (th->os_handle == (HANDLE) 0)
		th->os_handle = INVALID_HANDLE_VALUE;
	else
		rc = 1;
#else
	rc = pthread_create(&th->os_handle,
		NULL,
		start,
		arg);
#endif
	return rc;
}

static void
ThreadExit(int code)
{
#ifdef WIN32
    _endthreadex((unsigned) code);
#else
	pthread_exit((void *)NULL);
	return;
#endif
}

#define ALL_DB_TABLE_SQL "select n.nspname, c.relname from pg_class c, pg_namespace n where n.oid = c.relnamespace and c.relkind = 'r' and n.nspname not in ('pg_catalog','tiger','tiger_data','topology','postgis','information_schema') order by c.relpages desc;"
#define GET_NAPSHOT "SELECT pg_export_snapshot()"

int 
db_sync_main(char *src, char *desc, char *local, int nthread)
{
	int 		i = 0;
	Thread_hd th_hd;
	Thread			*thread = NULL;
	PGresult		*res = NULL;
	PGconn		*origin_conn_repl;
	char		*snapshot = NULL;
//	XLogRecPtr	lsn = 0;
	char	   *query = NULL;
	long		s_count = 0;
	bool		have_err = false;
	TimevalStruct before,
					after; 
	double		elapsed_msec = 0;

	GETTIMEOFDAY(&before);

	memset(&th_hd, 0, sizeof(Thread_hd));
	th_hd.nth = nthread;
	th_hd.src = src;
	th_hd.desc = desc;
	th_hd.local = local;

	query = GET_NAPSHOT;
	origin_conn_repl = pglogical_connect(src, EXTENSION_NAME "_main");

	if (origin_conn_repl == NULL)
	{
		fprintf(stderr, "conn to src faild: %s", PQerrorMessage(origin_conn_repl));
		return 1;
	}

	res = PQexec(origin_conn_repl, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "init open a tran failed: %s", PQresultErrorMessage(res));
		return 1;
	}

	query = GET_NAPSHOT;
	res = PQexec(origin_conn_repl, query);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "init sql run failed: %s", PQresultErrorMessage(res));
		return 1;
	}
	snapshot = pstrdup(PQgetvalue(res, 0, 0));
	th_hd.snapshot = snapshot;
	PQclear(res);

	query = ALL_DB_TABLE_SQL;
	res = PQexec(origin_conn_repl, query);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "init sql run failed: %s", PQresultErrorMessage(res));
		return 1;
	}

	th_hd.ntask = PQntuples(res);
	if (th_hd.ntask >= 1)
	{
		th_hd.task = (Task_hd *)palloc0(sizeof(Task_hd) * th_hd.ntask);
	}

	for (i = 0; i < th_hd.ntask; i++)
	{
		th_hd.task[i].id = i;
		th_hd.task[i].schemaname = pstrdup(PQgetvalue(res, i, 0));
		th_hd.task[i].relname = pstrdup(PQgetvalue(res, i, 1));
		th_hd.task[i].count = 0;
		th_hd.task[i].complete = false;
		if (i != th_hd.ntask - 1)
		{
			th_hd.task[i].next = &th_hd.task[i+1];
		}
	}

	th_hd.l_task = &(th_hd.task[0]);

	PQclear(res);

	th_hd.th = (ThreadArg *)malloc(sizeof(ThreadArg) * th_hd.nth);
	for (i = 0; i < th_hd.nth; i++)
	{
		th_hd.th[i].id = i;
		th_hd.th[i].count = 0;
		th_hd.th[i].all_ok = false;

		/*
		th_hd.th[i].from = pglogical_connect(th_hd.src, EXTENSION_NAME "_copy");
		if (th_hd.th[i].from == NULL)
		{
			fprintf(stderr, "init src conn failed: %s", PQerrorMessage(th_hd.th[i].from));
			return 1;
		}
		start_copy_origin_tx(th_hd.th[i].from, snapshot);
		
		th_hd.th[i].to = pglogical_connect(th_hd.desc, EXTENSION_NAME "_copy");
		if (th_hd.th[i].to == NULL)
		{
			fprintf(stderr, "init desc conn failed: %s", PQerrorMessage(th_hd.th[i].to));
			return 1;
		}
		start_copy_target_tx(th_hd.th[i].to);
		*/

		th_hd.th[i].hd = &th_hd;
	}
	pthread_mutex_init(&th_hd.t_lock, NULL);
	//pthread_mutex_init(&th_hd.t_lock_com, NULL);

	thread = (Thread *)palloc0(sizeof(Thread) * th_hd.nth);
	for (i = 0; i < th_hd.nth; i++)
	{
		ThreadCreate(&thread[i], copy_table_data, &th_hd.th[i]);
	}
	
	WaitThreadEnd(th_hd.nth, thread);

	GETTIMEOFDAY(&after);
	DIFF_MSEC(&after, &before, elapsed_msec);

	for (i = 0; i < th_hd.nth; i++)
	{
		if(th_hd.th[i].all_ok)
		{
			s_count = th_hd.th[i].count;
		}
		else
		{
			have_err = true;;
		}
	}

	fprintf(stderr, "job migrate	row %ld\n", s_count);
	fprintf(stderr, "all time cost %.3f ms\n", elapsed_msec);
	if (have_err)
	{
		fprintf(stderr, "migration process with errors\n");
	}

	return 0;
}

/*
 * Make replication connection, ERROR on failure.
 */
static PGconn *
pglogical_connect_replica(const char *connstring, const char *connname)
{
	PGconn		   *conn;
	StringInfoData	dsn;

	initStringInfo(&dsn);
	appendStringInfo(&dsn,
					"%s replication=database fallback_application_name='%s'",
					connstring, connname);

	conn = PQconnectdb(dsn.data);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "could not connect to the postgresql server in replication mode: %s dsn was: %s",
						PQerrorMessage(conn), dsn.data);
		return NULL;
	}

	
	return conn;
}


