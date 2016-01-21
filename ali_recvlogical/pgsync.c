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
#include "libpq/pqsignal.h"

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

static void sigint_handler(int signum);

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
	int			src_version;
	char		*slot_name;

	char		*desc;
	int			desc_version;
	bool		desc_is_greenplum;
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

static PGconn *pglogical_connect(const char *connstring, const char *connname);
static int start_copy_origin_tx(PGconn *conn, const char *snapshot, int pg_version);
static int start_copy_target_tx(PGconn *conn, int pg_version, bool is_greenplum);
static int finish_copy_origin_tx(PGconn *conn);
static int finish_copy_target_tx(PGconn *conn);
static void *copy_table_data(void *arg);
static int ThreadCreate(Thread *th, void *(*start)(void *arg), void *arg);
static bool WaitThreadEnd(int n, Thread *th);
static void ThreadExit(int code);
static int ExecuteSqlStatement(PGconn	   *conn, const char *query);
static char *get_synchronized_snapshot(PGconn *conn);
static int setup_connection(PGconn *conn, int remoteVersion, bool is_greenplum);
static bool is_slot_exists(PGconn *conn, char *slotname);
static bool is_greenplum(PGconn *conn);
static void *logical_decoding_thread(void *arg);


static volatile bool time_to_abort = false;
static volatile bool full_sync_complete = false;


/*
 * Transaction management for COPY.
 */
static int
start_copy_origin_tx(PGconn *conn, const char *snapshot, int pg_version)
{
	PGresult	   *res;
	const char	   *setup_query =
		"BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;\n";
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
	
	setup_connection(conn, pg_version, false);

	return 0;
}

static int
start_copy_target_tx(PGconn *conn, int pg_version, bool is_greenplum)
{
	PGresult	   *res;
	const char	   *setup_query =
		"BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;\n";

	res = PQexec(conn, setup_query);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "BEGIN on target node failed: %s",
				PQresultErrorMessage(res));
		return 1;
	}

	PQclear(res);

	setup_connection(conn, pg_version, is_greenplum);

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

		start_copy_origin_tx(origin_conn, hd->snapshot, hd->src_version);
		start_copy_target_tx(target_conn, hd->desc_version, hd->desc_is_greenplum);

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
		fprintf(stderr,"thread %d migrate task %d table %s.%s %ld rows complete, time cost %.3f ms\n",
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
	PGconn		*desc_conn;
	PGconn		*local_conn;
	char		*snapshot = NULL;
	XLogRecPtr	lsn = 0;
	char	   *query = NULL;
	long		s_count = 0;
	long		t_count = 0;
	bool		have_err = false;
	TimevalStruct before,
					after; 
	double		elapsed_msec = 0;
	Decoder_handler *hander = NULL;
	int	src_version = 0;
	struct Thread decoder;
	bool	replication_sync = false;

#ifndef WIN32
		signal(SIGINT, sigint_handler);
#endif

	GETTIMEOFDAY(&before);

	memset(&th_hd, 0, sizeof(Thread_hd));
	th_hd.nth = nthread;
	th_hd.src = src;
	th_hd.desc = desc;
	th_hd.local = local;

	origin_conn_repl = pglogical_connect(src, EXTENSION_NAME "_main");
	if (origin_conn_repl == NULL)
	{
		fprintf(stderr, "conn to src faild: %s", PQerrorMessage(origin_conn_repl));
		return 1;
	}

	desc_conn = pglogical_connect(desc, EXTENSION_NAME "_main");
	if (desc_conn == NULL)
	{
		fprintf(stderr, "init desc conn failed: %s", PQerrorMessage(desc_conn));
		return 1;
	}
	th_hd.desc_version = PQserverVersion(desc_conn);
	th_hd.desc_is_greenplum = is_greenplum(desc_conn);
	PQfinish(desc_conn);

	local_conn = pglogical_connect(local, EXTENSION_NAME "_main");
	if (local_conn == NULL)
	{
		fprintf(stderr, "init local conn failed: %s", PQerrorMessage(local_conn));
		return 1;
	}
	ExecuteSqlStatement(local_conn, "CREATE TABLE IF NOT EXISTS sync_sqls(id bigserial, sql text)");
	PQfinish(local_conn);

	res = PQexec(origin_conn_repl, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "init open a tran failed: %s", PQresultErrorMessage(res));
		return 1;
	}

	src_version = PQserverVersion(origin_conn_repl);
	is_greenplum(origin_conn_repl);
	th_hd.src_version = src_version;
	if (src_version >= 90400)
	{
		if (!is_slot_exists(origin_conn_repl, EXTENSION_NAME "_slot"))
		{
			int rc = 0;

			hander = init_hander();
			hander->connection_string = src;
			init_logfile(hander);
			rc = initialize_connection(hander);
			if(rc != 0)
			{
				fprintf(stderr, "create replication conn failed\n");
				return 1;
			}
			hander->do_create_slot = true;
			snapshot = create_replication_slot(hander, &lsn, EXTENSION_NAME "_slot");
			if (snapshot == NULL)
			{
				fprintf(stderr, "create replication slot failed\n");
				return 1;
			}

			th_hd.slot_name = hander->replication_slot;
		}
		else
		{
			th_hd.slot_name = EXTENSION_NAME "_slot";
		}

		replication_sync = true;
	}
	else if (src_version >= 90200)
	{
		snapshot = get_synchronized_snapshot(origin_conn_repl);
		th_hd.snapshot = snapshot;
	}

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

		th_hd.th[i].hd = &th_hd;
	}
	pthread_mutex_init(&th_hd.t_lock, NULL);

	thread = (Thread *)palloc0(sizeof(Thread) * th_hd.nth);
	for (i = 0; i < th_hd.nth; i++)
	{
		ThreadCreate(&thread[i], copy_table_data, &th_hd.th[i]);
	}

	if (replication_sync)
	{
		ThreadCreate(&decoder, logical_decoding_thread, &th_hd);
	}
	
	WaitThreadEnd(th_hd.nth, thread);

	full_sync_complete = true;

	GETTIMEOFDAY(&after);
	DIFF_MSEC(&after, &before, elapsed_msec);

	for (i = 0; i < th_hd.nth; i++)
	{
		if(th_hd.th[i].all_ok)
		{
			s_count += th_hd.th[i].count;
		}
		else
		{
			have_err = true;;
		}
	}

	for (i = 0; i < th_hd.ntask; i++)
	{
		t_count += th_hd.task[i].count;
	}

	fprintf(stderr, "job migrate row %ld task row %ld \n", s_count, t_count);
	fprintf(stderr, "all time cost %.3f ms\n", elapsed_msec);
	if (have_err)
	{
		fprintf(stderr, "migration process with errors\n");
	}

	WaitThreadEnd(1, &decoder);

	return 0;
}

static int
setup_connection(PGconn *conn, int remoteVersion, bool is_greenplum)
{
	char *dumpencoding = "utf8";

	/*
	 * Set the client encoding if requested. If dumpencoding == NULL then
	 * either it hasn't been requested or we're a cloned connection and then
	 * this has already been set in CloneArchive according to the original
	 * connection encoding.
	 */
	if (PQsetClientEncoding(conn, dumpencoding) < 0)
	{
		fprintf(stderr, "invalid client encoding \"%s\" specified\n",
					dumpencoding);
		return 1;
	}

	/*
	 * Get the active encoding and the standard_conforming_strings setting, so
	 * we know how to escape strings.
	 */
	//AH->encoding = PQclientEncoding(conn);

	//std_strings = PQparameterStatus(conn, "standard_conforming_strings");
	//AH->std_strings = (std_strings && strcmp(std_strings, "on") == 0);

	/* Set the datestyle to ISO to ensure the dump's portability */
	ExecuteSqlStatement(conn, "SET DATESTYLE = ISO");

	/* Likewise, avoid using sql_standard intervalstyle */
	if (remoteVersion >= 80400)
		ExecuteSqlStatement(conn, "SET INTERVALSTYLE = POSTGRES");

	/*
	 * If supported, set extra_float_digits so that we can dump float data
	 * exactly (given correctly implemented float I/O code, anyway)
	 */
	if (remoteVersion >= 90000)
		ExecuteSqlStatement(conn, "SET extra_float_digits TO 3");
	else if (remoteVersion >= 70400)
		ExecuteSqlStatement(conn, "SET extra_float_digits TO 2");

	/*
	 * If synchronized scanning is supported, disable it, to prevent
	 * unpredictable changes in row ordering across a dump and reload.
	 */
	if (remoteVersion >= 80300 && !is_greenplum)
		ExecuteSqlStatement(conn, "SET synchronize_seqscans TO off");

	/*
	 * Disable timeouts if supported.
	 */
	if (remoteVersion >= 70300)
		ExecuteSqlStatement(conn, "SET statement_timeout = 0");
	if (remoteVersion >= 90300)
		ExecuteSqlStatement(conn, "SET lock_timeout = 0");

	return 0;
}

static int
ExecuteSqlStatement(PGconn *conn, const char *query)
{
	PGresult   *res;
	int		rc = 0;

	res = PQexec(conn, query);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "set %s failed: %s",
						query, PQerrorMessage(conn));
		rc = 1;
	}
	PQclear(res);

	return rc;
}

static char *
get_synchronized_snapshot(PGconn *conn)
{
	char	   *query = "SELECT pg_export_snapshot()";
	char	   *result;
	PGresult   *res;

	res = PQexec(conn, query);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "init sql run failed: %s", PQresultErrorMessage(res));
		return NULL;
	}
	result = pstrdup(PQgetvalue(res, 0, 0));
	PQclear(res);

	return result;
}

static bool
is_slot_exists(PGconn *conn, char *slotname)
{
	PGresult   *res;
	int			ntups;
	bool	exist = false;
	PQExpBuffer query;

	query = createPQExpBuffer();
	appendPQExpBuffer(query, "select slot_name from pg_replication_slots where slot_name = '%s';",
							  slotname);

	res = PQexec(conn, query->data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		destroyPQExpBuffer(query);
		return false;
	}

	/* Expecting a single result only */
	ntups = PQntuples(res);
	if (ntups == 1)
	{
		exist = true;
	}

	PQclear(res);
	destroyPQExpBuffer(query);

	return exist;
}

static bool
is_greenplum(PGconn *conn)
{
	char	   *query = "select version from  version()";
	bool	is_greenplum = false;
	char	*result;
	PGresult   *res;

	res = PQexec(conn, query);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "init sql run failed: %s", PQresultErrorMessage(res));
		return false;
	}
	result = PQgetvalue(res, 0, 0);
	if (strstr(result, "Greenplum") != NULL)
	{
		is_greenplum = true;
	}
	
	PQclear(res);

	return is_greenplum;
}

#define RECONNECT_SLEEP_TIME 5

/*
 * COPY single table over wire.
 */
static void *
logical_decoding_thread(void *arg)
{
	Thread_hd *hd = (Thread_hd *)arg;
	Decoder_handler *hander;
	int		rc = 0;
	bool	init = false;
	PGconn *local_conn;
	PQExpBuffer buffer;
    char    *stmtname = "insert_sqls";
    Oid     type[1];
	const char *paramValues[1];
	PGresult *res = NULL;

    type[0] = 25;

	buffer = createPQExpBuffer();

	local_conn = pglogical_connect(hd->local, EXTENSION_NAME "_decoding");
	if (local_conn == NULL)
	{
		fprintf(stderr, "init src conn failed: %s", PQerrorMessage(local_conn));
		goto exit;
	}
	setup_connection(local_conn, 90400, false);

	res = PQprepare(local_conn, stmtname, "INSERT INTO sync_sqls (sql) VALUES($1)", 1, type);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "create PQprepare failed: %s", PQerrorMessage(local_conn));
		PQfinish(local_conn);
		goto exit;
	}
	PQclear(res);

	hander = init_hander();
	hander->connection_string = hd->src;
	init_logfile(hander);
	rc = check_handler_parameters(hander);
	if(rc != 0)
	{
		exit(1);
	}

	rc = initialize_connection(hander);
	if(rc != 0)
	{
		exit(1);
	}

	hander->replication_slot = hd->slot_name;
	init_streaming(hander);
	init = true;

	while (true)
	{
		ALI_PG_DECODE_MESSAGE *msg = NULL;

		if (time_to_abort)
		{
			if (hander->copybuf != NULL)
			{
				PQfreemem(hander->copybuf);
				hander->copybuf = NULL;
			}
			if (hander->conn)
			{
				PQfinish(hander->conn);
				hander->conn = NULL;
			}
			if (local_conn)
			{
				PQdescribePrepared(local_conn, stmtname);
				PQfinish(local_conn);
			}
			break;
		}

		if (!init)
		{
			initialize_connection(hander);
			init_streaming(hander);
			init = true;
		}

		msg = exec_logical_decoder(hander, &time_to_abort);
		if (msg != NULL)
		{
			out_put_tuple_to_sql(hander, msg, buffer);
			paramValues[0] = buffer->data;
			res = PQexecPrepared(local_conn, stmtname, 1, paramValues, NULL, NULL, 1);
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				fprintf(stderr, "exec prepare INSERT INTO sync_sqls failed: %s", PQerrorMessage(local_conn));
				time_to_abort = true;
			}
			else
			{
				hander->flushpos = hander->recvpos;
			}

			PQclear(res);
			resetPQExpBuffer(buffer);
		}
		else
		{
			pg_sleep(RECONNECT_SLEEP_TIME * 1000000);
			init = false;
		}
	}


exit:

	destroyPQExpBuffer(buffer);

	ThreadExit(0);
	return NULL;
}

#ifndef WIN32

/*
 * When sigint is called, just tell the system to exit at the next possible
 * moment.
 */
static void
sigint_handler(int signum)
{
	time_to_abort = true;
}

#endif

