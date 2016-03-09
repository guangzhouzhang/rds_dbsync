

#ifndef PG_SYNC_H
#define PG_SYNC_H

#include "postgres_fe.h"

#include "lib/stringinfo.h"
#include "lib/stringinfo.h"
#include "common/fe_memutils.h"

#include "libpq-fe.h"

#include "access/transam.h"
#include "libpq/pqformat.h"
#include "pqexpbuffer.h"

#ifdef __cplusplus
extern		"C"
{
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

extern void sigint_handler(int signum);

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

#ifdef WIN32
static int64 atoll(const char *nptr);
#endif


#include "mysql.h"

typedef struct ThreadArg
{
	int			id;
	long		count;
	bool		all_ok;
	PGconn		*from;
	PGconn		*to;

	struct Thread_hd *hd;
}ThreadArg;

typedef struct mysql_conn_info
{
	char	*host;
	int		port;
	char	*user;
	char	*passwd;
	char	*encoding;
	char	*db;
	char	*encodingdir;
	char	*tabname;
}mysql_conn_info;

typedef struct Thread_hd
{
	int			nth;
	struct ThreadArg *th;
	
	const char *snapshot;
	char		*src;
	int			src_version;
	char		*slot_name;

	mysql_conn_info	*mysql_src;

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


#define EXTENSION_NAME "rds_logical_sync"

extern int db_sync_main(char *src, char *desc, char *local, int nthread);
extern PGconn *pglogical_connect(const char *connstring, const char *connname);
extern bool is_greenplum(PGconn *conn);
extern bool WaitThreadEnd(int n, Thread *th);
extern void ThreadExit(int code);
extern int ThreadCreate(Thread *th, void *(*start)(void *arg), void *arg);
extern int mysql2pgsql_sync_main(char *desc, int nthread, mysql_conn_info *hd);
extern int start_copy_target_tx(PGconn *conn, int pg_version, bool is_greenplum);
extern int finish_copy_target_tx(PGconn *conn);
extern int ExecuteSqlStatement(PGconn	   *conn, const char *query);
extern int setup_connection(PGconn *conn, int remoteVersion, bool is_greenplum);


#ifdef __cplusplus
}
#endif

#endif 

