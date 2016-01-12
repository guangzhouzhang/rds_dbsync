

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


#define EXTENSION_NAME "rds_logical_sync"


extern char *ensure_replication_slot_snapshot(PGconn *origin_conn, char *slot_name,
								 XLogRecPtr *lsn);
extern int db_sync_main(char *src, char *desc, char *local, int nthread);

#ifdef __cplusplus
}
#endif

#endif 

