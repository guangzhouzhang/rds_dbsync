/*-------------------------------------------------------------------------
 *
 * pg_recvlogical.c - receive data from a logical decoding slot in a streaming
 *					  fashion and write it to a local file.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_basebackup/pg_recvlogical.c
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <sys/stat.h>

#include "lib/stringinfo.h"

#include "access/xlog_internal.h"
#include "common/fe_memutils.h"
#include "getopt_long.h"
#include "libpq-fe.h"
#include "libpq/pqsignal.h"
#include "pqexpbuffer.h"
#include "libpq/pqformat.h"

#include "catalog/catversion.h"

#include "pgsync.h"

#include "mysql.h"

static char *tabname = NULL;

int
main(int argc, char **argv)
{
	int		rc = 0;
	char	*desc = NULL;
	mysql_conn_info src;
	int		num_thread = 0;

	src.host = (char *)"localhost";
	src.port = 3306;
	src.user = (char *)"test";
	src.passwd = (char *)"test";
	src.db = (char *)"test";
	src.encodingdir = (char *)"share";
	src.encoding = (char *)"utf8";
	if(tabname != NULL)
	{
		src.tabname = (char *)tabname;
		num_thread = 1;
	}
	else
	{
		src.tabname = NULL;
		num_thread = 5;
	}
	
	desc =  (char *)"host=10.98.109.111 dbname=gptest port=5888  user=gptest password=pgsql";

	return mysql2pgsql_sync_main(desc , num_thread, &src);
}
