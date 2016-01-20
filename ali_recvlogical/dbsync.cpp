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

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "lib/stringinfo.h"

#include "access/xlog_internal.h"
#include "common/fe_memutils.h"
#include "getopt_long.h"
#include "libpq-fe.h"
#include "libpq/pqsignal.h"
#include "pqexpbuffer.h"
#include "libpq/pqformat.h"

#include "pg_logicaldecode.h"
#include "catalog/catversion.h"

#include "pgsync.h"

/*
 * Unfortunately we can't do sensible signal handling on windows...
 */


// SELECT * FROM pg_create_logical_replication_slot('regression_slot', 'ali_decoding');
//hander->connection_string = (char *)"hostaddr=10.101.82.48 port=5432 dbname=test user=test password=pgsql";

int
main(int argc, char **argv)
{
	int		rc = 0;
	bool	init = false;
	char	*src = NULL;
	char	*desc = NULL;
	char	*local = NULL;

	src =   (char *)"host=10.98.109.111 port=3012 dbname=base_dplus_phoenixdev user=pg012 password=pgsql";
	local = (char *)"host=10.101.82.48 port=5432 dbname=test user=test password=pgsql";
	desc =  (char *)"host=10.98.109.111 dbname=test2 port=5888  user=test password=pgsql";

	return db_sync_main(src, desc, local ,2);
}

