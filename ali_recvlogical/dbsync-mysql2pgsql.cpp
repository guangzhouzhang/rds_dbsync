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

#include "mysql.h"

static bool getOption(int argc, char* argv[]);

static char *tabname = NULL;
/*
 * Unfortunately we can't do sensible signal handling on windows...
 */


// SELECT * FROM pg_create_logical_replication_slot('regression_slot', 'ali_decoding');
//hander->connection_string = (char *)"hostaddr=10.101.82.48 port=5432 dbname=test user=test password=pgsql";

int
main(int argc, char **argv)
{
	int		rc = 0;
	char	*desc = NULL;
	mysql_conn_info src;
	int		num_thread = 0;

	if (!getOption(argc, argv))
	{
		return 1;
	}

	src.host = (char *)"10.97.252.32";
	src.port = 8080;
	src.user = (char *)"crdsperf";
	src.passwd = (char *)"crdsperf";
	src.db = (char *)"crdsperf";
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

static 
bool getOption(int argc, char* argv[])
{
	int c;
	while (true)
	{
		c = getopt(argc, argv, "t:");
		if (c == -1)
			break;

		switch (c)
		{
		case 't':
			tabname = optarg;
			break;
	
		case '?':
		default:
			return false;
		}
	}

	return true;
}

