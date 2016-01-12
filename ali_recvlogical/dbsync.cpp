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

/* Time to sleep between reconnection attempts */
#define RECONNECT_SLEEP_TIME 5

static int	standby_message_timeout = 10 * 1000;		/* 10 sec = default */

static volatile bool time_to_abort = false;

/*
 * Unfortunately we can't do sensible signal handling on windows...
 */
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


// SELECT * FROM pg_create_logical_replication_slot('regression_slot', 'ali_decoding');
//hander->connection_string = (char *)"hostaddr=10.101.82.48 port=5432 dbname=test user=test password=pgsql";

int
main(int argc, char **argv)
{
	Decoder_handler *hander;
	int		rc = 0;
	bool	init = false;
	char	*src = NULL;
	char	*desc = NULL;

	src = "host=10.98.109.111 port=3012 dbname=base_dplus_phoenixdev user=pg012 password=pgsql";
	desc = "host=10.101.82.48 port=5432 dbname=test user=test password=pgsql";
	
	hander = init_hander();
	hander->connection_string = (char *)src;

	init_logfile(hander);

	return db_sync_main(src, desc, NULL ,1);
}
