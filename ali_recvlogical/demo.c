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


int
main(int argc, char **argv)
{
	uint32		hi = 0;
	uint32		lo = 0;
	Decoder_handler *hander;
	int		rc = 0;
	bool	init = false;
	StringInfoData buffer;

	hander = (Decoder_handler *)malloc(sizeof(Decoder_handler));
	memset(hander, 0, sizeof(Decoder_handler));
	hander->verbose = 1;
	hander->startpos = InvalidXLogRecPtr;
	hander->outfd = -1;
	
	hander->recvpos = InvalidXLogRecPtr;
	hander->flushpos= InvalidXLogRecPtr;
	hander->startpos= InvalidXLogRecPtr;

	hander->standby_message_timeout = 10 * 1000;
	hander->last_status = -1;
	hander->progname = (char *)"pg_recvlogical";
	

	hander->outfile = (char *)"-";
	hander->startpos = ((uint64) hi) << 32 | lo;
	hander->replication_slot = (char *)"regression_slot";
	hander->do_create_slot = false;
	hander->do_start_slot = true;
	hander->do_drop_slot = false;
	hander->connection_string = (char *)"hostaddr=10.101.82.48 port=5432 dbname=test user=test password=pgsql";
	//hander->connection_string = (char *)"hostaddr=10.98.109.111 port=3012 dbname=base_dplus_phoenixprod user=pg012 password=pgsql";
	// SELECT * FROM pg_create_logical_replication_slot('regression_slot', 'ali_decoding');

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
	init_streaming(hander);
	init = true;

#ifndef WIN32
		signal(SIGINT, sigint_handler);
#endif

	if (hander->do_drop_slot)
	{
		rc = drop_replication_slot(hander);
		return 0;
	}

	if (hander->do_create_slot)
	{
		rc = create_replication_slot(hander);
		if(rc != 0)
		{
			exit(1);
		}
	}

	if (!hander->do_start_slot)
	{
		disconnect(hander);
		exit(0);
	}

	hander->buffer = &buffer;
	initStringInfo(hander->buffer);
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
			out_put_decode_message(hander, msg, hander->outfd);
			hander->flushpos = hander->recvpos;
		}
		else
		{
			//printf("%s: disconnected; waiting %d seconds to try again\n",hander->progname, RECONNECT_SLEEP_TIME);
			pg_sleep(RECONNECT_SLEEP_TIME * 1000000);
			init = false;
		}
	}
}

