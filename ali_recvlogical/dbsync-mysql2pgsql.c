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

#include "pgsync.h"
#include "ini.h"
#include "mysql.h"

static char *tabname = NULL;

int
main(int argc, char **argv)
{
	int		rc = 0;
	char	*desc = NULL;
	mysql_conn_info src;
	int		num_thread = 0;
	void	*cfg = NULL;
	char	*sport = NULL;

	cfg = init_config("my.cfg");
	if (cfg == NULL)
	{
		return 1;
	}

	memset(&src, 0, sizeof(mysql_conn_info));
	get_config(cfg, "src.mysql", "host", &src.host);
	get_config(cfg, "src.mysql", "port", &sport);
	get_config(cfg, "src.mysql", "user", &src.user);
	get_config(cfg, "src.mysql", "password", &src.passwd);
	get_config(cfg, "src.mysql", "db", &src.db);
	get_config(cfg, "src.mysql", "encodingdir", &src.encodingdir);
	get_config(cfg, "src.mysql", "encoding", &src.encoding);
	get_config(cfg, "desc.pgsql", "connect_string", &desc);

	if (src.host == NULL || sport == NULL ||
		src.user == NULL || src.passwd == NULL ||
		src.db == NULL || src.encodingdir == NULL ||
		src.encoding == NULL || desc == NULL)
		return 1;

	src.port = atoi(sport);

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

	return mysql2pgsql_sync_main(desc , num_thread, &src);
}

