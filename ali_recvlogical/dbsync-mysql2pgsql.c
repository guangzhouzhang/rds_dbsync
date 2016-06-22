
#include "postgres_fe.h"
#include "libpq-fe.h"

#include "pgsync.h"
#include "ini.h"
#include "mysql.h"

int
main(int argc, char **argv)
{
	char	*desc = NULL;
	mysql_conn_info src;
	int		num_thread = 0;
	void	*cfg = NULL;
	char	*sport = NULL;
	char *tabname = NULL;

	cfg = init_config("my.cfg");
	if (cfg == NULL)
	{
		fprintf(stderr, "read config file error, insufficient permissions or my.cfg does not exist");
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
	{
		fprintf(stderr, "parameter error, the necessary parameter is empty");
		return 1;
	}

	src.port = atoi(sport);

	if (argc == 2)
	{
		tabname = argv[1];
		fprintf(stderr, "tablename %s", tabname);
	}

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

