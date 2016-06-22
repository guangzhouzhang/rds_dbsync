# pgsql2pgsql
工具 pgsql2pgsql 支持不落地的把 GreenPlum/PostgreSQL/PPAS 中的表迁移到 GreenPlum/PostgreSQL/PPAS

# pgsql2pgsql 支持的功能

	1 PostgreSQL/PPAS/GreenPlum 全量数据迁移到 PostgreSQL/PPAS/GreenPlum

	2 PostgreSQL/PPAS(版本大于9.4) 全量+增量迁移到 PostgreSQL/PPAS

# 参数配置
修改配置文件 my.cfg，配置源和目的库连接信息

	1. 源库 pgsql 连接信息
		[src.pgsql]
		connect_string = "host=192.168.1.1 dbname=test port=5888  user=test password=pgsql"

	2. 本地临时DB pgsql 连接信息
		[local.pgsql]
		connect_string = "host=192.168.1.1 dbname=test port=5888  user=test2 password=pgsql"

	3. 目的库 pgsql 连接信息
		[desc.pgsql]
		connect_string = "host=192.168.1.1 dbname=test port=5888  user=test3 password=pgsql"


#注意
	1. 如果要做增量数据同步，连接源库需要有创建 replication slot 的权限
	1. 源库 pgsql 的连接信息中，用户最好是对应 åDB 的 owner
	2. 目的库 pgsql 的连接信息，用户需要对目标表有写权限

# mysql2pgsql用法

	
	1 全库迁移
	
	./pgsql2pgsql 
	迁移程序会默认把对应 pgsql 库中所有的用户表数据将迁移到 pgsql
	
	2 状态信息查询
	连接本地临时DB，可以查看到单次迁移过程中的状态信息。他们放在表 db_sync_status 中，包括全量迁移的开始和结束时间，增量迁移的开始时间，增量同步的数据情况。

