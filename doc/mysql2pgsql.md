# mysql2pgsql
工具 mysql2pgsql 支持不落地的把 MYSQL 中的表迁移到 GreenPlum/PostgreSQL/PPAS


# 参数配置
修改配置文件 my.cfg，配置源和目的库连接信息

	1. 源库 mysql 连接信息
		[src.mysql]
		host = "192.168.1.1"
		port = "3306"
		user = "test"
		password = "test"
		db = "test"
		encodingdir = "share"
		encoding = "utf8"

	2. 目的库 pgsql 连接信息
		[desc.pgsql]
		connect_string = "host=192.168.1.1 dbname=test port=5888  user=test password=pgsql"


#注意
	1. 源库 mysql 的连接信息中，用户需要有对所有用户表的读权限
	2. 目的库 pgsql 的连接信息，用户需要对目标表有写的权限

# mysql2pgsql用法

	1 单表迁移
	
	./mysql2pgsql testtable
	
	您可以选择迁移对应MYSQL库中的单个表到pgsql中，同时得到我们推荐的在 pgsql 中对应表的 create table ddl 语句定义
	
	2 全库迁移
	
	./mysql2pgsql 
	
	迁移程序会默认把对应 mysql 库中所有的用户表数据将迁移到 pgsql


