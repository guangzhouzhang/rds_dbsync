PGFILEDESC = "ali_recvlogical"

NAME = ali_recvlogical

MODULE_big = ali_recvlogical
MODULES = ali_recvlogical

EXTENSION = ali_recvlogical
OBJS = pg_logicaldecode.o pqformat.o stringinfo.o utils.o misc.o pgsync.o

PG_CPPFLAGS  = -DFRONTEND -I$(srcdir) -I$(libpq_srcdir) -I/u01/mysql/include/
PG_FLAGS  = -DFRONTEND -I$(srcdir) -I$(libpq_srcdir) -I/u01/mysql/include/

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)


all: all-lib demo.o dbsync.o mysql2pgsql.o dbsync-mysql2pgsql.o
	$(CXX) $(CFLAGS) demo.o ali_recvlogical.so $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o demo
	$(CXX) $(CFLAGS) dbsync.o ali_recvlogical.so $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o dbsync
	$(CXX) $(CFLAGS) mysql2pgsql.o dbsync-mysql2pgsql.o misc.o stringinfo.o $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -L/u01/mysql/lib -lmysqlclient -o mysql2pgsql


clean:
	rm -rf *.o dbsync mysql2pgsql demo
