PGFILEDESC = "ali_recvlogical"

NAME = ali_recvlogical

MODULE_big = ali_recvlogical
MODULES = ali_recvlogical

EXTENSION = ali_recvlogical
OBJS = pg_logicaldecode.o pqformat.o stringinfo.o utils.o

PG_CPPFLAGS  = -DFRONTEND -I$(srcdir) -I$(libpq_srcdir)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)


all: all-lib demo.o
	$(CXX) $(CFLAGS) demo.o ali_recvlogical.so $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o demo
