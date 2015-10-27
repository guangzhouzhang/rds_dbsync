# contrib/ali_decoding/Makefile
MODULE_big = ali_decoding
MODULES = ali_decoding
OBJS    = ali_decoding.o 

DATA = ali_decoding--0.0.1.sql ali_decoding--unpackaged--0.0.1.sql

EXTENSION = ali_decoding

NAME = ali_decoding

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
