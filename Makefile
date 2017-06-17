ERLANG_PATH = $(shell erl -eval 'io:format("~s", [lists:concat([code:root_dir(), "/erts-", erlang:system_info(version), "/include"])])' -s init stop -noshell)
CFLAGS = -O3 -std=c99 -fPIC -Wall -Wextra -Wimplicit-fallthrough=0 -I$(ERLANG_PATH)
CC = gcc

KERNEL_NAME = $(shell uname -s)
ifeq ($(KERNEL_NAME),Linux)
	CFLAGS += -shared
endif
ifeq ($(KERNEL_NAME),Darwin)
	CFLAGS += -undefined dynamic_lookup -dynamiclib
endif

all: murmur_nif.so

murmur_nif.so:
	mkdir -p priv
	$(CC) $(CFLAGS) -o priv/murmur_nif.so native/murmur_nif.c

clean:
	rm priv/murmur_nif.so
