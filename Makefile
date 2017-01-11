ERLANG_PATH = $(shell erl -eval 'io:format("~s", [lists:concat([code:root_dir(), "/erts-", erlang:system_info(version), "/include"])])' -s init stop -noshell)
CFLAGS = -O3 -fPIC -shared -Wall -Wextra -I$(ERLANG_PATH)
CC = gcc

all: murmur_nif.so

murmur_nif.so:
	mkdir -p priv
	$(CC) $(CFLAGS) -o priv/murmur_nif.so native/murmur_nif.c

clean:
	rm native/murmur_nif.so
