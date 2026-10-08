CFLAGS = -Wall -Wextra -pedantic -std=gnu99 -g -I/local/courses/csse2310/include

LDFLAGS = -L/local/courses/csse2310/lib -lcsse2310a3
all: uqllmclient uqllmserver
uqllmclient: uqllmclient.c
	$(CC) $(CFLAGS) -o uqllmclient uqllmclient.c $(LDFLAGS)
uqllmserver: uqllmserver.c
	$(CC) $(CFLAGS) -o uqllmserver uqllmserver.c $(LDFLAGS) -lpthread
clean:
	rm -f uqllmclient uqllmserver

