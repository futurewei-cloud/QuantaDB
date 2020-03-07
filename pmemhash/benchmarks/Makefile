TOP=..
CC=g++

#CFLAGS=-g -O3 -I $(TOP)/internal --std=c++17 -mavx2 -Wall
CFLAGS= -O3 -I $(TOP)/internal -I$(TOP)/utils --std=c++17 -mavx2 -Wall

%.o: %.cc
	$(CC) -c $(CFLAGS) -o $@ $<

%: %.o
	$(CC) $(CFLAGS) -o $@ $< -lpthread
	#./$@

TARGET=hashmap_bench hashmap_kv_bench

all: $(TARGET)

hashmap_bench.o: ../internal/hash_map.h
hashmap_kv_bench.o: ../internal/hash_map.h

hashmap_bench: hashmap_bench.o
hashmap_kv_bench: hashmap_kv_bench.o

clean:
	rm -rf *.o a.out mklog cscope.out testlog $(TARGET)
