TOP=..
CC=g++

CFLAGS= -O3 -I $(TOP)/internal -I$(TOP)/utils --std=c++17 -mavx2 -Wall -DNDEBUG -march=native -I$(TOP)/../src/quantadb

## Turn-on one (of the two lines below) to compile string-key and prehash option for hashmap_bench
#CFLAGS += -DSTR_KEY -DPMEMHASH_PREHASH
#CFLAGS += -DSTR_KEY

%.o: $(TOP)/../src/quantadb/%.cc
	$(CC) -c $(CFLAGS) -o $@ $<

%.o: %.cc
	$(CC) -c $(CFLAGS) -o $@ $<

%: %.o
	$(CC) $(CFLAGS) -o $@ $^ -lpthread
	#./$@

# TARGET=hashmap_bench hashmap_kv_bench
TARGET=hashmap_bench

all: $(TARGET)

hashmap_bench.o: ../internal/hash_map.h
hashmap_kv_bench.o: ../internal/hash_map.h

hashmap_bench: hashmap_bench.o clhash.o

hashmap_kv_bench: hashmap_kv_bench.o

bench:
	echo "Pmemhash Benchmark Run: `date`" > bench_log
	./hashmap_bench 8196 >> bench_log
	./hashmap_bench 65536 >> bench_log
	./hashmap_bench 655360 >> bench_log

clean:
	rm -rf *.o a.out mklog cscope.out testlog $(TARGET)
