TOP=..

CFLAGS=-g -I$(TOP)/internal -I$(TOP)/utils --std=c++17 -mavx2 -Wall

%.o: %.cc
	g++ -c $(CFLAGS) -o $@ $<

%: %.o
	g++ $(CFLAGS) -o $@ $< -lpthread
	./$@ > testlog

TARGET=hashmap_poc hashmap_unit_test cbuf_unit_test hashmap_unit_test2 hashmap_kv_test hashmap_unit_test3

all: $(TARGET)

hashmap_kv_test.o: ../internal/hashmap_kv_rcache.h ../internal/cbuf_per_thread.h ../internal/hash_map.h
hashmap_unit_test2.o: ../internal/hash_map.h
hashmap_unit_test3.o: ../internal/hash_map.h ../utils/c_str_util_classes.h

hashmap_poc: hashmap_poc.o ../internal/hash_map.h
hashmap_unit_test: hashmap_unit_test.o ../internal/hash_map.h
hashmap_unit_test2: hashmap_unit_test2.o
hashmap_unit_test3: hashmap_unit_test3.o
cbuf_unit_test: cbuf_unit_test.o ../internal/cbuf_per_thread.h
hashmap_kv_test: hashmap_kv_test.o ../internal/hash_map.h ../internal/cbuf_per_thread.h ../internal/hashmap_kv_rcache.h

clean:
	rm -rf *.o a.out mklog cscope.out testlog core $(TARGET)