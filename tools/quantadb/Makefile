TOP = ../..

## Figure out the build directory
TOPOBJSUFFIX := $(shell git symbolic-ref -q HEAD | \
	       sed -e s,refs/heads/,.,)

TOPOBJDIR	:= obj$(TOPOBJSUFFIX)

CFLAGS = -I$(TOP)/src/quantadb -I$(TOP)/src -I$(TOP)/install/include/ramcloud -I$(TOP)/$(TOPOBJDIR) -I$(TOP)/prometheus-cpp/core/include/ -I$(TOP)/prometheus-cpp/pull/include/ -I$(TOP)/prometheus-cpp/build/pull/include/ -I$(TOP)/prometheus-cpp/build/core/include/ -march=native

CFLAGS += -g
#CFLaGS += -O3 -NDEBUG

%.o:$(TOP)/src/%.cc
	g++ -c $(CFLAGS) $< -o $@

%.o:$(TOP)/src/quantadb/%.cc
	g++ -c $(CFLAGS) $< -o $@

%.o:%.cc
	g++ -c $(CFLAGS) $< -o $@

%:%.o
	g++ -o $@ $^

TARGETS = txlog datalog rdtscp rdtscp_test2 process_cpu phc_test slab_bench slab_bench_je slab_bench_tc

all: $(TARGETS)

txlog.o: txlog.cc
	g++ -c $(CFLAGS) $< -o $@

datalog.o: datalog.cc $(TOP)/src/quantadb/DataLog.h
	g++ -c $(CFLAGS) $< -o $@

txlog: txlog.o TxLog.o TxEntry.o MurmurHash3.o
	g++ -o $@ $^ -lpthread

datalog: datalog.o
	g++ -o $@ $^ -lpthread

rdtscp: rdtscp.o
	g++ -o $@ $^ -lpthread

rdtscp_test2: rdtscp_test2.o
	g++ -o $@ $^ -lpthread

phc_test: phc_test.c
	g++ -DMAIN_FUNCTION phc_test.c -o phc_test

slab_bench_tc: slab_bench.o
	g++ $(CFLAGS) -o $@ $^ -lpthread -ltcmalloc

slab_bench_je: slab_bench.o
	g++ $(CFLAGS) -o $@ $^ -lpthread -ljemalloc

slab_bench: slab_bench.o
	g++ $(CFLAGS) -o $@ $^ -lpthread

run_bench:
	(for prog in slab_bench slab_bench_je slab_bench_tc; do \
	  for type in malloc new slab; do \
	    for size in 32 1024 vari; do \
	        for nth in 1 4 8 16; do \
	            echo $$prog $$type $$size $$nth; \
	            ./$$prog $$type $$size $$nth; \
	        done; \
	    done; \
	  done; \
	done)

clean:
	rm -rf txlog_dumper *.o core $(TARGETS)
