TOP = ../..

CFLAGS = -I$(TOP)/src/quantadb -I$(TOP)/src -I$(TOP)/install/include/ramcloud -I$(TOP)/obj.master -I$(TOP)/prometheus-cpp/core/include/ -I$(TOP)/prometheus-cpp/pull/include/ -I$(TOP)/prometheus-cpp/build/pull/include/ -I$(TOP)/prometheus-cpp/build/core/include/

%.o:$(TOP)/src/%.cc
	g++ -c $(CFLAGS) $< -o $@

%.o:$(TOP)/src/quantadb/%.cc
	g++ -c $(CFLAGS) $< -o $@

%:%.o
	g++ -o $@ $^

TARGETS = txlog datalog rdtscp rdtscp_test2 process_cpu phc_test

all: $(TARGETS)

txlog.o: txlog.cc
	g++ -c $(CFLAGS) $< -o $@

datalog.o: datalog.cc $(TOP)/src/quantadb/DataLog.h
	g++ -c $(CFLAGS) $< -o $@

txlog: txlog.o TxLog.o TxEntry.o MurmurHash3.o
	g++ -o $@ $^

datalog: datalog.o
	g++ -o $@ $^

rdtscp: rdtscp.o
	g++ -o $@ $^ -lpthread

rdtscp_test2: rdtscp_test2.o
	g++ -o $@ $^ -lpthread

phc_test: phc_test.c
	g++ -DMAIN_FUNCTION phc_test.c -o phc_test

clean:
	rm -rf txlog_dumper *.o core $(TARGETS)
