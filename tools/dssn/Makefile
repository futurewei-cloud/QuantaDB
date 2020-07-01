TOP = ../..

CFLAGS = -I$(TOP)/src/dssn -I$(TOP)/src -I$(TOP)/install/include/ramcloud -I$(TOP)/obj.master

%.o:$(TOP)/src/%.cc
	g++ -c $(CFLAGS) $< -o $@

%.o:$(TOP)/src/dssn/%.cc
	g++ -c $(CFLAGS) $< -o $@

all: txlog datalog

txlog.o: txlog.cc
	g++ -c $(CFLAGS) $< -o $@

datalog.o: datalog.cc $(TOP)/src/dssn/DataLog.h
	g++ -c $(CFLAGS) $< -o $@

txlog: txlog.o TxLog.o TxEntry.o MurmurHash3.o
	g++ -o $@ $^

datalog: datalog.o
	g++ -o $@ $^

clean:
	rm -rf txlog_dumper *.o core
