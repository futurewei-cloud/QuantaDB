TOP = ../..

CFLAGS = -I$(TOP)/src/dssn -I$(TOP)/src -I$(TOP)/install/include/ramcloud -I$(TOP)/obj.master

%.o:$(TOP)/src/%.cc
	g++ -c $(CFLAGS) $< -o $@

%.o:$(TOP)/src/dssn/%.cc
	g++ -c $(CFLAGS) $< -o $@

all: txlog_dumper

txlog_dumper.o: txlog_dumper.cc
	g++ -c $(CFLAGS) $< -o $@

txlog_dumper: txlog_dumper.o TxLog.o TxEntry.o MurmurHash3.o
	g++ -o txlog_dumper $^

clean:
	rm -rf txlog_dumper *.o
