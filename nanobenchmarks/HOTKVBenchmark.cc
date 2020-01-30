// Copyright (c) 2020 Futurewei Technologies Inc

/* Copyright (c) 2009-2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file
 * A performance benchmark for the HOT
 */

#include <math.h>

#include "Common.h"
#include "Context.h"
#include "Cycles.h"
#include "HOTKV.h"
#include "LargeBlockOfMemory.h"
#include "Memory.h"
#include "OptionParser.h"


namespace RAMCloud {
  class HOTBenchmark;
  struct ThreadData {
      ThreadData() {};
      uint64_t keyoffset;
      uint64_t nkeys;
      DSSN::HOTKV* kvs;
      HOTBenchmark* hb;
  };

  
  class HOTBenchmark {
    
  public:
    HOTBenchmark(uint64_t nkeys):
        totalGetCycles(0),
        totalPutCycles(0),
	totalUpdateCycles(0),
	totalGetMetaCycles(0),
        baseTotalGetCycles(0),
        baseTotalPutCycles(0),
        nKeys(nkeys)
    {

    }
    void
    run(uint64_t nthreads)
    {
        uint64_t nkeysPerThread = nKeys/nthreads;
	float speedup = 0;
	DSSN::HOTKV hotkv;
	printf("Number of Threads = %u, Number of Keys = %u\n", nthreads, nKeys);
 
	for (uint32_t i = 0; i< nthreads; i++) {
	  ThreadData* data = new ThreadData();
	  data->keyoffset = nkeysPerThread * i;
	  data->nkeys = nkeysPerThread;
	  data->kvs = &hotkv;
	  data->hb = this;
	  std::thread t(hotKVPutTest, data);
	  t.join();
	}
	printf("== Put() took %.3f s ==\n", Cycles::toSeconds(totalPutCycles));

	printf("    external avg: %lu ticks, %lu nsec\n", totalPutCycles / nKeys,
	       Cycles::toNanoseconds(totalPutCycles / nKeys));
	printf("    Operations/sec: %f\n", ((float)nKeys/Cycles::toSeconds(totalPutCycles)));
	if (baseTotalPutCycles) {
	  speedup = ((float)baseTotalPutCycles)/totalPutCycles;
	}
	printf("    Speedup: %f\n", speedup);

	if (nthreads == 1) {
	    baseTotalPutCycles = totalPutCycles;
	}
	totalUpdateCycles = 0;
	
	for (uint32_t i = 0; i< nthreads; i++) {
	  ThreadData* data = new ThreadData();
	  data->keyoffset = nkeysPerThread * i;
	  data->nkeys = nkeysPerThread;
	  data->kvs = &hotkv;
	  data->hb = this;
	  std::thread t(hotKVPutTest, data);
	  t.join();
	}
	printf("== Replace took %.3f s ==\n", Cycles::toSeconds(totalPutCycles));

	printf("    external avg: %lu ticks, %lu nsec\n", totalPutCycles / nKeys,
	       Cycles::toNanoseconds(totalPutCycles / nKeys));
	printf("    Operations/sec: %f\n", ((float)nKeys/Cycles::toSeconds(totalPutCycles)));
	

	for (uint32_t i = 0; i< nthreads; i++) {
	  ThreadData* data = new ThreadData();
	  data->keyoffset = nkeysPerThread * i;
	  data->nkeys = nkeysPerThread;
	  data->kvs = &hotkv;
	  data->hb = this;
	  std::thread t(hotKVGetTest, data);
	  t.join();
	}
	printf("== Get() took %.3f s ==\n", Cycles::toSeconds(totalGetCycles));

	printf("    external avg: %lu ticks, %lu nsec\n", totalGetCycles / nKeys,
	       Cycles::toNanoseconds(totalGetCycles / nKeys));
	printf("    Operations/sec: %f\n", ((float)nKeys/Cycles::toSeconds(totalGetCycles)));
	if (baseTotalGetCycles) {
	  speedup = ((float)baseTotalGetCycles)/totalGetCycles;
	}
	printf("    Speedup: %f\n", speedup);

	if (nthreads == 1) {
	    baseTotalGetCycles = totalGetCycles;
	}
	for (uint32_t i = 0; i< nthreads; i++) {
	  ThreadData* data = new ThreadData();
	  data->keyoffset = nkeysPerThread * i;
	  data->nkeys = nkeysPerThread;
	  data->kvs = &hotkv;
	  data->hb = this;
	  std::thread t(hotKVUpdateMetaTest, data);
	  t.join();
	}
	printf("== UpdateMetaData() took %.3f s ==\n", Cycles::toSeconds(totalUpdateCycles));

	printf("    external avg: %lu ticks, %lu nsec\n", totalUpdateCycles / nKeys,
	       Cycles::toNanoseconds(totalUpdateCycles / nKeys));
	printf("    Operations/sec: %f\n", ((float)nKeys/Cycles::toSeconds(totalUpdateCycles)));

	for (uint32_t i = 0; i< nthreads; i++) {
	  ThreadData* data = new ThreadData();
	  data->keyoffset = nkeysPerThread * i;
	  data->nkeys = nkeysPerThread;
	  data->kvs = &hotkv;
	  data->hb = this;
	  std::thread t(hotKVGetMetaTest, data);
	  t.join();
	}
	printf("== GetMetaData() took %.3f s ==\n", Cycles::toSeconds(totalGetMetaCycles));

	printf("    external avg: %lu ticks, %lu nsec\n", totalGetMetaCycles / nKeys,
	       Cycles::toNanoseconds(totalGetMetaCycles / nKeys));
	printf("    Operations/sec: %f\n", ((float)nKeys/Cycles::toSeconds(totalGetMetaCycles)));

	totalUpdateCycles = 0;
	totalGetCycles = 0;
	totalPutCycles = 0;
	totalGetMetaCycles = 0;
    }
    
  private:

    inline void
    hotKVPut(uint64_t keyoffset, uint64_t nkeys, DSSN::HOTKV* kvs)
    {
        uint64_t i;
	uint64_t insertCycles = Cycles::rdtsc();
	for (i = keyoffset; i < nkeys; i++) {
	    std::string key = std::to_string(i);
	    std::string value = std::to_string(i);
	    DSSN::dssnMeta meta;
	    kvs->put(key, value, meta);
	}
	i = Cycles::rdtsc() - insertCycles;
	totalPutCycles += i;
    }
  
    inline void
    hotKVGet(uint64_t keyoffset, uint64_t nkeys, DSSN::HOTKV* kvs)
    {
        uint64_t i;
	uint64_t lookupCycles = Cycles::rdtsc();
	bool success;
	for (i = keyoffset; i < nkeys; i++) {
	    DSSN::dssnMeta meta;
	    std::string key = std::to_string(i);
	    const std::string* value = kvs->get(key, meta);
	    if (value->length()> 0 && key == *value)
	        success = true;
	    else success = false;
	    assert(success);
	}
	i = Cycles::rdtsc() - lookupCycles;
	totalGetCycles += i;
    }

    inline void
    hotKVUpdateMeta(uint64_t keyoffset, uint64_t nkeys, DSSN::HOTKV* kvs)
    {
        uint64_t i;
	uint64_t lookupCycles = Cycles::rdtsc();
	bool result = false;
	for (i = keyoffset; i < nkeys; i++) {
	    DSSN::dssnMeta meta;
	    meta.cStamp = i;
	    std::string key = std::to_string(i);
	    result = kvs->updateMeta(key, meta);
	    assert(result);
	}
	i = Cycles::rdtsc() - lookupCycles;
	totalUpdateCycles += i;
    }

        inline void
    hotKVGetMeta(uint64_t keyoffset, uint64_t nkeys, DSSN::HOTKV* kvs)
    {
        uint64_t i;
	uint64_t lookupCycles = Cycles::rdtsc();
	bool result = false;
	for (i = keyoffset; i < nkeys; i++) {
	    DSSN::dssnMeta meta;
	    meta.cStamp = 0;
	    std::string key = std::to_string(i);
	    result = kvs->getMeta(key, meta);
	    assert(result);
	    assert(meta.cStamp == i);
	}
	i = Cycles::rdtsc() - lookupCycles;
	totalGetMetaCycles += i;
    }

    static void
    hotKVPutTest(struct ThreadData *data)
    {
        data->hb->hotKVPut(data->keyoffset, data->nkeys, data->kvs);
    }

    static void
    hotKVGetTest(struct ThreadData *data)
    {
        data->hb->hotKVGet(data->keyoffset, data->nkeys, data->kvs);
    }

    static void
    hotKVUpdateMetaTest(struct ThreadData *data)
    {
        data->hb->hotKVUpdateMeta(data->keyoffset, data->nkeys, data->kvs);
    }
    
    static void
    hotKVGetMetaTest(struct ThreadData *data)
    {
        data->hb->hotKVGetMeta(data->keyoffset, data->nkeys, data->kvs);
    }
    
    uint64_t totalGetCycles;
    uint64_t totalPutCycles;
    uint64_t totalUpdateCycles;
    uint64_t totalGetMetaCycles;
    uint64_t baseTotalGetCycles;
    uint64_t baseTotalPutCycles;
    uint64_t nKeys;
  };

  
} // namespace RAMCloud

int
main(int argc, char **argv)
{
    using namespace RAMCloud;

    Context context(true);

    uint64_t numberOfKeys;

    numberOfKeys = 1000000;
    RAMCloud::HOTBenchmark hb(numberOfKeys);
    
    for (uint32_t nthreads = 1; nthreads < 16; nthreads++) {
        hb.run(nthreads);
    }
    return 0;
}
