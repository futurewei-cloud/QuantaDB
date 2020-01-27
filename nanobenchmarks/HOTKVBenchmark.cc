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


struct ThreadData {
    ThreadData() {};
    uint64_t keyoffset;
    uint64_t nkeys;
    DSSN::HOTKV* kvs;
};

namespace RAMCloud {
uint64_t TotalGetCycles = 0;
uint64_t TotalPutCycles = 0;
uint64_t BaseTotalGetCycles = 0;
uint64_t BaseTotalPutCycles = 0;

inline void
hotKVPut(uint64_t keyoffset, uint64_t nkeys, DSSN::HOTKV* kvs)
{
    uint64_t i;
    uint64_t insertCycles = Cycles::rdtsc();
    for (i = keyoffset; i < nkeys; i++) {
        std::string key = std::to_string(i);
	std::string value = std::to_string(i);
	kvs->put(key, value, 0);
    }
    i = Cycles::rdtsc() - insertCycles;
    TotalPutCycles += i;
}
  
inline void
hotKVGet(uint64_t keyoffset, uint64_t nkeys, DSSN::HOTKV* kvs)
{
    uint64_t i;
    uint64_t lookupCycles = Cycles::rdtsc();
    bool success;
    for (i = keyoffset; i < nkeys; i++) {
        uint64_t meta = 0;
        std::string key = std::to_string(i);
	std::string value = kvs->get(key, &meta);
	if (value.length()> 0 && key == value)
	  success = true;
	else success = false;
	assert(success);
    }
    i = Cycles::rdtsc() - lookupCycles;
    TotalGetCycles += i;
}

void
hotKVPutTest(struct ThreadData *data)
{
  hotKVPut(data->keyoffset, data->nkeys, data->kvs);
}

void
hotKVGetTest(struct ThreadData *data)
{
  hotKVGet(data->keyoffset, data->nkeys, data->kvs);
}

void
hotKVBenchmarkMt(uint64_t nkeys, uint64_t nthreads)
{
    uint64_t nkeysPerThread = nkeys/nthreads;
    float speedup = 0;
    DSSN::HOTKV hotkv;
    printf("Number of Threads = %u, Number of Keys = %u\n", nthreads, nkeys);
    for (uint32_t i = 0; i< nthreads; i++) {
        ThreadData* data = new ThreadData();
	data->keyoffset = nkeysPerThread * i;
	data->nkeys = nkeysPerThread;
	data->kvs = &hotkv;
	std::thread t(hotKVPutTest, data);
	t.join();
    }
    printf("== Put() took %.3f s ==\n", Cycles::toSeconds(TotalPutCycles));

    printf("    external avg: %lu ticks, %lu nsec\n", TotalPutCycles / nkeys,
	   Cycles::toNanoseconds(TotalPutCycles / nkeys));
    printf("    Operations/sec: %f\n", ((float)nkeys/Cycles::toSeconds(TotalPutCycles)));
    if (BaseTotalPutCycles) {
        speedup = ((float)BaseTotalPutCycles)/TotalPutCycles;
    }
    printf("    Speedup: %f\n", speedup);

    for (uint32_t i = 0; i< nthreads; i++) {
        ThreadData* data = new ThreadData();
	data->keyoffset = nkeysPerThread * i;
	data->nkeys = nkeysPerThread;
	data->kvs = &hotkv;
	std::thread t(hotKVGetTest, data);
	t.join();
    }
    printf("== Get() took %.3f s ==\n", Cycles::toSeconds(TotalGetCycles));

    printf("    external avg: %lu ticks, %lu nsec\n", TotalGetCycles / nkeys,
	   Cycles::toNanoseconds(TotalGetCycles / nkeys));
    printf("    Operations/sec: %f\n", ((float)nkeys/Cycles::toSeconds(TotalGetCycles)));
    if (BaseTotalGetCycles) {
        speedup = ((float)BaseTotalGetCycles)/TotalGetCycles;
    }
    printf("    Speedup: %f\n", speedup);
}
  
} // namespace RAMCloud

int
main(int argc, char **argv)
{
    using namespace RAMCloud;

    Context context(true);

    uint64_t numberOfKeys;

    numberOfKeys = 10000000;

    for (uint32_t nthreads = 1; nthreads < 16; nthreads++) {
        hotKVBenchmarkMt(numberOfKeys, nthreads);
	if (nthreads == 1) {
	    BaseTotalGetCycles = TotalGetCycles;
	    BaseTotalPutCycles = TotalPutCycles;
	}
	TotalGetCycles = 0;
	TotalPutCycles = 0;

    }
    return 0;
}
