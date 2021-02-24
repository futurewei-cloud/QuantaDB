/* Copyright 2020 Futurewei Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Copyright (c) 2020  Futurewei Technologies, Inc.
 */
#include <algorithm>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>
#include <dirent.h>
#include <assert.h>
#include <string.h>
#include <iostream>
#include <errno.h>
#include "Slab.h"
#include <x86intrin.h>

using namespace QDB;
using std::cout;
using std::endl;

// Globals
uint32_t loop = 1024*1024;
void ** mptr;
int *msize;

inline void Exit(std::string msg, std::string err) { printf("%s", msg.c_str()); exit (1); }

static inline u_int64_t rdtscp(u_int32_t &aux)
{
    u_int64_t rax,rdx;
    asm volatile ( "rdtscp\n" : "=a" (rax), "=d" (rdx), "=c" (aux) : : );
    return (rdx << 32) + rax;
}

double cyclesPerSec = 0;

double getCyclesPerSec() {
    uint32_t core;
    // Compute the frequency of the fine-grained CPU timer: to do this,
    // take parallel time readings using both rdtsc and gettimeofday.
    // After 10ms have elapsed, take the ratio between these readings.

    struct timeval startTime, stopTime;
    uint64_t startCycles, stopCycles, micros;
    double cyclesPerSec, oldCycles;

    // There is one tricky aspect, which is that we could get interrupted
    // between calling gettimeofday and reading the cycle counter, in which
    // case we won't have corresponding readings.  To handle this (unlikely)
    // case, compute the overall result repeatedly, and wait until we get
    // two successive calculations that are within 0.1% of each other.
    oldCycles = 0;
    while (1) {
        if (gettimeofday(&startTime, NULL) != 0) {
            Exit("couldn't read clock: %s", strerror(errno));
        }
        startCycles = rdtscp(core);
        while (1) {
            if (gettimeofday(&stopTime, NULL) != 0) {
                Exit("Cycles::init couldn't read clock: %s",
                        strerror(errno));
            }
            stopCycles = rdtscp(core);
            micros = (stopTime.tv_usec - startTime.tv_usec) +
                    (stopTime.tv_sec - startTime.tv_sec)*1000000;
            if (micros > 10000) {
                cyclesPerSec = static_cast<double>(stopCycles - startCycles);
                cyclesPerSec = 1000000.0*cyclesPerSec/
                        static_cast<double>(micros);
                break;
            }
        }
        double delta = cyclesPerSec/1000.0;
        if ((oldCycles > (cyclesPerSec - delta)) &&
                (oldCycles < (cyclesPerSec + delta))) {
            return cyclesPerSec;
        }
        oldCycles = cyclesPerSec;
    }
    return cyclesPerSec;
}

inline uint64_t toNanoSeconds(uint64_t cycles)
{
    return (uint64_t) (1e09*static_cast<double>(cycles)/cyclesPerSec + 0.5);
}

typedef void *(*thread_func_t)(void*);

uint64_t run_parallel(int nthreads, int run_time/* #sec */, thread_func_t func)
{
	pthread_t tid[nthreads];
	// 
	bool thread_run_run = true;
	for (auto idx = 0; idx < nthreads; idx++) {
	    //pthread_create(&tid[idx], NULL, func, (uint64_t)idx, bool* thread_run_run);
	    pthread_create(&tid[idx], NULL, func, (void*)(uint64_t)idx);
	}

	sleep(run_time);
	thread_run_run = false;

	uint64_t total = 0;

	for (auto idx = 0; idx < nthreads; idx++) {
		void * ret;
	    pthread_join(tid[idx], &ret);
		total += (uint64_t)ret;
	}

	return total;
}


void* bench_slab(void *arg)
{
    uint32_t tid = (uint32_t)(uint64_t)arg;
    uint32_t off = tid * loop;
    Slab * slab = new Slab(32, 1024*1024*16);

    uint64_t get_latency, put_latency;

    uint64_t start, stop;
    start = __rdtsc();
    for (uint32_t i = 0; i < loop; i += 16) {
        mptr[off+i+0] = slab->get();
        mptr[off+i+1] = slab->get();
        mptr[off+i+2] = slab->get();
        mptr[off+i+3] = slab->get();
        mptr[off+i+4] = slab->get();
        mptr[off+i+5] = slab->get();
        mptr[off+i+6] = slab->get();
        mptr[off+i+7] = slab->get();
        mptr[off+i+8] = slab->get();
        mptr[off+i+9] = slab->get();
        mptr[off+i+10] = slab->get();
        mptr[off+i+11] = slab->get();
        mptr[off+i+12] = slab->get();
        mptr[off+i+13] = slab->get();
        mptr[off+i+14] = slab->get();
        mptr[off+i+15] = slab->get();
    }
    stop = __rdtsc();

    get_latency = toNanoSeconds(stop - start)/loop;

    start = __rdtsc();
    for (uint32_t i = 0; i < loop; i += 16) {
        slab->put(mptr[off+i+0]);
        slab->put(mptr[off+i+1]);
        slab->put(mptr[off+i+2]);
        slab->put(mptr[off+i+3]);
        slab->put(mptr[off+i+4]);
        slab->put(mptr[off+i+5]);
        slab->put(mptr[off+i+6]);
        slab->put(mptr[off+i+7]);
        slab->put(mptr[off+i+8]);
        slab->put(mptr[off+i+9]);
        slab->put(mptr[off+i+10]);
        slab->put(mptr[off+i+11]);
        slab->put(mptr[off+i+12]);
        slab->put(mptr[off+i+13]);
        slab->put(mptr[off+i+14]);
        slab->put(mptr[off+i+15]);
    }
    stop = __rdtsc();

    put_latency = toNanoSeconds(stop - start)/loop;

    return (void*)((get_latency << 32) + put_latency);
}

void* bench_malloc(void *arg)
{
    uint32_t tid = (uint32_t)(uint64_t)arg;
    uint32_t off = tid * loop;
    uint64_t malloc_latency, free_latency;
    uint64_t start, stop;
    start = __rdtsc();
    for (uint32_t i = 0; i < loop; i += 16) {
        mptr[off+i+0]  = malloc(msize[off+i+0]);
        mptr[off+i+1]  = malloc(msize[off+i+1]);
        mptr[off+i+2]  = malloc(msize[off+i+2]);
        mptr[off+i+3]  = malloc(msize[off+i+3]);
        mptr[off+i+4]  = malloc(msize[off+i+4]);
        mptr[off+i+5]  = malloc(msize[off+i+5]);
        mptr[off+i+6]  = malloc(msize[off+i+6]);
        mptr[off+i+7]  = malloc(msize[off+i+7]);
        mptr[off+i+8]  = malloc(msize[off+i+8]);
        mptr[off+i+9]  = malloc(msize[off+i+9]);
        mptr[off+i+10] = malloc(msize[off+i+10]);
        mptr[off+i+10] = malloc(msize[off+i+11]);
        mptr[off+i+10] = malloc(msize[off+i+12]);
        mptr[off+i+10] = malloc(msize[off+i+13]);
        mptr[off+i+10] = malloc(msize[off+i+14]);
        mptr[off+i+10] = malloc(msize[off+i+15]);
    }
    stop = __rdtsc();
    malloc_latency = toNanoSeconds(stop - start)/loop;

    start = __rdtsc();
    for (uint32_t i = 0; i < loop; i += 16) {
        free(mptr[off+i+0]);
        free(mptr[off+i+1]);
        free(mptr[off+i+2]);
        free(mptr[off+i+3]);
        free(mptr[off+i+4]);
        free(mptr[off+i+5]);
        free(mptr[off+i+6]);
        free(mptr[off+i+7]);
        free(mptr[off+i+8]);
        free(mptr[off+i+9]);
        free(mptr[off+i+10]);
        free(mptr[off+i+11]);
        free(mptr[off+i+12]);
        free(mptr[off+i+13]);
        free(mptr[off+i+14]);
        free(mptr[off+i+15]);
    }
    stop = __rdtsc();
    free_latency = toNanoSeconds(stop - start)/loop;

    return (void*)((malloc_latency << 32) + free_latency);
}

void* bench_new(void *arg)
{
    uint32_t tid = (uint32_t)(uint64_t)arg;
    uint32_t off = tid * loop;
    uint64_t new_latency, delete_latency;
    uint64_t start, stop;
    start = __rdtsc();
    for (uint32_t i = 0; i < loop; i += 16) {
        mptr[off+i+0]  = new char[msize[off+i+0]];
        mptr[off+i+0]  = new char[msize[off+i+1]];
        mptr[off+i+0]  = new char[msize[off+i+2]];
        mptr[off+i+0]  = new char[msize[off+i+3]];
        mptr[off+i+0]  = new char[msize[off+i+4]];
        mptr[off+i+0]  = new char[msize[off+i+5]];
        mptr[off+i+0]  = new char[msize[off+i+6]];
        mptr[off+i+0]  = new char[msize[off+i+7]];
        mptr[off+i+0]  = new char[msize[off+i+8]];
        mptr[off+i+0]  = new char[msize[off+i+9]];
        mptr[off+i+10] = new char[msize[off+i+10]];
        mptr[off+i+10] = new char[msize[off+i+11]];
        mptr[off+i+10] = new char[msize[off+i+12]];
        mptr[off+i+10] = new char[msize[off+i+13]];
        mptr[off+i+10] = new char[msize[off+i+14]];
        mptr[off+i+10] = new char[msize[off+i+15]];
    }
    stop = __rdtsc();
    new_latency = toNanoSeconds(stop - start)/loop;

    start = __rdtsc();
    for (uint32_t i = 0; i < loop; i += 16) {
        delete((char*)mptr[off+i+0]);
        delete((char*)mptr[off+i+1]);
        delete((char*)mptr[off+i+2]);
        delete((char*)mptr[off+i+3]);
        delete((char*)mptr[off+i+4]);
        delete((char*)mptr[off+i+5]);
        delete((char*)mptr[off+i+6]);
        delete((char*)mptr[off+i+7]);
        delete((char*)mptr[off+i+8]);
        delete((char*)mptr[off+i+9]);
        delete((char*)mptr[off+i+10]);
        delete((char*)mptr[off+i+11]);
        delete((char*)mptr[off+i+12]);
        delete((char*)mptr[off+i+13]);
        delete((char*)mptr[off+i+14]);
        delete((char*)mptr[off+i+15]);
    }
    stop = __rdtsc();
    delete_latency = toNanoSeconds(stop - start)/loop;
    return (void*)((new_latency << 32) + delete_latency);
}

void Usage(char *prog)
{
    printf("Usage %s <malloc|new|slab> <size|'variable'> [nthreads]  # test malloc/free allocator\n", prog);
    printf("Eg:   %s slab   32     # test slab allocator fixed 32 byte, single thread (default)\n", prog);
    printf("Eg:   %s new    32 10  # test new/delete fixed 32 byte, 10 threads \n", prog);
    printf("Eg:   %s malloc 32 10  # test malloc/free fixed 32 byte, 10 threads \n", prog);
    printf("Eg:   %s new    var 10 # test malloc/free variable size, 10 threads \n", prog);
    exit (1);
}

int main(int ac, char *av[])
{
    if (ac < 3)
        Usage(av[0]);

    // Parse cmd
    char *allocator = av[1];
    uint32_t dsize = atoi(av[2]);
    uint32_t nthreads = (ac >= 4)? atoi(av[3]) : 1;

    // Init
    cyclesPerSec = getCyclesPerSec();
    mptr = (void**)malloc(sizeof(void*) * loop * nthreads);
    msize = (int*)malloc(sizeof(uint32_t)*loop * nthreads);

    for (uint32_t idx = 0; idx < loop; idx++) {
        msize[idx] = (dsize > 0)? dsize : 1 + (rand() % 1024*16);
    }

    uint64_t total;
    std::string get, put;
    if (strcasecmp(allocator, "slab") == 0) {
        if (!dsize) {
            cout << "\t" << "N/A" << endl;
            exit (0);
        }
        total = run_parallel(nthreads, 5/* #sec */, bench_slab);
        get = "get   ";
        put = "put   ";
    } else if (strcasecmp(allocator, "malloc") == 0) {
        total = run_parallel(nthreads, 5/* #sec */, bench_malloc);
        get = "malloc";
        put = "free  ";
    } else if (strcasecmp(allocator, "new") == 0) {
        total = run_parallel(nthreads, 5/* #sec */, bench_new);
        get = "new   ";
        put = "delete";
    } else
        Usage(av[0]);

    free(mptr);

    uint32_t ave_get = (total >> 32)/nthreads;
    uint32_t ave_put = (total & 0xFFFFFFFF) / nthreads;

    cout << "\t" << get << ": " << ave_get << " nsec" << endl;
    cout << "\t" << put << ": " << ave_put << " nsec" << endl;
}
