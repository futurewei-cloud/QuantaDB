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

void Usage(char *prog)
{
    printf("Usage %s \n", prog);
    exit (1);
}

void Exit(std::string msg, std::string err)
{
    exit (1);
}

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

int main(int ac, char *av[])
{
    cyclesPerSec = getCyclesPerSec();
    Slab * slab = new Slab(32, 1024*1024*16);

    uint32_t loop = 1024*1024;
    void ** foo = (void**)malloc(sizeof(void*) * loop);
    uint64_t start, stop;
    start = __rdtsc();
    for (uint32_t i = 0; i < loop; i += 16) {
        foo[i+0] = slab->get();
        foo[i+1] = slab->get();
        foo[i+2] = slab->get();
        foo[i+3] = slab->get();
        foo[i+4] = slab->get();
        foo[i+5] = slab->get();
        foo[i+6] = slab->get();
        foo[i+7] = slab->get();
        foo[i+8] = slab->get();
        foo[i+9] = slab->get();
        foo[i+10] = slab->get();
        foo[i+11] = slab->get();
        foo[i+12] = slab->get();
        foo[i+13] = slab->get();
        foo[i+14] = slab->get();
        foo[i+15] = slab->get();
    }
    stop = __rdtsc();
    cout << "Slab get latency: "
    << toNanoSeconds(stop - start)/loop << " nsec "
    << " free cnt: " << slab->count_free() 
    << std::endl;

    start = __rdtsc();
    for (uint32_t i = 0; i < loop; i += 16) {
        slab->put(foo[i+0]);
        slab->put(foo[i+1]);
        slab->put(foo[i+2]);
        slab->put(foo[i+3]);
        slab->put(foo[i+4]);
        slab->put(foo[i+5]);
        slab->put(foo[i+6]);
        slab->put(foo[i+7]);
        slab->put(foo[i+8]);
        slab->put(foo[i+9]);
        slab->put(foo[i+10]);
        slab->put(foo[i+11]);
        slab->put(foo[i+12]);
        slab->put(foo[i+13]);
        slab->put(foo[i+14]);
        slab->put(foo[i+15]);
    }
    stop = __rdtsc();
    cout << "Slab put latency: "
    << toNanoSeconds(stop - start)/loop << " nsec "
    << " free cnt: " << slab->count_free() 
    << std::endl;

    start = __rdtsc();
    for (uint32_t i = 0; i < loop; i += 16) {
        foo[i+0] = malloc(32);
        foo[i+1] = malloc(32);
        foo[i+2] = malloc(32);
        foo[i+3] = malloc(32);
        foo[i+4] = malloc(32);
        foo[i+5] = malloc(32);
        foo[i+6] = malloc(32);
        foo[i+7] = malloc(32);
        foo[i+8] = malloc(32);
        foo[i+9] = malloc(32);
        foo[i+10] = malloc(32);
        foo[i+11] = malloc(32);
        foo[i+12] = malloc(32);
        foo[i+13] = malloc(32);
        foo[i+14] = malloc(32);
        foo[i+15] = malloc(32);
    }
    stop = __rdtsc();
    cout << "malloc() latency: "
    << toNanoSeconds(stop - start)/loop << " nsec "
    << std::endl;

    start = __rdtsc();
    for (uint32_t i = 0; i < loop; i += 16) {
        free(foo[i+0]);
        free(foo[i+1]);
        free(foo[i+2]);
        free(foo[i+3]);
        free(foo[i+4]);
        free(foo[i+5]);
        free(foo[i+6]);
        free(foo[i+7]);
        free(foo[i+8]);
        free(foo[i+9]);
        free(foo[i+10]);
        free(foo[i+11]);
        free(foo[i+12]);
        free(foo[i+13]);
        free(foo[i+14]);
        free(foo[i+15]);
    }
    stop = __rdtsc();
    cout << "free() latency: "
    << toNanoSeconds(stop - start)/loop << " nsec "
    << std::endl;

    start = __rdtsc();
    for (uint32_t i = 0; i < loop; i += 16) {
        foo[i+0] = new(char[32]);
        foo[i+1] = new(char[32]);
        foo[i+2] = new(char[32]);
        foo[i+3] = new(char[32]);
        foo[i+4] = new(char[32]);
        foo[i+5] = new(char[32]);
        foo[i+6] = new(char[32]);
        foo[i+7] = new(char[32]);
        foo[i+8] = new(char[32]);
        foo[i+9] = new(char[32]);
        foo[i+10] = new(char[32]);
        foo[i+11] = new(char[32]);
        foo[i+12] = new(char[32]);
        foo[i+13] = new(char[32]);
        foo[i+14] = new(char[32]);
        foo[i+15] = new(char[32]);
    }
    stop = __rdtsc();
    cout << "new() latency: "
    << toNanoSeconds(stop - start)/loop << " nsec "
    << std::endl;

    start = __rdtsc();
    for (uint32_t i = 0; i < loop; i += 16) {
        delete((char*)foo[i+0]);
        delete((char*)foo[i+1]);
        delete((char*)foo[i+2]);
        delete((char*)foo[i+3]);
        delete((char*)foo[i+4]);
        delete((char*)foo[i+5]);
        delete((char*)foo[i+6]);
        delete((char*)foo[i+7]);
        delete((char*)foo[i+8]);
        delete((char*)foo[i+9]);
        delete((char*)foo[i+10]);
        delete((char*)foo[i+11]);
        delete((char*)foo[i+12]);
        delete((char*)foo[i+13]);
        delete((char*)foo[i+14]);
        delete((char*)foo[i+15]);
    }
    stop = __rdtsc();
    cout << "delete latency: "
    << toNanoSeconds(stop - start)/loop << " nsec "
    << std::endl;
}
