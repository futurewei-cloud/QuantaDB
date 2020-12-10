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

#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <sched.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/time.h>
#include <errno.h>
#include <string>

#define MAX_CORE 32
sem_t child, parent;
bool child_start = false;

static inline u_int64_t rdtscp(u_int32_t &aux)
{
    u_int64_t rax,rdx;
    asm volatile ( "rdtscp\n" : "=a" (rax), "=d" (rdx), "=c" (aux) : : );
    return (rdx << 32) + rax;
}

static inline uint64_t getnsec()
{
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

void Exit(std::string msg, std::string err)
{
    exit (1);
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
            Exit("Cycles::init couldn't read clock: %s", strerror(errno));
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

inline uint64_t toNanoSecond(uint64_t cycles)
{
    return (uint64_t) (1e09*static_cast<double>(cycles)/cyclesPerSec + 0.5);
}

void *func(void *arg)
{
    u_int64_t tsc_start, tsc_end;
    u_int32_t core;
    sem_post(&parent);
#ifdef CHILD_USE_SEM
    sem_wait(&child);
#else
    while (child_start == false) {;}
#endif
    tsc_start = rdtscp(core);
    uint64_t clock_start = getnsec();
    int i,j; for (i=0, j=0; i< 65536; i++) { j+= i*i; }
    tsc_end = rdtscp(core);
    uint64_t clock_end = getnsec();

    u_int64_t tsc_diff = tsc_end - tsc_start;
    uint64_t clock_diff = clock_end - clock_start;

    float scale = (float) tsc_diff / (float) clock_diff;

    uint64_t boot_time_nsec_start = clock_start - toNanoSecond(tsc_start);
    uint64_t boot_time_nsec_end   = clock_end   - toNanoSecond(tsc_end);
    int boot_time_diff = boot_time_nsec_start - boot_time_nsec_end;

    printf("t[%02lx] c[%04x] tsc %08ld %08ld clock %08ld %08ld tsc diff %08ld clock diff %08ld %1.4f boot_st %ld boot_en %ld boot dif %d\n",
        (u_int64_t)arg, core, tsc_start, tsc_end, clock_start, clock_end,
        tsc_diff, clock_diff, scale, boot_time_nsec_start, boot_time_nsec_end, boot_time_diff);

}

int main()
{
    pthread_t threads[MAX_CORE];
    sem_init(&child, 0, 0);    //shared within process, all blocked
    sem_init(&parent, 0, 0);

    cyclesPerSec = getCyclesPerSec();

    for (int i=0; i<MAX_CORE; i++) {
        pthread_create(&threads[i], NULL, func, (void*)(u_int64_t)i);
    }

    //wait for all child to be ready.
    for (int i=0; i<MAX_CORE; i++) {
        sem_wait(&parent);
    } 

#ifdef CHILD_USE_SEM
    for (int i=0; i<MAX_CORE; i++) {
        sem_post(&child);
    }
#else
    child_start = true;
#endif

    for (int i=0; i<MAX_CORE; i++) {
        pthread_join(threads[i], NULL);
    }

    return 0;
}
