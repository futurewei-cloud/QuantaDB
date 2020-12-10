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
#include "intel-family.h"

uint32_t max_level, hygon_genuine, genuine_intel, authentic_amd, max_extended_level, crystal_hz;
unsigned long long tsc_hz;

static inline void native_cpuid(unsigned int *eax, unsigned int *ebx,
        unsigned int *ecx, unsigned int *edx)
{
    /* ecx is often an input as well as an output. */
    asm volatile(
        "cpuid;"
        : "=a" (*eax),
          "=b" (*ebx),
          "=c" (*ecx),
          "=d" (*edx)
        : "0" (*eax), "2" (*ecx));
}

static inline void __cpuid(uint32_t op, uint32_t &eax, uint32_t &ebx, uint32_t &ecx, uint32_t &edx)
{
    eax = op;
    native_cpuid(&eax, &ebx, &ecx, &edx);
}

unsigned int intel_model_duplicates(unsigned int model)
{

    switch(model) {
    case INTEL_FAM6_NEHALEM_EP: /* Core i7, Xeon 5500 series - Bloomfield, Gainstown NHM-EP */
    case INTEL_FAM6_NEHALEM:    /* Core i7 and i5 Processor - Clarksfield, Lynnfield, Jasper Forest */
    case 0x1F:  /* Core i7 and i5 Processor - Nehalem */
    case INTEL_FAM6_WESTMERE:   /* Westmere Client - Clarkdale, Arrandale */
    case INTEL_FAM6_WESTMERE_EP:    /* Westmere EP - Gulftown */
        return INTEL_FAM6_NEHALEM;

    case INTEL_FAM6_NEHALEM_EX: /* Nehalem-EX Xeon - Beckton */
    case INTEL_FAM6_WESTMERE_EX:    /* Westmere-EX Xeon - Eagleton */
        return INTEL_FAM6_NEHALEM_EX;

    case INTEL_FAM6_XEON_PHI_KNM:
        return INTEL_FAM6_XEON_PHI_KNL;

    case INTEL_FAM6_BROADWELL_X:
    case INTEL_FAM6_BROADWELL_D:    /* BDX-DE */
        return INTEL_FAM6_BROADWELL_X;

    case INTEL_FAM6_SKYLAKE_L:
    case INTEL_FAM6_SKYLAKE:
    case INTEL_FAM6_KABYLAKE_L:
    case INTEL_FAM6_KABYLAKE:
    case INTEL_FAM6_COMETLAKE_L:
    case INTEL_FAM6_COMETLAKE:
        return INTEL_FAM6_SKYLAKE_L;

    case INTEL_FAM6_ICELAKE_L:
    case INTEL_FAM6_ICELAKE_NNPI:
    case INTEL_FAM6_TIGERLAKE_L:
    case INTEL_FAM6_TIGERLAKE:
        return INTEL_FAM6_CANNONLAKE_L;

    case INTEL_FAM6_ATOM_TREMONT_D:
        return INTEL_FAM6_ATOM_GOLDMONT_D;

    case INTEL_FAM6_ATOM_TREMONT_L:
        return INTEL_FAM6_ATOM_TREMONT;

    case INTEL_FAM6_ICELAKE_X:
        return INTEL_FAM6_SKYLAKE_X;
    }
    return model;
}

void process_cpuid()
{
    unsigned int eax, ebx, ecx, edx;
    unsigned int fms, family, model, stepping, ecx_flags, edx_flags;
    unsigned int has_turbo;

    eax = ebx = ecx = edx = 0;

    __cpuid(0, max_level, ebx, ecx, edx);

    if (ebx == 0x756e6547 && ecx == 0x6c65746e && edx == 0x49656e69)
        genuine_intel = 1;
    else if (ebx == 0x68747541 && ecx == 0x444d4163 && edx == 0x69746e65)
        authentic_amd = 1;
    else if (ebx == 0x6f677948 && ecx == 0x656e6975 && edx == 0x6e65476e)
        hygon_genuine = 1;

    printf("CPUID(0): %.4s%.4s%.4s ",
            (char *)&ebx, (char *)&edx, (char *)&ecx);

    __cpuid(1, fms, ebx, ecx, edx);
    family = (fms >> 8) & 0xf;
    model = (fms >> 4) & 0xf;
    stepping = fms & 0xf;
    if (family == 0xf)
        family += (fms >> 20) & 0xff;
    if (family >= 6)
        model += ((fms >> 16) & 0xf) << 4;
    ecx_flags = ecx;
    edx_flags = edx;

    /*
     * check max extended function levels of CPUID.
     * This is needed to check for invariant TSC.
     * This check is valid for both Intel and AMD.
     */
    ebx = ecx = edx = 0;
    __cpuid(0x80000000, max_extended_level, ebx, ecx, edx);

    if (genuine_intel) {
        model = intel_model_duplicates(model);
        printf("model=%X\n", model);
    }

    if (max_level > 0x15) {
	    unsigned int eax_crystal;
        unsigned int ebx_tsc;

        /*
         * CPUID 15H TSC/Crystal ratio, possibly Crystal Hz
         */
        eax_crystal = ebx_tsc = crystal_hz = edx = 0;
        __cpuid(0x15, eax_crystal, ebx_tsc, crystal_hz, edx);

        if (ebx_tsc != 0) {

            if (ebx != 0)
                printf("CPUID(0x15): eax_crystal: %d ebx_tsc: %d ecx_crystal_hz: %d\n",
                    eax_crystal, ebx_tsc, crystal_hz);

            if (crystal_hz == 0)
                switch(model) {
                case INTEL_FAM6_SKYLAKE_L:  /* SKL */
                    crystal_hz = 24000000;  /* 24.0 MHz */
                    break;
                case INTEL_FAM6_SKYLAKE_X:
                case INTEL_FAM6_ATOM_GOLDMONT_D:    /* DNV */
                    crystal_hz = 25000000;  /* 25.0 MHz */
                    break;
                case INTEL_FAM6_ATOM_GOLDMONT:  /* BXT */
                case INTEL_FAM6_ATOM_GOLDMONT_PLUS:
                    crystal_hz = 19200000;  /* 19.2 MHz */
                    break;
                default:
                    crystal_hz = 0;
            }

            printf("crystal_hz=%d\n", crystal_hz);

            if (crystal_hz) {
                tsc_hz =  (unsigned long long) crystal_hz * ebx_tsc / eax_crystal;
                printf("TSC: %lld MHz (%d Hz * %d / %d / 1000000)\n",
                        tsc_hz / 1000000, crystal_hz, ebx_tsc,  eax_crystal);
            }
        }
    }
}


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

int main()
{
    unsigned int eax_crystal, ebx_tsc, edx, crystal_hz;

    eax_crystal = ebx_tsc = crystal_hz = edx = 0;
    __cpuid(0x15, eax_crystal, ebx_tsc, crystal_hz, edx);

    printf("eax_crystal=%d ebx_tsc=%d crystal_hz=%d edx=%d\n", eax_crystal,ebx_tsc,crystal_hz,edx);

    process_cpuid();

    cyclesPerSec = getCyclesPerSec();

    printf("cyclesPerSec=%f\n", cyclesPerSec);

    return 0;
}
