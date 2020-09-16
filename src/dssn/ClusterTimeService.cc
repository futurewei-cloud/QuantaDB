/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <sched.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <string>
#include <iostream>
#include "intel-family.h"
#include "Cycles.h"
#include "ClusterTimeService.h"


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

/* borrowed from linux kernel */
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

static double getTSCHz()
{
    uint32_t max_level, genuine_intel, max_extended_level, crystal_hz;
    double tsc_hz = 0;

    unsigned int ebx, ecx, edx;
    unsigned int fms, family, model;

    ebx = ecx = edx = 0;

    __cpuid(0, max_level, ebx, ecx, edx);

    if (ebx == 0x756e6547 && ecx == 0x6c65746e && edx == 0x49656e69)
        genuine_intel = 1;

    // printf("CPUID(0): %.4s%.4s%.4s ", (char *)&ebx, (char *)&edx, (char *)&ecx);

    __cpuid(1, fms, ebx, ecx, edx);
    family = (fms >> 8) & 0xf;
    model = (fms >> 4) & 0xf;
    if (family == 0xf)
        family += (fms >> 20) & 0xff;
    if (family >= 6)
        model += ((fms >> 16) & 0xf) << 4;

    /*
     * check max extended function levels of CPUID.
     * This is needed to check for invariant TSC.
     * This check is valid for both Intel and AMD.
     */
    ebx = ecx = edx = 0;
    __cpuid(0x80000000, max_extended_level, ebx, ecx, edx);

    if (genuine_intel) {
        model = intel_model_duplicates(model);
        // printf("model=%X\n", model);
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

            // if (ebx != 0) {
            //     printf("CPUID(0x15): eax_crystal: %d ebx_tsc: %d ecx_crystal_hz: %d\n",
            //         eax_crystal, ebx_tsc, crystal_hz);
            // }

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

            // printf("crystal_hz=%d\n", crystal_hz);

            if (crystal_hz) {
                tsc_hz =  (double) crystal_hz * ebx_tsc / eax_crystal;
                // printf("TSC: %lld MHz (%d Hz * %d / %d / 1000000)\n",
                //        tsc_hz / 1000000, crystal_hz, ebx_tsc,  eax_crystal);
            }
        }
    }

    if (tsc_hz == 0)
        tsc_hz = Cycles::perSecond();

    // printf("tsc_hz = %f\n", tsc_hz);

    return tsc_hz;
}

namespace DSSN {
using namespace RAMCloud;

#define TRACKER_SLEEP_USEC  1

void * ClusterTimeService::update_ts_tracker(void *arg)
{
    ClusterTimeService *ctsp = (ClusterTimeService*)arg;
    ts_tracker_t *tp = ctsp->tp;


    // See if a tracker already running, by detecting its heartbeat.
    uint32_t heartbeat;
    int loop = 5;
    while(loop-- > 0) {
        heartbeat = tp->heartbeat;
        usleep(TRACKER_SLEEP_USEC+10);
        if (heartbeat != tp->heartbeat) { // alive
            ctsp->tracker_init = true;
            return NULL;
        }
    }
    
    // Init ts_tracker
    tp->tracker_id = ctsp->my_tracker_id;
    tp->pingpong = 0;
    tp->cyclesPerSec = getTSCHz();
    tp->nt[0].last_clock       = tp->nt[1].last_clock       = getnsec();
    tp->nt[0].last_clock_tsc   = tp->nt[1].last_clock_tsc   = rdtscp();

    //
    ctsp->tracker_init = true;
    ctsp->uctr = ctsp->nctr = 0;

    // uint64_t max_delay = 0;
    while (ctsp->thread_run_run && tp->tracker_id == ctsp->my_tracker_id) {
        tp->heartbeat++;

        nt_pair_t *ntp = &tp->nt[tp->pingpong];
        uint64_t tsc;
        uint64_t nsec = getnsec(&tsc);
        if (nsec >
            (ntp->last_clock + Cycles::toNanoseconds(tsc - ntp->last_clock_tsc, tp->cyclesPerSec))) {
            int nidx = 1 - tp->pingpong;
            tp->nt[nidx].last_clock_tsc = tsc;
            tp->nt[nidx].last_clock = nsec;
            tp->pingpong = nidx;
            ctsp->uctr++;
        } else
            ctsp->nctr++;

        if (tp->tracker_id == ctsp->my_tracker_id) {
            Cycles::sleep(TRACKER_SLEEP_USEC);
        }
    }
    return NULL;
}

ClusterTimeService::ClusterTimeService()
{
    int fd;

    // attached to shared delta tracker
    if ((fd = shm_open(TS_TRACKER_NAME, O_CREAT|O_RDWR, 0666)) == -1) {
        printf("Fatal Error: shm_open failed. Errno=%d\n", errno);
        *(int *)0 = 0;  // panic
    }

    struct stat st;
    int ret = fstat(fd, &st); assert(ret == 0); 
    if (st.st_size == 0) {
        ret = ftruncate(fd, sizeof(ts_tracker_t));
        assert(ret == 0);
    }

    tp = (ts_tracker_t *)mmap(NULL, sizeof(ts_tracker_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    assert(tp);

    close(fd);

    // Start tracker thread
    thread_run_run = true;
    tracker_init = false;
	pthread_create(&tid, NULL, update_ts_tracker, (void *)this);
    srand(tid);
    my_tracker_id = rand();

    while(!tracker_init)
        usleep(1);
}

ClusterTimeService::~ClusterTimeService()
{
    if (thread_run_run && tp->tracker_id == my_tracker_id) {
        void * ret;
        thread_run_run = false;
	    pthread_join(tid, &ret);
    }
    // printf("~ClusterTimeService uctr=%ld nctr=%ld\n", uctr, nctr);
    munmap(tp, sizeof(ts_tracker_t));
}

} // DSSN

