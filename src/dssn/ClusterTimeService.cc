/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#include <sys/socket.h>
#include <sys/time.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <assert.h>
#include <iostream>
#include "Cycles.h"
#include "ClusterTimeService.h"

namespace DSSN {
using namespace RAMCloud;

#define TRACKER_SLEEP_USEC  1

void * ClusterTimeService::update_ts_tracker(void *arg)
{
    ClusterTimeService *ctsp = (ClusterTimeService*)arg;
    ts_tracker_t *tp = ctsp->tp;

    // Set ts->tracker_id
    tp->tracker_id = ctsp->my_tracker_id;

    // Wait for other tracker's heartbeat to stop
    uint32_t heartbeat;
    do {
        heartbeat = tp->heartbeat;
        usleep(TRACKER_SLEEP_USEC+10);
    } while (heartbeat != tp->heartbeat); // still alive
    
    // Init ts_tracker
    tp->pingpong = 0;
    tp->cyclesPerSec = Cycles::perSecond();
    tp->nt[0].last_clock       = tp->nt[1].last_clock       = getnsec();
    tp->nt[0].last_clock_tsc   = tp->nt[1].last_clock_tsc   = rdtscp();

    //
    ctsp->tracker_init = true;
    ctsp->uctr = ctsp->nctr = 0;

    // uint64_t max_delay = 0;
    while (ctsp->thread_run_run && tp->tracker_id == ctsp->my_tracker_id) {
        tp->heartbeat++;

        nt_pair_t *ntp = &tp->nt[tp->pingpong];
        uint64_t tsc; // = rdtscp();
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

