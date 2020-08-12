/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#include <x86intrin.h>
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

void * ClusterTimeService::update_ts_tracker(void *arg)
{
    ClusterTimeService *ctsp = (ClusterTimeService*)arg;
    ts_tracker_t *tp = ctsp->tp;

    // See if another tracker may be already running
    if (tp->idx <= 1) {
        uint64_t nsec = tp->nt[tp->idx].last_nsec;
        usleep(3);
        if (nsec != tp->nt[tp->idx].last_nsec) {
            ctsp->thread_run_run = false;
            ctsp->tracker_init   = true;
            return NULL;
        }
    }

    // Init
    tp->nt[0].last_nsec = tp->nt[1].last_nsec = getnsec();
    tp->nt[0].ctr       = tp->nt[1].ctr       = 0;
    tp->idx = 0;
    ctsp->tracker_init = true;

    while (ctsp->thread_run_run) {
        nt_pair_t *ntp = &tp->nt[tp->idx];
        uint64_t nsec = getnsec();

        if (nsec > ntp->last_nsec + ntp->ctr) {
            int nidx = 1 - tp->idx;
            tp->nt[nidx].last_nsec = nsec;
            tp->nt[nidx].ctr = 0;
            tp->idx = nidx;
        }
        usleep(2);
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

    while(!tracker_init)
        usleep(1);
}

ClusterTimeService::~ClusterTimeService()
{
    if (thread_run_run) {
        void * ret;
        thread_run_run = false;
	    pthread_join(tid, &ret);
    }
    munmap(tp, sizeof(ts_tracker_t));
}

} // DSSN

