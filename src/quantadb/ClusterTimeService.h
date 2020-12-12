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

#pragma once
#include <x86intrin.h>
#include <fcntl.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <atomic>
#include <iostream>
#include "Cycles.h"

using namespace RAMCloud;

namespace QDB {
/**
 * QDB Cluster Time Service:
 * - High speed and high precision (nano-second) cluster time service
 * - Issues cluster unique time stamp (logical time stamp, explained below)
 * - Issues node local time stamp (real clock time stamp) in nanosecond precision.
 * - Convertion utility converse between logical time stamp and the corresponding real time.
 *
 * QDB Cluster Time Service relies on an underlying time synchronization module to sync time to sub-microsecond
 * precision. There are a few existing such protocols. In this implementation, we use the PTP (IEEE1588) protocol
 * to synchronize system clocks in the cluster.
 *
 * A logical time-stamp is a 64-bit value consisted of <54-bit real clock time in nano-sec><10-bit node id>.
 * A logical time-stamp is monotonically increasing and unique across the QDB cluster. 
 *
 * The 10-bit node id field ensures logical time stamp is unique among the cluster nodes. 10 bits implies a
 * maximum of 1024 nodes can be supported. 
 *
 * Node id must be unique and is automatically assigned at ClusterTimeService class instantiation time.
 * Ina production system, node id assignment could be done in an automated fashion (such as plug-in to a
 * cluster node management sw like zookeeper).  In this POC implementation, a light-weight approach is used
 * by picking up the last number of the IP address of the PTP interface port.
 *
 * This implementation depends on PTP to synchronize system closks of all nodes in a cluster.
 *
 * There are two linux/unix system library functions to get the system time. One is gettimeofday(3) which is 
 * fast (about ~25ns) but only returns micro-second precision. The other  is clock_gettime(3) which returns
 * nanoseconds time stamp, yet could take ~500ns per call.
 *
 * To generate fast nanosecond time stamps, ClusterTimeService uses a background deamon process (or can also be
 * a background thread which is easier for deployment and testing purpose). The background thread maintais a
 * shared memory area which holds a nano-seond system timestamp along with a TSC counter which notes the cputime
 * when the system timestamp was last updated. The ClusterTimeService library reads the nano-second system
 * timestamp on the shared memory and uses TSC to calculate how much time had passed since the last update to
 * get the current time.
 *
 * EXTRA NOTE.
 * Using TSC to calculate time is hard to be reliable.
 * ref: http://oliveryang.net/2015/09/pitfalls-of-TSC-usage/#312-tsc-sync-behaviors-on-smp-system. 
 *
 * An alternative way to use an atomic counter to replace the TSC counter. This way could generate reliable
 * monotonically increasing timestamps.
 */

// ClusterTimeService
class ClusterTimeService {
    public:
    ClusterTimeService();
    ~ClusterTimeService();

    // return a local system clock time stamp
    inline uint64_t getLocalTime()
    {
        return getnsec();
        /*
        nt_pair_t *ntp = &tp->nt[tp->pingpong];
        uint64_t last_clock      = ntp->last_clock;
        uint64_t last_clock_tsc  = ntp->last_clock_tsc;
        return last_clock + Cycles::toNanoseconds(rdtscp() - last_clock_tsc, tp->cyclesPerSec);
        */
    }

    private:

    // static void* update_ts_tracker(void *);

    // Fast (~25ns) about only at usec precision
    static inline uint64_t getusec()
    {
	    struct timeval tv;
	    gettimeofday(&tv,NULL);
	    return (uint64_t)(1000000*tv.tv_sec) + tv.tv_usec;
    }

    // Slow (~500ns) about at nsec precision
    inline uint64_t getnsec()
    {
        timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        return (uint64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
    }

    // Slow (~500ns) about at nsec precision
    inline uint64_t getnsec(uint64_t *tsc)
    {
        timespec ts;
        uint64_t tsc1 = rdtscp();
        clock_gettime(CLOCK_REALTIME, &ts);
        uint64_t tsc2 = rdtscp();
        *tsc = (tsc1 + tsc2) / 2;
        return (uint64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
    }

    static inline u_int64_t rdtscp()
    {
        uint32_t aux;
        return __rdtscp(&aux);
    }
#if (0) // DISABLE TS_TRACKER
    #define TS_TRACKER_NAME  "DSSN_TS_Tracker"
    typedef struct {
        uint64_t last_clock;           // nano-sec from the last update
        uint64_t last_clock_tsc;       // TSC counter value of the last_clock
    } nt_pair_t;

    typedef struct {
        volatile uint32_t pingpong;    // Ping-pong index. Eiter 0 or 1.
        nt_pair_t nt[2];
        double cyclesPerSec;
        std::atomic<int> tracker_id;
        uint32_t heartbeat;
        clockid_t clockid;             //
    } ts_tracker_t;

    
    ts_tracker_t * tp;                // pointing to a shared ts_tracker
    pthread_t tid;
    int my_tracker_id;
    bool thread_run_run;
    bool tracker_init;
#endif
    uint64_t uctr, nctr;              // statistics
}; // ClusterTimeService

} // end namespace QDB
