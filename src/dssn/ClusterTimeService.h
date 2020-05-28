/* Copyright (c) 2020  Futurewei Technologies, Inc.
 * All rights are reserved.
 */
#pragma once
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

namespace DSSN {
/**
 * DSSN Cluster Time Service:
 * - High speed and high precision (nano-second) cluster time service
 * - Issues cluster unique time stamp (logical time stamp, explained below)
 * - Issues node local time stamp (real clock time stamp) in nanosecond precision.
 * - Convertion utility converse between logical time stamp and the corresponding real time.
 *
 * DSSN Cluster Time Service relies on an underlying time synchronization module to sync time to sub-microsecond
 * precision. There are a few existing such protocols. In this implementation, we use the PTP (IEEE1588) protocol
 * to synchronize system clocks in the cluster.
 *
 * A logical time-stamp is a 64-bit value consisted of <54-bit real clock time in nano-sec><10-bit node id>.
 * A logical time-stamp is monotonically increasing and unique across the DSSN cluster. 
 *
 * The 10-bit node id field ensures logical time stamp is unique among the cluster nodes. 10 bits implies a
 * maximum of 1024 nodes can be supported. 
 *
 * Node id must be unique and is automatically assigned at ClusterTimeService class instantiation time.
 * A formal approach to obtain a unique node id could be using a cluster management service such as zookeeper.
 * In this implementation, a light-weight approach is used by picking up the last number of the IP address of
 * the PTP interface port.
 *
 * This implementation uses PTP and gettimeofday(2) to get synchronized micro-second precision system time.
 * Sub-microsecond precision is simulated using a call counter. Monotonic increasing property is preserved.
 *
 * Extra Note:
 * Using __rdtsc() and Cycles::toNanoseconds() to calculate nanosecond precision would be fairer than
 * using a call counter. But it adds about 15ns to latency. This implementation trades speed with ignorable
 * inter-node timestamp fairness.
 */


// ClusterTimeService
class ClusterTimeService {
    public:
    ClusterTimeService();
    ~ClusterTimeService();

    // return a cluster unique logical time stamp
    inline uint64_t getClusterTime()   	         
    {
        return (getLocalTime() << 10) + node_id;
    }

    // cluster unique time stamp that is local clock + delta
    inline uint64_t getClusterTime(uint32_t delta /* nanosec */)
    {
        return ((getLocalTime() + delta) << 10) + node_id;
    }

    // return a local system clock time stamp
    inline uint64_t getLocalTime()
    {
        nt_pair_t *ntp = &tp->nt[tp->idx];
        return ntp->last_nsec + Cycles::toNanoseconds(__rdtsc() - ntp->last_tsc);
    }

    // Convert a cluster time stamp to local clock time stamp
    inline uint64_t Cluster2Local(uint64_t cluster_ts)
    {
        return (cluster_ts >> 10);
    }

    private:

    static void* update_ts_tracker(void *);

    // This is fast (~25ns) about only at usec precision
    static inline uint64_t getusec()
    {
	    struct timeval tv;
	    gettimeofday(&tv,NULL);
	    return (uint64_t)(1000000*tv.tv_sec) + tv.tv_usec;
    }

    // this is slow (~500ns) about at nsec precision
    static inline uint64_t getnsec()
    {
        timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        return (uint64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
        // return getusec() * 1000;
    }

    #define TS_TRACKER_NAME  "DSSN_TS_Tracker"
    typedef struct {
        uint64_t last_nsec;           // nano-sec from the last update
        uint64_t last_tsc;            // TSC from the last update
    } nt_pair_t;

    typedef struct {
        uint32_t idx;                 // Ping-pong index. Eiter 0 or 1.
        nt_pair_t nt[2];
        uint64_t stat_nt_skip,
                 stat_nt_switch;  // Statistics
    } ts_tracker_t;

    // 
    ts_tracker_t * tp;                // pointing to a shared ts_tracker
    uint32_t node_id;		          // 
    pthread_t tid;
    bool thread_run_run;
    bool tracker_init;
}; // ClusterTimeService

} // end namespace DSSN
