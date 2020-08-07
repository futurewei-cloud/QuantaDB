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

    // return a cluster unique logical time stamp
    inline uint64_t getClusterTime()   	         
    {
        return (getLocalTime() << 10) + node_id;
    }

    // return a cluster unique logical time stamp
    inline __uint128_t getClusterTime128(uint32_t delta /* nanosec */)   	         
    {
        return ((__uint128_t)getLocalTime() << 64) + node_id;
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
        return ntp->last_nsec + ntp->ctr++;
    }

    // Convert a cluster time stamp to local clock time stamp
    inline uint64_t Cluster2Local(uint64_t cluster_ts)
    {
        return (cluster_ts >> 10);
    }

    // Convert a local time stamp to cluster clock time stamp
    inline uint64_t Local2Cluster(uint64_t local_ts)
    {
        return (local_ts << 10);
    }

    private:

    static void* update_ts_tracker(void *);

    // Fast (~25ns) about only at usec precision
    static inline uint64_t getusec()
    {
	    struct timeval tv;
	    gettimeofday(&tv,NULL);
	    return (uint64_t)(1000000*tv.tv_sec) + tv.tv_usec;
    }

    // Slow (~500ns) about at nsec precision
    static inline uint64_t getnsec()
    {
        timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        return (uint64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
    }

    #define TS_TRACKER_NAME  "DSSN_TS_Tracker"
    typedef struct {
        uint64_t last_nsec;           // nano-sec from the last update
        std::atomic<uint32_t> ctr;
    } nt_pair_t;

    typedef struct {
        volatile uint32_t idx;        // Ping-pong index. Eiter 0 or 1.
        nt_pair_t nt[2];
    } ts_tracker_t;

    // 
    ts_tracker_t * tp;                // pointing to a shared ts_tracker
    uint32_t node_id;		          // 
    pthread_t tid;
    bool thread_run_run;
    bool tracker_init;
}; // ClusterTimeService

} // end namespace DSSN
