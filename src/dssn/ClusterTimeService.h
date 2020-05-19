/* Copyright (c) 2020  Futurewei Technologies, Inc.
 * All rights are reserved.
 */
#pragma once
#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <atomic>

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
 */


// ClusterTimeService
class ClusterTimeService {
    public:
    ClusterTimeService();

    // return a cluster unique logical time stamp
    inline uint64_t getClusterTime()   	         
    {
        return getClusterTime(0);
    }

    // cluster unique time stamp that is local clock + delta
    inline uint64_t getClusterTime(uint32_t delta /* nanosec */)
    {
        return ((getLocalTime() + delta) << 10) + node_id;
    }

    // return a local system clock time stamp
    inline uint64_t getLocalTime()
    {
        uint64_t usec = getusec();
        while (tp->flag.test_and_set());
        if (usec > tp->last_usec) {
            tp->last_usec = usec;
            tp->ctr = 0;
        }
        uint64_t ret = (tp->last_usec << 10) + tp->ctr++;
        tp->flag.clear();
        return ret;
    }

    // Convert a cluster time stamp to local clock time stamp
    inline uint64_t Cluster2Local(uint64_t cluster_ts)
    {
        return (cluster_ts >> 10);
    }

    private:
    inline uint64_t getusec()
    {
	    struct timeval tv;
	    gettimeofday(&tv,NULL);
	    return (uint64_t)(1000000*tv.tv_sec) + tv.tv_usec;
    }

    #define TS_TRACKER_NAME  "DSSN_TS_Tracker"
    typedef struct {
        uint64_t last_usec;             // usec from the last gettimeofday() call
        uint32_t ctr;                   // counter
        std::atomic_flag flag;          // spin lock
    } ts_tracker_t;

    ts_tracker_t * tp;                  // pointing to a shared ts_tracker
    uint32_t node_id;		            // 
}; // ClusterTimeService

} // end namespace DSSN
