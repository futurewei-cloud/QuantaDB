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
 * - High performance (~20ns delay) and high precision (nano-second) time service
 * - Issues cluster unique time stamp (logical time stamp, explained below)
 * - Issues node local time stamp (real clock time stamp) in nanosecond precision.
 * - Convertion utility that converse between logical time stamp and the corresponding real time stamp.
 *
 * DSSN Cluster Time Service relies on an underlying cluster time synchronization module to the sub-microsecond
 * precision. There are a few existing such protocols. For this implementation, we use the PTP (IEEE1588) protocol
 * to synchronize system clocks in the cluster.
 *
 * A logical time-stamp is a 64-bit value consisted of <54-bit real clock time in nano-sec><10-bit node id>.
 * A logical time-stamp is monotonically increasing and unique across the DSSN cluster. How these three fields
 * are generated is explained below. 
 *
 * The 54-bit field is real clock time in nano-seconds.
 *
 * Each ClusterTS is assigned a unique id to ensures no logical time stamp collision between nodes. A 10-bit
 * id field implies a maximum of 1024 nodes can be supported. Each node runs a single Cluster Time Server instance. 
 *
 * Automatic node id is assigned at ClusterTimeService class instantiation.
 * A formal approach to the unique id assignment would be to hook-up to a cluster node management
 * service such as zookeeper. In this implementation, a light-weight approach is used by picking up the last
 * number of the IP address of the PTP interface port.
 *
 * High performance is achieved by getting system time from the gettimeofday() which gives micro-second precision.
 *
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
        uint32_t ctr;
        std::atomic_flag flag;          // spin lock
    } ts_tracker_t;

    ts_tracker_t * tp;                  // pointing to a shared delta tracker struct 
    uint32_t node_id;		            // 
}; // ClusterTimeService

} // end namespace DSSN
