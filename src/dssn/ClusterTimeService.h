/* Copyright (c) 2020  Futurewei Technologies, Inc.
 * All rights are reserved.
 */
#pragma once

#include <sys/types.h>
#include <atomic>
#include <stdint.h>

namespace DSSN {
/**
 * DSSN Cluster Time Service Description - ver 1
 *
 * DSSN Cluster Time Service does:
 * - High performance (~20ns delay) and high precision (nano-second) time service
 * - Issues cluster unique time stamp (logical time stamp, explained below)
 * - Issues node local time stamp (real clock time stamp) in nanosecond precision.
 * - Convertion function that takes a logical time stamp and returns the corresponding real clock time stamp.
 *
 * DSSN Cluster Time Service depends on an underlying module that synchronizes system time of all nodes.
 * There are a few existing such protocols. For this implementation, we use the PTP (IEEE1588) protocol
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
 * A client side library class is provided to get cluster time service via RPC.
 *
 * Automatic node id assignment is done at Cluster Time Service startup time.
 * A formal approach to the unique id assignment would probably be to hook-up to a cluster node management
 * service such as zookeeper. In this implementation, we choose a light-weight approach by picking up the last
 * number of the IP address of the PTP interface port.
 *
 * High performance is achieved by getting system time from the gettimeofday() call which cost just 20ns, but 
 * only has micro-second precision.
 *
 * Sub-usec precision is achieved by using the rdtsc() to get the delta (in nano-second) between calls that
 * return the same usec time stamp.
 */


// ClusterTimeService
class ClusterTimeService {
    public:
    ClusterTimeService();

    uint64_t getClusterTime();   	         // return a cluster unique logical time stamp
    uint64_t getClusterTime(uint32_t delta_nanosec); // return a cluster unique time stamp that is local clock + delta
    uint64_t getLocalTime();    	         // return a local system clock time stamp
    uint64_t Cluster2Local(uint64_t);    // Convert a cluster time stamp to local clock time stamp

    private:
    uint32_t node_id;		// 
    uint32_t last_tsc;      //
    uint64_t last_usec;     // usec of the last gettimeofday() call
    std::atomic<uint32_t>   ctr;
}; // ClusterTimeService

} // end namespace DSSN
