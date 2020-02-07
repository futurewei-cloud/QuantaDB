/* Copyright (c) 2020  Futurewei Technologies, Inc.
 * All rights are reserved.
 */
#pragma once

#include <sys/types.h>

namespace DSSN {
/**
 * DSSN Sequencer Description - ver 1
 *
 * A DSSN Sequencer provides time stamp service to DSSN Coordinator for obantining CTS value. DSSN Sequencer is
 * a per client node service daemon. A client side library is used to send request and get time stamp value.
 *
 * DSSN Sequencer generates logical time-stamp that is monotonically increasing and unique across the DSSN cluster.
 *
 * A logical time-stamp is a 64-bit value consisted of <44-bit phc time in usec><10-bit weight><10-bit counter>.
 * How these three fields are generated is explained below. 
 *
 * DSSN Sequencer works in a distributed model. One challenge in distributed sequencer is multi-node clock
 * synchronization. Clock synchronization is a topic independent from DSSN. There are many clock synchronization
 * solutions. This implementation uses LinuxPTP to synchronize clocks. LinuxPTP can achieve sub-microsecond
 * synchronization precision. A sub-microsecond precision is sufficient to support 1M transactions per second.
 *
 * The 44-bit field is PHC (Physical Hardware Clock) time rounded down to microsecond.
 *
 * The 10-bit weight field is used to avoid time-stamp collision between nodes. Each Sequencer is assigned
 * a different weight to ensures no logical time stamp collison between nodes. A 10-bit weight field implies
 * a maximum of 1024 Sequencers can be supported. 
 *
 * The 10-bit counter field allows Sequencer to generate as much as 1024 unique time stamps in a microsecond interval.
 * If more then 1024 was requested, Sequencer will need to wait until the next microsecond. This is a theoretical
 * assurance. In practice, it is impossible for Sequencer to handle 1024 time stamp requests per micro-second
 *
 * Automatic 'weight' assignment at Sequencer startup time.
 * A formal solution to the unique weight assignment problem is probably to hook-up to cluster node managerment
 * such as zookeeper. In this implementation, we choose a light-weight approach by picking up the last number of
 * the IP address of the PTP port.
 */
class Sequencer {
    public:
    Sequencer();
    u_int64_t getTimeStamp();    // return logical time-stamp

    private:
    u_int64_t    readPHC();      // return micro-second time stamp from PHC
    u_int64_t    last_phc;       // Last PHC time-stamp read
    u_int32_t    counter;        // local counter to ensure unique logical time-stamp is geneerated.
                                 // If last_phc == this_phc, then counter++, else counter = 0;
}; // end Sequencer class

} // end namespace DSSN
