/* Copyright (c) 2020  Futurewei Technologies, Inc.
 * All rights are reserved.
 */
#pragma once

#include "Common.h"
#include "Object.h"

namespace DSSN {
/**
 * A DSSN Sequencer issues a 64-bit logical time-stamp that is unique and monotonically increasing.
 * DSSN Sequencer works in a distributed model and is a lib that goes side-by-side with a Coordinator.
 * 
 * One challenge in distributed sequencer is in multi-node clock synchronization. Clock synchronization
 * is a topic independent from DSSN. There are many clock synchronization solutions. This implementation
 * gets time-stamp from a PHC (Physical Hardware Clock) on a NIC/IB card which is managed by LinuxPTP.
 *
 * LinuxPTP provides sub-microsecond time synchronization precision. A sub-microsecond precision should
 * be sufficient to support 1M transactions per second.
 *
 * A logical time-stamp is a 64-bit value consisted of <44-bit phc time in usec><10-bit weight><10-bit counter> 
 *
 * The 10-bit counter field is used by Sequencer to ensure unique logical time-stamps. This allows
 * Sequencer to handle upto 1024 requests within a one microsecond period. More then 1024, Sequencer will
 * need to re-read PHC until the next microsecond has arrived. 
 *
 * The 10-bit weight field is used to avoid time-stamp collision between nodes. Each Sequencer is assigned
 * a different weight and its initialization time. When multiple nodes generated logical time-stamp from the
 * the same PHC time-stamp and the same local counter, the weight field will serve as a tie-breaker to ensure
 * no logical time-stamp collision will happen between any node. A 10-bit weight field implies a max of 1024
 * Sequencers can be supported. 
 *
 * [TBD: How to automatically assign 'weight' at Sequencer startup time? ]
 */
class Sequencer {
    PROTECTED:

    PUBLIC:
    Sequencer();
    uint64_t getTimeStamp();    // return logical time-stamp

    PRIVATE:
    uint64_t    readPHC();      // Read time-stamp from PHC
    uint64_t    last_phc;       // Last PHC time-stamp read
    uint32_t    counter;        // local counter to ensure unique logical time-stamp is geneerated.
                                // If last_phc == this_phc, then counter++, else counter = 0;
}; // end Sequencer class

} // end namespace DSSN
