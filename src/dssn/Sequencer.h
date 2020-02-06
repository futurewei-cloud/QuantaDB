/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */
#pragma once

#include "Common.h"
#include "Object.h"

namespace DSSN {
/**
 * A DSSN Sequencer issues a 64-bit logical time-stamp that is unique and monotonically increasing.
 * DSSN Sequencer works in a distributed model and is a lib that go side-by-side with a Coordinator.
 * 
 * One challenge in distributed sequencer is in multi-node clock synchronization. Clock synchronization
 * s an issue ndependent from DSSN. There are many clock synchronization solutions. This implementation
 * gets time-stamp from a PHC (Physical Hardware Clock) on a NIC/IB card which is managed by LinuxPTP.
 *
 * LinuxPTP provides sub-microsecond time synchronization precision. A sub-microsecond precision should
 * be sufficient to support 1M transactions per second.
 *
 * To avoid time-stamp collision between nodes, each Sequencer is given a 'weight' as a tie-breaker.
 * Out of the 64-bit logical time stamp, the lower 10-bit is allocated for the tie-breaker. This implies
 * a max of 1024 Sequencers can be supported. The upper 54-bit is taken directly from the PHC's micro-second 
 * readings.
 *
 * An embedded error detection is built-in to Sequencer by checking the same logical time-stamp is not
 * issued twice. The maximum transaction throught of a single node should be much less than 1M transaction
 * per second. So it should be impossible for this error to happen.
 */
class Sequencer {
    PROTECTED:

    PUBLIC:
    Sequencer();
    uint64_t getTimeStamp();    // return logical time-stamp

    PRIVATE:
    uint64_t readPHC();         // Read time-stamp from PHC

    uint64_t    last_issued;    // Last time-stamp issued
}; // end Sequencer class

} // end namespace DSSN
