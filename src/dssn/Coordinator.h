/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef COORDINATOR_H
#define COORDINATOR_H

#include "Common.h"
#include "Object.h"
#include "TXEntry.h"

namespace DSSN {

typedef RAMCloud::Object Object;
typedef RAMCloud::KeyLength KeyLength;

/**
 * Coordinator instance is used as a lib, tracking one transaction at a time for its client,
 * as the initiator of the DSSN commit protocol.
 *
 * It does early-abort by performing SSN exclusion check upon each read operation.
 * It does not send any write operation RPC to the storage node. Instead, it caches
 * the writeset to support any read of tuple that it has written.
 * It makes the readset non-overlapping with the writeset.
 * It uses its sequencer to get a CTS before initiating the commit-intent.
 *
 */
class Coordinator {
    PROTECTED:


    PUBLIC:
    Coordinator();

}; // end Coordinator class

} // end namespace DSSN

#endif  /* COORDINATOR_H */

