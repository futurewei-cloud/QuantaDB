/* Copyright (c) 2020  Futurewei Technologies, Inc.
 * All rights are reserved.
 */
#pragma once

#include <sys/types.h>
#include <stdint.h>
#include "ClusterTimeService.h"

namespace DSSN {
/**
 * DSSN Sequencer Description - ver 3
 *
 * A DSSN Sequencer generates Commit Time Stamp (CTS) value.
 *
 * CTS must be globally unique for a cross-shard transaction. Therefore it is made up with a Cluster
 * Time Stamp. A CTS also needs to be set slightly ahead of Validator's local clock, so that the Tx won't be
 * scheduled as soon as it arrives a Validator. This gives Validator a window to reorder transactions.
 *
 * Therefore, CTS = Cluster Time Stamp + 'delta'.
 *
 */

#define SEQUENCER_DELTA 1000    // 1 usec delay

class Sequencer {
    public:
    Sequencer ();
    uint64_t    getCTS();    	     // return CTS
    __uint128_t   getCTS128();    	     // return CTS

    private:
    // ClusterTimeClient clock;
    ClusterTimeService clock;   // Use server class for now.
}; // Sequencer

} // end namespace DSSN
