/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "ClusterTimeService.h"
#include "Sequencer.h"
#include "HashmapKVStore.h"

namespace DSSN {

Sequencer::Sequencer() { }

// Return CTS
__uint128_t Sequencer::getCTS()
{
    return clock.getClusterTime(SEQUENCER_DELTA);
}

} // DSSN

