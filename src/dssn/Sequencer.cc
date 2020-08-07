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
uint64_t Sequencer::getCTS()
{
    return clock.getClusterTime(SEQUENCER_DELTA);
}

__uint128_t Sequencer::getCTS128()
{
    return clock.getClusterTime128(SEQUENCER_DELTA);
}

} // DSSN

