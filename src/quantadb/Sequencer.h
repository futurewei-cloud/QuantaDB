/* Copyright 2020 Futurewei Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#pragma once

#include <sys/types.h>
#include <stdint.h>
#include "ClusterTimeService.h"

namespace QDB {
/**
 * QDB Sequencer Description - ver 3
 *
 * A QDB Sequencer generates Commit Time Stamp (CTS) value.
 *
 * CTS must be globally unique for a cross-shard transaction. Therefore it is made up with a Cluster
 * Time Stamp. A CTS also needs to be set slightly ahead of Validator's local clock, so that the Tx won't be
 * scheduled as soon as it arrives a Validator. This gives Validator a window to reorder transactions.
 *
 * Therefore, CTS = Cluster Time Stamp + 'delta'.
 *
 */

#define SEQUENCER_DELTA 1000000    // 1 usec delay

class Sequencer {
    public:
    Sequencer ();
    __uint128_t   getCTS();    	     // return CTS

    private:
    // ClusterTimeClient clock;
    ClusterTimeService clock;   // Use server class for now.
    uint64_t    node_id;
}; // Sequencer

} // end namespace QDB
