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

#ifndef COORDINATOR_H
#define COORDINATOR_H

#include "Common.h"
#include "KVStore.h"
#include "Buffer.h"

namespace QDB {

typedef RAMCloud::Buffer Buffer;

/**
 * Coordinator instance is used as a lib, tracking one transaction at a time for its client,
 * as the initiator of the DSSN commit protocol.
 *
 * This is the QDB-equivalent of the RAMCloud::Transaction class.
 *
 * It does early-abort by performing SSN exclusion check upon each read operation.
 *
 * There are two options of how a write is handled. The first option is to skip sending
 * write op RPC to the validator and just pass the write set at commit intent. Then,
 * there is not early abort puon write op. The second option is to to send write RPC
 * to the validator though the write value would not matter and be cached in validator.
 * The meta data returned by the validator would help early abort.

 * It makes the read set non-overlapping with the write set.
 * It uses its sequencer to get a CTS before initiating the commit-intent.
 * It partitions the read set and write set according to the relevant shards
 * and initiates commit-intent(s) to relevant validator(s).
 *
 */
class Coordinator {
    PROTECTED:
    //DSSN data
    uint64_t cts;       //commit time-stamp, globally unique
    uint64_t pstamp;    //predecessor high watermark
    uint64_t sstamp;    //successor low watermark

    uint32_t txState;
    std::set<KVLayout> readSet;
    std::set<KVLayout> writeSet;

    inline bool isExclusionViolated() { return sstamp <= pstamp; }

    bool ssnRead(KVLayout& kv, uint64_t cStamp, uint64_t sStamp) {
    	if (writeSet.find(kv) != writeSet.end()) {
    		pstamp = std::max(pstamp, cStamp);
    		if (sStamp == 0xffffffffffffffff) {
    			readSet.insert(kv);
    		} else {
    			sstamp = std::min(sstamp, sStamp);
    		}
    	}
    	return isExclusionViolated();
    }

    bool ssnWrite(KVLayout& kv, uint64_t pStampPrev) {
    	if (writeSet.find(kv) != writeSet.end()) {
    		pstamp = std::max(pstamp, pStampPrev);
    		writeSet.insert(kv);
    		readSet.erase(kv);
    	}
    	return isExclusionViolated();
    }

    PUBLIC:
    Coordinator();

    bool commit();

    void read(uint64_t tableId, const void* key, uint16_t keyLength,
            Buffer* value, bool* objectExists = NULL);

    void remove(uint64_t tableId, const void* key, uint16_t keyLength);

    void write(uint64_t tableId, const void* key, uint16_t keyLength,
            const void* buf, uint32_t length);

}; // end Coordinator class

} // end namespace QDB

#endif  /* COORDINATOR_H */

