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

#include "ActiveTxSet.h"
#include "Validator.h"

namespace QDB {

bool
ActiveTxSet::add(TxEntry *txEntry) {
    for (uint32_t i = 0; i < txEntry->getReadSetSize(); i++) {
        bool success = cbf.add(txEntry->getReadSetHash()[i]);
        assert(success);
        if (!success) {
            // undo effects
            for (int j = i - 1; j >= 0; j--) {
                cbf.remove(txEntry->getReadSetHash()[j]);
            }
            return false;
        }
    }
    for (uint32_t i = 0; i < txEntry->getWriteSetSize(); i++) {
        bool success = cbf.add(txEntry->getWriteSetHash()[i]);
        assert(success);
        if (!success) {
            // undo effects
            for (int j = i - 1; j >= 0; j--) {
                cbf.remove(txEntry->getWriteSetHash()[j]);
            }
            for (uint32_t j = 0; j < txEntry->getReadSetSize(); j++) {
                cbf.remove(txEntry->getReadSetHash()[j]);
            }
            return false;
        }
    }
    addedTxCount.fetch_add(1);
    return true;
}

bool
ActiveTxSet::remove(TxEntry *txEntry) {
    for (uint32_t i = 0; i < txEntry->getReadSetSize(); i++) {
        if (!cbf.remove(txEntry->getReadSetHash()[i]))
	    abort();
	    // RAMCLOUD_LOG(ERROR, "CBF failed to remove entry: %lu, readset %d", (uint64_t)(txEntry->getCTS() >> 64), i);
    }
    for (uint32_t i = 0; i < txEntry->getWriteSetSize(); i++) {
        if (!cbf.remove(txEntry->getWriteSetHash()[i]))
	    abort();
	    // RAMCLOUD_LOG(ERROR, "CBF failed to remove entry: %lu, writeset %d", (uint64_t)(txEntry->getCTS() >> 64), i);
    }
    removedTxCount.fetch_add(1);
    return true;
}

bool
ActiveTxSet::blocks(TxEntry *txEntry) {
    for (uint32_t i = txEntry->getReadSetIndex(); i < txEntry->getReadSetSize(); i++) {
        if (cbf.shouldNotAdd(txEntry->getReadSetHash()[i])) {
            txEntry->getReadSetIndex() = i;
            return true;
        }
    }
    for (uint32_t i = 0; i < txEntry->getReadSetIndex(); i++) {
        if (cbf.shouldNotAdd(txEntry->getReadSetHash()[i])) {
            txEntry->getReadSetIndex() = i;
            return true;
        }
    }
    for (uint32_t i = txEntry->getWriteSetIndex(); i < txEntry->getWriteSetSize(); i++) {
        if (cbf.shouldNotAdd(txEntry->getWriteSetHash()[i])) {
            txEntry->getWriteSetIndex() = i;
            return true;
        }
    }
    for (uint32_t i = 0; i < txEntry->getWriteSetIndex(); i++) {
        if (cbf.shouldNotAdd(txEntry->getWriteSetHash()[i])) {
            txEntry->getWriteSetIndex() = i;
            return true;
        }
    }
    return false;
}


} // end ActiveTxSet class

