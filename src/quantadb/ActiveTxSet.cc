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
        if (txEntry->isReadTupleSkipLock(i)) continue;
        bool success = cbm.add(txEntry->getReadSetHash()[i]);
        assert(success);
        if (!success) {
            // undo effects
            for (int j = i - 1; j >= 0; j--) {
	        if (txEntry->isReadTupleSkipLock(j)) continue;
                cbm.remove(txEntry->getReadSetHash()[j]);
            }
            return false;
        }
    }
    for (uint32_t i = 0; i < txEntry->getWriteSetSize(); i++) {
        if (txEntry->isWriteTupleSkipLock(i)) continue;
        bool success = cbm.add(txEntry->getWriteSetHash()[i]);
        assert(success);
        if (!success) {
            // undo effects
            for (int j = i - 1; j >= 0; j--) {
	        if (txEntry->isWriteTupleSkipLock(j)) continue;
                cbm.remove(txEntry->getWriteSetHash()[j]);
            }
            for (uint32_t j = 0; j < txEntry->getReadSetSize(); j++) {
	        if (txEntry->isReadTupleSkipLock(j)) continue;
                cbm.remove(txEntry->getReadSetHash()[j]);
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
        if (txEntry->isReadTupleSkipLock(i)) continue;
        if (!cbm.remove(txEntry->getReadSetHash()[i])) {
            //abort();
            RAMCLOUD_LOG(ERROR, "CBF failed to remove entry: %lu, readset %d", (uint64_t)(txEntry->getCTS() >> 64), i);
            return false;
        }
    }
    for (uint32_t i = 0; i < txEntry->getWriteSetSize(); i++) {
        if (txEntry->isWriteTupleSkipLock(i)) continue;
        if (!cbm.remove(txEntry->getWriteSetHash()[i])) {
            //abort();
            RAMCLOUD_LOG(ERROR, "CBF failed to remove entry: %lu, writeset %d", (uint64_t)(txEntry->getCTS() >> 64), i);
            return false;
        }
    }
    removedTxCount.fetch_add(1);
    return true;
}

bool
ActiveTxSet::blocks(TxEntry *txEntry) {
    for (uint32_t i = txEntry->getReadSetIndex(); i < txEntry->getReadSetSize(); i++) {
        if (txEntry->isReadTupleSkipLock(i)) continue;
        if (cbm.shouldNotAdd(txEntry->getReadSetHash()[i])) {
            txEntry->getReadSetIndex() = i;
            return true;
        }
    }
    for (uint32_t i = 0; i < txEntry->getReadSetIndex(); i++) {
        if (txEntry->isReadTupleSkipLock(i)) continue;
        if (cbm.shouldNotAdd(txEntry->getReadSetHash()[i])) {
            txEntry->getReadSetIndex() = i;
            return true;
        }
    }
    for (uint32_t i = txEntry->getWriteSetIndex(); i < txEntry->getWriteSetSize(); i++) {
        if (txEntry->isWriteTupleSkipLock(i)) continue;
        if (cbm.shouldNotAdd(txEntry->getWriteSetHash()[i])) {
            txEntry->getWriteSetIndex() = i;
            return true;
        }
    }
    for (uint32_t i = 0; i < txEntry->getWriteSetIndex(); i++) {
        if (txEntry->isWriteTupleSkipLock(i)) continue;
        if (cbm.shouldNotAdd(txEntry->getWriteSetHash()[i])) {
            txEntry->getWriteSetIndex() = i;
            return true;
        }
    }
    return false;
}


} // end ActiveTxSet class

