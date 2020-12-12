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

#include "DistributedTxSet.h"
#include "Validator.h"
#include "Logger.h"

namespace QDB {

template <class T>
bool
DistributedTxSet::dependsOnEarlierTxs(T &cbf, TxEntry *txEntry, uint32_t &count) {
    count = 0;
    for (uint32_t i = 0; i < txEntry->getReadSetSize(); i++) {
        uint32_t tmp;
        if ((tmp = cbf.hitCount(txEntry->getReadSetHash()[i])) > count)
            count = tmp;
    }
    for (uint32_t i = 0; i < txEntry->getWriteSetSize(); i++) {
        uint32_t tmp;
        if ((tmp = cbf.hitCount(txEntry->getWriteSetHash()[i])) > count)
            count = tmp;
    }
    return count > 0;
}

template <class T>
bool
DistributedTxSet::dependsOnEarlierTxs(T &cbf, TxEntry *txEntry) {
    for (uint32_t i = 0; i < txEntry->getReadSetSize(); i++) {
        if (cbf.shouldNotAdd(txEntry->getReadSetHash()[i]))
            return true;
    }
    for (uint32_t i = 0; i < txEntry->getWriteSetSize(); i++) {
        if (cbf.shouldNotAdd(txEntry->getWriteSetHash()[i]))
            return true;
    }
    return false;
}

template <class T>
bool
DistributedTxSet::addToCBF(T &cbf, TxEntry *txEntry) {
    for (uint32_t i = 0; i < txEntry->getWriteSetSize(); i++) {
        bool success = cbf.add(txEntry->getWriteSetHash()[i]);
        if (!success) {
            // undo effects
            for (int j = i - 1; j >= 0; j--) {
                if (!cbf.remove(txEntry->getWriteSetHash()[j]))
                    abort();
                //RAMCLOUD_LOG(ERROR, "CBF failed to remove entry: %lu, writeset %d", (uint64_t)(txEntry->getCTS() >> 64), j);
            }
            return false;
        }
    }
    for (uint32_t i = 0; i < txEntry->getReadSetSize(); i++) {
        bool success = cbf.add(txEntry->getReadSetHash()[i]);
        if (!success) {
            // undo effects
            for (int j = i - 1; j >= 0; j--) {
                if (!cbf.remove(txEntry->getReadSetHash()[j]))
                    abort();
                // RAMCLOUD_LOG(ERROR, "CBF failed to remove entry: %lu, readset %d", (uint64_t)(txEntry->getCTS() >> 64), j);
            }
            for (uint32_t j = 0; j < txEntry->getWriteSetSize(); j++) {
                if (!cbf.remove(txEntry->getWriteSetHash()[j]))
                    abort();
                // RAMCLOUD_LOG(ERROR, "CBF failed to remove entry: %lu, writeset %d", (uint64_t)(txEntry->getCTS() >> 64), j);
            }
            return false;
        }
    }
    return true;
}

template <class T>
bool
DistributedTxSet::removeFromCBF(T &cbf, TxEntry *txEntry) {
    for (uint32_t i = 0; i < txEntry->getWriteSetSize(); i++) {
        bool success = cbf.remove(txEntry->getWriteSetHash()[i]);
        assert(success);
    }
    for (uint32_t i = 0; i < txEntry->getReadSetSize(); i++) {
        bool success = cbf.remove(txEntry->getReadSetHash()[i]);
        assert(success);
    }
    return true;
}

bool
DistributedTxSet::addToHotTxs(TxEntry *txEntry) {
    if (hotDependQueue.isFull()) {
        RAMCLOUD_LOG(ERROR, "queue is full");
        return false;
    }
    if (addToCBF(hotDependCBF, txEntry)) {
        if (!hotDependQueue.add(txEntry)) {
            // RAMCLOUD_LOG(ERROR, "Failed to add into hotQueue");
            // return false;
            abort();
        }
        addedTxCount.fetch_add(1);
        return true;
    }
    return false; //limited by CBF depth
}

bool
DistributedTxSet::addToColdTxs(TxEntry *txEntry) {
    if (coldDependQueue.isFull()) {
        RAMCLOUD_LOG(ERROR, "queue is full");
        return false;
    }
    if (addToCBF(coldDependCBF, txEntry)) {
        if (!coldDependQueue.add(txEntry)) {
            // RAMCLOUD_LOG(ERROR, "Failed to add into coldQueue");
            // return false;
            abort();
        }
        addedTxCount.fetch_add(1);
        return true;
    }
    return addToHotTxs(txEntry); //limited by CBF depth, move it to hot queue
}

bool
DistributedTxSet::addToIndependentTxs(TxEntry *txEntry) {
    if (independentQueue.isFull()) {
        RAMCLOUD_LOG(ERROR, "queue is full");
        return false;
    }
    if (addToCBF(independentCBF, txEntry)) {
        if (!independentQueue.add(txEntry)) {
            // RAMCLOUD_LOG(ERROR, "fail to add into indepQueue");
            // return false;
            abort();
        }
        addedTxCount.fetch_add(1);
        return true;
    }
    return addToColdTxs(txEntry); //limited by CBF depth, move it to cold queue
}

bool
DistributedTxSet::add(TxEntry *txEntry) {
    uint32_t count;
    if (dependsOnEarlierTxs(hotDependCBF, txEntry)) {
        return addToHotTxs(txEntry);
    }

    if (dependsOnEarlierTxs(coldDependCBF, txEntry, count)) {
        if (count >= hotThreshold) {
            return addToHotTxs(txEntry);
        }

        return (count >= coldDependCBF.countLimit() ? false : addToColdTxs(txEntry));
    }

    if (dependsOnEarlierTxs(independentCBF, txEntry)) {
        return addToColdTxs(txEntry);
    }

    return addToIndependentTxs(txEntry);
}

TxEntry*
DistributedTxSet::findReadyTx(ActiveTxSet &activeTxSet) {

    //skip scanning if there is no change that matters
    //Fixme: since the counters are not trustworthy, disabling this optimization for now
    //if ((addedTxCount + removedTxCount + activeTxSet.getRemovedTxCount()) == activitySignature)
    //    return NULL;
    activitySignature = addedTxCount + removedTxCount + activeTxSet.getRemovedTxCount();

    uint64_t itHot, itCold, itIndepend;
    TxEntry *txHot = hotDependQueue.findFirst(itHot);
    TxEntry *txCold = coldDependQueue.findFirst(itCold);
    TxEntry *txIndepend = independentQueue.findFirst(itIndepend);
    if (txHot
            && (!txCold || txHot->getCTS() < txCold->getCTS())
            && (!txIndepend || txHot->getCTS() < txIndepend->getCTS())
            && !activeTxSet.blocks(txHot)) {
        hotDependQueue.remove(itHot, txHot);
        removeFromCBF(hotDependCBF, txHot);
        removedTxCount.fetch_add(1);
        return txHot;
    }

    if (txCold
            && (!txIndepend || txCold->getCTS() < txIndepend->getCTS())
            && !activeTxSet.blocks(txCold)) {
        coldDependQueue.remove(itCold, txCold);
        removeFromCBF(coldDependCBF, txCold);
        removedTxCount.fetch_add(1);
        return txCold;
    }

    if (txIndepend) {
        do {
            if (!activeTxSet.blocks(txIndepend)) {
                independentQueue.remove(itIndepend, txIndepend);
                removeFromCBF(independentCBF, txIndepend);
                removedTxCount.fetch_add(1);
                return txIndepend;
            }
        } while ((txIndepend = independentQueue.findNext(itIndepend)));
    }

    return NULL;
}

} // end DistributedTxSet class

