/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "ActiveTxSet.h"

namespace DSSN {

bool
ActiveTxSet::add(TxEntry *txEntry) {
    std::vector<KVLayout *> &readSet = txEntry->getReadSet();
    for (uint32_t i = 0; i < readSet.size(); i++) {
        KVLayout *object = readSet[i];
        bool success = cbf.add((const uint8_t *)object->getKey(), object->getKeyLength());
        if (!success) {
            // undo effects
            for (int j = i - 1; j >= 0; j--) {
                cbf.remove((const uint8_t *)object->getKey(), object->getKeyLength());
            }
            return false;
        }
    }
    std::vector<KVLayout *> &writeSet = txEntry->getWriteSet();
    for (uint32_t i = 0; i < writeSet.size(); i++) {
        KVLayout *object = writeSet[i];
        bool success = cbf.add((const uint8_t *)object->getKey(), object->getKeyLength());
        if (!success) {
            // undo effects
            for (int j = i - 1; j >= 0; j--) {
                cbf.remove((const uint8_t *)object->getKey(), object->getKeyLength());
            }
            return false;
        }
    }
    return true;
}

bool
ActiveTxSet::remove(TxEntry *txEntry) {
    std::vector<KVLayout *> &readSet = txEntry->getReadSet();
    for (uint32_t i = 0; i < readSet.size(); i++) {
    	KVLayout *object = readSet[i];
        cbf.remove((const uint8_t *)object->getKey(), object->getKeyLength());
    }
    std::vector<KVLayout *> &writeSet = txEntry->getWriteSet();
    for (uint32_t i = 0; i < writeSet.size(); i++) {
    	KVLayout *object = writeSet[i];
        cbf.remove((const uint8_t *)object->getKey(), object->getKeyLength());
    }
    return true;
}

bool
ActiveTxSet::blocks(TxEntry *txEntry) {
    // only check write set of txEntry against the active tx set
    std::vector<KVLayout *> &writeSet = txEntry->getWriteSet();
    for (uint32_t i = 0; i < writeSet.size(); i++) {
        KVLayout *object = writeSet[i];
        if (cbf.contains((const uint8_t *)object->getKey(), object->getKeyLength()))
            return true;
    }
    return false;
}

void
ActiveTxSet::createIndexes(TxEntry *txEntry) {
    //LATER precalculate the indexes and store them in txEntry
}

} // end ActiveTxSet class

