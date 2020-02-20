/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "ActiveTxSet.h"

namespace DSSN {

bool
ActiveTxSet::add(TxEntry *txEntry) {
    std::vector<RAMCloud::Object *> &readSet = txEntry->getReadSet();
    for (uint32_t i = 0; i < readSet.size(); i++) {
        RAMCloud::Object *object = readSet[i];
        bool success = cbf.add((const uint8_t *)object->getKey(i), object->getKeyLength(i));
        if (!success) {
            // undo effects
            for (int j = i - 1; j >= 0; j--) {
                cbf.remove((const uint8_t *)object->getKey(j), object->getKeyLength(j));
            }
            return false;
        }
    }
    std::vector<RAMCloud::Object *> &writeSet = txEntry->getWriteSet();
    for (uint32_t i = 0; i < writeSet.size(); i++) {
        RAMCloud::Object *object = writeSet[i];
        bool success = cbf.add((const uint8_t *)object->getKey(i), object->getKeyLength(i));
        if (!success) {
            // undo effects
            for (int j = i - 1; j >= 0; j--) {
                cbf.remove((const uint8_t *)object->getKey(j), object->getKeyLength(j));
            }
            return false;
        }
    }
    return true;
}

bool
ActiveTxSet::remove(TxEntry *txEntry) {
    std::vector<RAMCloud::Object *> &readSet = txEntry->getReadSet();
    for (uint32_t i = 0; i < readSet.size(); i++) {
        RAMCloud::Object *object = readSet[i];
        cbf.remove((const uint8_t *)object->getKey(i), object->getKeyLength(i));
    }
    std::vector<RAMCloud::Object *> &writeSet = txEntry->getWriteSet();
    for (uint32_t i = 0; i < writeSet.size(); i++) {
        RAMCloud::Object *object = writeSet[i];
        cbf.remove((const uint8_t *)object->getKey(i), object->getKeyLength(i));
    }
    return true;
}

bool
ActiveTxSet::blocks(TxEntry *txEntry) {
    // only check write set of txEntry against the active tx set
    std::vector<RAMCloud::Object *> &writeSet = txEntry->getWriteSet();
    for (uint32_t i = 0; i < writeSet.size(); i++) {
        RAMCloud::Object *object = writeSet[i];
        if (cbf.contains((const uint8_t *)object->getKey(i), object->getKeyLength(i)))
            return true;
    }
    return false;
}

void
ActiveTxSet::createIndexes(TxEntry *txEntry) {
    //LATER precalculate the indexes and store them in txEntry
}

} // end ActiveTxSet class

