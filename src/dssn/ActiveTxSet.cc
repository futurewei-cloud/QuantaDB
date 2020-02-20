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
        RAMCloud::Object *tuple = readSet[i];
        uint16_t keyLength;
        bool success = cbf.add((const uint8_t *)tuple->getKey(i, &keyLength), keyLength);
        if (!success) {
            // undo effects
            for (int j = i - 1; j >= 0; j--) {
                cbf.remove((const uint8_t *)tuple->getKey(i, &keyLength), keyLength);
            }
            return false;
        }
    }
    std::vector<RAMCloud::Object *> &writeSet = txEntry->getWriteSet();
    for (uint32_t i = 0; i < writeSet.size(); i++) {
        RAMCloud::Object *tuple = writeSet[i];
        uint16_t keyLength;
        bool success = cbf.add((const uint8_t *)tuple->getKey(i, &keyLength), keyLength);
        if (!success) {
            // undo effects
            for (int j = i - 1; j >= 0; j--) {
                cbf.remove((const uint8_t *)tuple->getKey(i, &keyLength), keyLength);
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
        RAMCloud::Object *tuple = readSet[i];
        uint16_t keyLength;
        cbf.remove((const uint8_t *)tuple->getKey(i, &keyLength), keyLength);
    }
    std::vector<RAMCloud::Object *> &writeSet = txEntry->getWriteSet();
    for (uint32_t i = 0; i < writeSet.size(); i++) {
        RAMCloud::Object *tuple = writeSet[i];
        uint16_t keyLength;
        cbf.remove((const uint8_t *)tuple->getKey(i, &keyLength), keyLength);
    }
    return true;
}

bool
ActiveTxSet::blocks(TxEntry *txEntry) {
    // only check write set of txEntry against the active tx set
    std::vector<RAMCloud::Object *> &writeSet = txEntry->getWriteSet();
    for (uint32_t i = 0; i < writeSet.size(); i++) {
        RAMCloud::Object *tuple = writeSet[i];
        uint16_t keyLength;
        if (cbf.contains((const uint8_t *)tuple->getKey(i, &keyLength), keyLength))
            return true;
    }
    return false;
}

void
ActiveTxSet::createIndexes(TxEntry *txEntry) {
    //LATER precalculate the indexes and store them in txEntry
}

} // end ActiveTxSet class

