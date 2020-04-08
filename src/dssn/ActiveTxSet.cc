/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "ActiveTxSet.h"

namespace DSSN {

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
    addedTxCount++;
    return true;
}

bool
ActiveTxSet::remove(TxEntry *txEntry) {
    for (uint32_t i = 0; i < txEntry->getReadSetSize(); i++) {
        assert(cbf.remove(txEntry->getReadSetHash()[i]));
    }
    for (uint32_t i = 0; i < txEntry->getWriteSetSize(); i++) {
        assert(cbf.remove(txEntry->getWriteSetHash()[i]));
    }
    removedTxCount++;
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

