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
        if (!success) {
            // undo effects
            for (int j = i - 1; j >= 0; j--) {
                cbf.remove(txEntry->getWriteSetHash()[j]);
            }
            return false;
        }
    }
    return true;
}

bool
ActiveTxSet::remove(TxEntry *txEntry) {
    for (uint32_t i = 0; i < txEntry->getReadSetSize(); i++) {
        cbf.remove(txEntry->getReadSetHash()[i]);
    }
    for (uint32_t i = 0; i < txEntry->getWriteSetSize(); i++) {
        cbf.remove(txEntry->getWriteSetHash()[i]);
    }
    return true;
}

bool
ActiveTxSet::blocks(TxEntry *txEntry) {
    for (uint32_t i = 0; i < txEntry->getReadSetSize(); i++) {
        if (cbf.contains(txEntry->getReadSetHash()[i]))
            return true;
    }
    for (uint32_t i = 0; i < txEntry->getWriteSetSize(); i++) {
        if (cbf.contains(txEntry->getWriteSetHash()[i]))
            return true;
    }
    return false;
}


} // end ActiveTxSet class

