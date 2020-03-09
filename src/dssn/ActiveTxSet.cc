/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "ActiveTxSet.h"

namespace DSSN {

bool
ActiveTxSet::add(TxEntry *txEntry) {
    std::vector<uint64_t> &hash = txEntry->getReadSetHash();
    for (uint32_t i = 0; i < hash.size(); i++) {
        bool success = cbf.add(hash[i]);
        if (!success) {
            // undo effects
            for (int j = i - 1; j >= 0; j--) {
                cbf.remove(hash[j]);
            }
            return false;
        }
    }
    hash = txEntry->getWriteSetHash();
    for (uint32_t i = 0; i < hash.size(); i++) {
        bool success = cbf.add(hash[i]);
        if (!success) {
            // undo effects
            for (int j = i - 1; j >= 0; j--) {
                cbf.remove(hash[j]);
            }
            return false;
        }
    }
    return true;
}

bool
ActiveTxSet::remove(TxEntry *txEntry) {
    std::vector<uint64_t> &hash = txEntry->getReadSetHash();
    for (uint32_t i = 0; i < hash.size(); i++) {
        cbf.remove(hash[i]);
    }
    hash = txEntry->getWriteSetHash();
    for (uint32_t i = 0; i < hash.size(); i++) {
        cbf.remove(hash[i]);
    }
    return true;
}

bool
ActiveTxSet::blocks(TxEntry *txEntry) {
    std::vector<uint64_t> &hash = txEntry->getReadSetHash();
    for (uint32_t i = 0; i < hash.size(); i++) {
        if (cbf.contains(hash[i]))
            return true;
    }
    hash = txEntry->getWriteSetHash();
    for (uint32_t i = 0; i < hash.size(); i++) {
        if (cbf.contains(hash[i]))
            return true;
    }
    return false;
}


} // end ActiveTxSet class

