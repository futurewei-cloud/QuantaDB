/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "KVInterface.h"
#include "sstream"
#include "Validator.h"
#include <thread>

namespace DSSN {

inline static std::string formTupleKey(Object& tuple) {
    KeyLength* kLen;
    uint8_t* key = tuple.getKey(0, kLen);
    if (key == NULL) // there is a bug if it happens
        return std::numeric_limits<uint64_t>::max();
    uint64_t tableId = tuple.getTableId();
    std::vector<uint8_t> ckey(sizeof(uint64_t) + *kLen);
    *(uint64_t *)ckey = tableId;
    for (uint32_t i = 0; i < *kLen; i++)
        ckey[sizeof(uint64_t) + i] = key[i];
    return std::string(ckey.begin(), ckey.end());
}

static uint64_t
Validator::getTuplePi(Object& object) {
    dssnMeta meta;
    Validator::tupleStore.getMeta(formTupleKey(object), meta);
    return meta.cStamp;
}

static uint64_t
Validator::getTuplePrevEta(Object& object) {
    dssnMeta meta;
    Validator::tupleStore.getMeta(formTupleKey(object), meta);
    return meta.sStampPrev;
}

static uint64_t
Validator::getTuplePrevPi(Object& object) {
    return 0; //not used yet
}

static bool
Validator::maximizeTupleEta(Object& object, uint64_t eta) {
    dssnMeta meta;
    std::string key = formTupleKey(object);
    Validator::tupleStore.getMeta(key, meta);
    meta.pStamp = std::max(eta, meta.pStamp);
    Validator::tupleStore.updateMeta(key, meta);
    return true;
}

static bool
Validator::updateTuple(Object& object, TXEntry& txEntry) {
    dssnMeta meta;
    std::string key = formTupleKey(object);
    Validator::tupleStore.getMeta(key, meta);

    /* because we have a single version HOT, copy current data to prev data */
    meta.pStampPrev = meta.pStamp;

    /* SSN required operations */
    meta.sStampPrev = txEntry.getPi();
    meta.cStamp = meta.pStamp = txEntry.getCTS();
    meta.sStamp = std::numeric_limits<uint64_t>::max();

    /* tuple: key, pointer to object, meta-data */
    std::stringstream ss;
    ss << static_cast<const void*>(object);
    tupleStore.put(key, ss.str(), meta);
    return true;
}

Validator::Validator() {
    std::thread commitIntentsSerializer(serialize);
}

bool
Validator::updateTxEtaPi(TXEntry &txEntry) {
    /*
     * Find out my largest predecessor (eta) and smallest successor (pi).
     * For reads, see if another has over-written the tuples by checking successor LSN.
     * For writes, see if another has read the tuples by checking access LSN.
     *
     * We use single-version in-memory KV store. Any stored tuple is the latest
     * committed version. Therefore, we are implementing SSN over RC (Read-Committed).
     * Moreover, the validator does not store the uncommitted write set; the tx client
     * is to pass the write set through the commit-intent.
     */

    txEntry.setPi(std::min(txEntry.getPi(), txEntry.getCTS()));

    auto &readSet = txEntry.getReadSet();
    for (uint32_t i = 0; i < readSet.size(); i++) {
        uint64_t vPi = Validator::getTuplePi(*readSet.at(i));
        txEntry.setPi(std::min(txEntry.getPi(), vPi));
        if (txEntry.isExclusionViolated()) {
            txEntry.setTxState(TXEntry::TX_ABORT);
            return false;
        }
    }

    auto  &writeSet = txEntry.getWriteSet();
    for (uint32_t i = 0; i < writeSet.size(); i++) {
        uint64_t vPrevEta = Validator::getTuplePrevEta(*writeSet.at(i));
        txEntry.setEta(std::max(txEntry.getEta(), vPrevEta));
        if (txEntry.isExclusionViolated()) {
            txEntry.setTxState(TXEntry::TX_ABORT);
            return false;
        }
    }

    return true;
}

bool
Validator::updateReadsetEta(TXEntry &txEntry) {
    auto &readSet = txEntry.getReadSet();
    for (uint32_t i = 0; i < readSet.size(); i++) {
        maximizeTupleEta(*readSet.at(i), txEntry.getCTS());
    }
    return true;
}

bool
Validator::updateWriteset(TXEntry &txEntry) {
    auto &writeSet = txEntry.getWriteSet();
    for (uint32_t i = 0; i < writeSet.size(); i++) {
        updateTuple(*writeSet.at(i), txEntry);
    }
    return true;
}

bool
Validator::validate(TXEntry& txEntry) {
    //calculate local eta and pi
    updateTxEtaPi(txEntry);

    //update commit intent state
    txEntry.setTxCIState = TXEntry::TX_CI_INPROGRESS;

    if (txEntry.getShardSet().size() > 1) { // cross-shard transaction
        //send out eta and pi
        //wait and check for all peers
        //if timed-out, loop to recover
    }

    if (txEntry.isExclusionViolated()) {
        txEntry.setTxState(TXEntry::TX_ABORT);
    } else {
        /*
         * Validation is passed. Record decision in this sequence:
         * WAL, update state, update tuple store: in-mem (and then pmem).
         */

        // WAL - write-ahead log, for failure recovery

        // update state
        txEntry.setTxState(TXEntry::TX_COMMIT);

        // update in-mem tuple store
        updateReadsetEta(txEntry);
        updateWriteset(txEntry);

    }

    return true;
};

void
Validator::serialize() {
    /*
     * This loop handles the DSSN serialization window critical section
     */
    while (true) {
        // process due commit-intents on cross-shard transaction queue

        // process all commit-intents on local transaction queue
        for (uint32_t i = 0; i < localTXQueue.unsafe_size(); i++) {
            TXEntry* txEntry;
            if (localTXQueue.try_pop(txEntry)) {
                if (activeTxSet.depends(txEntry)) {
                    localTXQueue.push(txEntry); // re-enqueued as this tx may be unblocked later
                } else {
                    // As local transactions can be validated in any order, we can set the CTS.
                    txEntry->setCTS(12345 /* deduce an approx uint64_t CTS LATER */);

                    // There is no need to update activeTXs because this tx is validated and concluded shortly

                    validateLocalTx(txEntry);

                    conclude(txEntry);
                }
            }
        }
    }
}
} // end Validator class

