/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "KVInterface.h"
#include "sstream"
#include "Validator.h"
#include <thread>

namespace DSSN {

const uint64_t maxTimeStamp = std::numeric_limits<uint64_t>::max();
const uint64_t minTimeStamp = 0;

inline std::string formTupleKey(Object& tuple) {
    KeyLength kLen;
    const uint8_t* key = (const uint8_t *)tuple.getKey(0, &kLen);
    if (key == NULL) // there is a bug if it happens
        return "";
    uint64_t tableId = tuple.getTableId();
    std::vector<uint8_t> ckey(sizeof(tableId) + kLen);
    for (uint32_t i = 0; i < sizeof(tableId); i++)
        ckey[i] = ((uint8_t *)&tableId)[i];
    for (uint32_t i = 0; i < kLen; i++)
        ckey[sizeof(tableId) + i] = key[i];
    return std::string(ckey.begin(), ckey.end());
}

uint64_t
Validator::getTuplePi(Object& object) {
    std::string tupleKey = formTupleKey(object);
    if (tupleKey.empty())
        return minTimeStamp; //cause exclusion violation
    DSSNMeta meta;
    Validator::tupleStore.getMeta(tupleKey, meta);
    return meta.cStamp;
}

uint64_t
Validator::getTuplePrevEta(Object& object) {
    std::string tupleKey = formTupleKey(object);
    if (tupleKey.empty())
        return maxTimeStamp; //cause exclusion violation
    DSSNMeta meta;
    Validator::tupleStore.getMeta(tupleKey, meta);
    return meta.sStampPrev;
}

uint64_t
Validator::getTuplePrevPi(Object& object) {
    return 0; //not used yet
}

const std::string *
Validator::getTupleValue(Object& object) {
    std::string tupleKey = formTupleKey(object);
    if (tupleKey.empty())
        return NULL;
    DSSNMeta meta;
    return Validator::tupleStore.get(tupleKey, meta);
}

bool
Validator::maximizeTupleEta(Object& object, uint64_t eta) {
    std::string tupleKey = formTupleKey(object);
    if (tupleKey.empty())
        return false;
    DSSNMeta meta;
    Validator::tupleStore.getMeta(tupleKey, meta);
    meta.pStamp = std::max(eta, meta.pStamp);
    Validator::tupleStore.updateMeta(tupleKey, meta);
    return true;
}

bool
Validator::updateTuple(Object& object, TxEntry& txEntry) {
    std::string tupleKey = formTupleKey(object);
    if (tupleKey.empty())
        return false;
    DSSNMeta meta;
    Validator::tupleStore.getMeta(tupleKey, meta);

    /* because we have a single version HOT, copy current data to prev data */
    meta.pStampPrev = meta.pStamp;

    /* SSN required operations */
    meta.sStampPrev = txEntry.getPi();
    meta.cStamp = meta.pStamp = txEntry.getCTS();
    meta.sStamp = maxTimeStamp;

    /* tuple: key, pointer to object, meta-data */
    std::stringstream ss;
    ss << static_cast<const void*>(&object);
    tupleStore.put(tupleKey, ss.str(), meta);
    return true;
}

void
Validator::start() {
    // Henry: may need to use TBB to pin the threads to specific cores LATER
    std::thread( [=] { serialize(); });
    std::thread( [=] { validateDistributedTxs(0); });
}

bool
Validator::updateTxEtaPi(TxEntry &txEntry) {
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
            txEntry.setTxState(TxEntry::TX_ABORT);
            return false;
        }
    }

    auto  &writeSet = txEntry.getWriteSet();
    for (uint32_t i = 0; i < writeSet.size(); i++) {
        uint64_t vPrevEta = Validator::getTuplePrevEta(*writeSet.at(i));
        txEntry.setEta(std::max(txEntry.getEta(), vPrevEta));
        if (txEntry.isExclusionViolated()) {
            txEntry.setTxState(TxEntry::TX_ABORT);
            return false;
        }
    }

    return true;
}

bool
Validator::updateReadsetEta(TxEntry &txEntry) {
    auto &readSet = txEntry.getReadSet();
    for (uint32_t i = 0; i < readSet.size(); i++) {
        maximizeTupleEta(*readSet.at(i), txEntry.getCTS());
    }
    return true;
}

bool
Validator::updateWriteset(TxEntry &txEntry) {
    auto &writeSet = txEntry.getWriteSet();
    for (uint32_t i = 0; i < writeSet.size(); i++) {
        updateTuple(*writeSet.at(i), txEntry);
    }
    return true;
}

bool
Validator::validateLocalTx(TxEntry& txEntry) {
    //calculate local eta and pi
    updateTxEtaPi(txEntry);

    if (txEntry.isExclusionViolated()) {
        txEntry.setTxState(TxEntry::TX_ABORT);
    } else {
        txEntry.setTxState(TxEntry::TX_COMMIT);
    }
    txEntry.setTxCIState(TxEntry::TX_CI_CONCLUDED);

    return true;
};

void
Validator::validateDistributedTxs(int worker) {

    /* Scheme B3
    while (true) {
        for (SkipList<std::vector<uint8_t>,TXEntry *>::iterator itr = activeTxSet[worker].begin(); itr != activeTxSet[worker].end(); ++itr) {
            TXEntry *txEntry = itr;

            if (txEntry->getTxState() == TXEntry::TX_PENDING 
                    && txEntry->getTxCIState() == TXEntry::TX_CI_TRANSIENT) {

                //normal case first try:

                //calculate local ETA and PI
                updateTxEtaPi(*txEntry);

                //send PEER_SSN_INFO to Tx peers.
                //FIXME: SendTxPeerSSNInfo(txEntry);

                txEntry->setTXCiState(TXEntry::TX_CI_INPROGRESS);
            }

            if (txEntry->getTxState() == TXEntry::TX_PENDING) {
                uint64_t waitingTime = currentTime() - txEntry->getCTS();
                if (waitingTime > abortThreshold) {
                    txEntry->setTxState(TXEntry::TX_ABORT);
                } else if (waitingTime > alertThreshold) {
                    //such tx should be waiting for the peer's SSN info
                    assert (txEntry->getTxCiState() == TXEntry::TX_CI_INPROGRESS);

                    //request PEER_SSN_INFO from Tx peers. (blocking/non-blocking)
                    //FIXME: RequestTxPeerSSNInfo(txEntry);
                    //
                    //after the request, either here or the PEER_SSN_INFO handler
                    //will set the TxEntry's state to TX_ABORT or TX_COMMIT.
                }
            }

            if (txEntry->getTxState() == TXEntry::TX_ABORT
                    || (txEntry->getTxState() == TXEntry::TX_COMMIT) {

                //FIXME: the PEER_SSN_INFO RPC handler will conclude
                //conclude(*txEntry);

                activeTxSet[worker].remove(txEntry);
            }
        }
    }
    */
            


    /*  
    while (true) {
        for (SkipList<std::vector<uint8_t>,TXEntry *>::iterator itr = reorderQueue.begin(); itr != reorderQueue.end(); ++itr) {
            TXEntry *txEntry = itr;
            if (txEntry->getCTS() > currentTime()) {
                break; //no need to look further in the sorted queue
            }
            if (txEntry->getTxState() == TXEntry::TX_PENDING
                    && txEntry->getTxCIState() == TXEntry::TX_CI_TRANSIENT) {
                //calculate local eta and pi
                updateTxEtaPi(*txEntry);

                //log the commit-intent for failure recovery
                //LATER

                //send non-blocking SEND_SSN_INFO IPC messages to tx peers
                //LATER

                //update state
                txEntry->setTxCIState(TXEntry::TX_CI_INPROGRESS);
            }
            if (txEntry->getTxState() == TXEntry::TX_PENDING) {
                //if tx takes too long, try to abort it
                if (txEntry->getCTS() - currentTime() > alertThreshold)
                    txEntry->setTxState(TXEntry::TX_ALERT);
            } else if (txEntry->getTxState() == TXEntry::TX_ALERT) {
                //send non-blocking REQUEST_SSN_INFO IPC to tx peers
            } else if (txEntry->getTxState() == TXEntry::TX_ABORT
                    || txEntry->getTxState() == TXEntry::TX_COMMIT) {
                //remove tx from reorder queue
                reorderQueue.remove(txEntry);

                //trigger conclusion, be it an abort or a commit
                conclude(*txEntry);

                //remove from active tx set if needed
                if (txEntry->getTxCIState() == TXEntry::TX_CI_TRANSIENT
                        || txEntry->getTxCIState() == TXEntry::TX_CI_INPROGRESS
                        || txEntry->getTxCIState() == TXEntry::TX_CI_CONCLUDED) {
                    activeTxSet.remove(txEntry);
                }
            }
        }
    }
    */
}

void
Validator::serialize() {
    /*
     * This loop handles the DSSN serialization window critical section
     */
    while (true) {
        // process due commit-intents on cross-shard transaction queue
        
        /* Scheme B3
        TXEntry *txEntry;
        // FIXME: do we need to wait for empty execution slot here?
        
        if (txEntry = blockedTxSet.findReadyTx()) {
            activeTxSet[worker].add(txEntry);
            blockedTxSet.remove(txEntry);
            continue;
        }

        //FIXME consider changing this to add itr one at a time.
        for (SkipList<std::vector<uint8_t>,TXEntry *>::iterator itr = reorderQueue.begin(); itr != reorderQueue.end(); ++itr) {
            txEntry = itr;
            if (txEntry->getCTS() > currentTime() ) {
                break;
            } else if (blockfactor = blockedTxSet.blocks(txEntry)) {
                blockedTxSet.add(txEntry, blockfactor);
            } else if (blockfactor = activeTxSet.blocks(txEntry)) {
                blockedTxSet.add(txEntry, blockfactor);
            } else {
                activeTxSet[worker].add(txEntry);
                // FIXME: Log the Commit Intent 
            }
        }
        */

        /*
        for (SkipList<std::vector<uint8_t>,TXEntry *>::iterator itr = reorderQueue.begin(); itr != reorderQueue.end(); ++itr) {
            TXEntry *txEntry = itr;
            if (txEntry->getCTS() > currentTime()) {
                break; //no need to look further in the sorted queue
            } else if (txEntry->getTxCIState() == TXEntry::TX_CI_QUEUED) {
                //check dependency on earlier transactions
                if (activeTxSet.depends(txEntry)) {
                    if (txEntry->getTxCIState() == TXEntry::TX_CI_QUEUED) {
                        txEntry->setTxCIState(TXEntry::TX_CI_WAITING);
                        blockedTxSet.add(txEntry);
                    }
                    continue;
                }
                if (blockedTxSet.depends(txEntry)) {
                    continue;
                }

                //schedule for validation as there is no dependency
                if (activeTxSet.add(txEntry)) {
                    txEntry->setTxCIState(TXEntry::TX_CI_TRANSIENT);

                    //remove from blocked tx set if needed
                    if (blockedTxSet.contains(txEntry)
                        blockedTxSet.remove(txEntry);
                }
            }
        }
        */

        // process all commit-intents on local transaction queue
        for (uint32_t i = 0; i < localTxQueue.unsafe_size(); i++) {
            TxEntry* txEntry;
            if (localTxQueue.try_pop(txEntry)) {
                if (activeTxSet.blocks(txEntry)) {
                    localTxQueue.push(txEntry); // re-enqueued as this tx may be unblocked later
                } else {
                    /* There is no need to update activeTXs because this tx is validated
                     * and concluded shortly. If the conclude() does through a queue and another
                     * task, then we should add tx to active tx set here.
                     */

                    // As local transactions can be validated in any order, we can set the CTS.
                    txEntry->setCTS(12345 /* deduce an approx uint64_t CTS LATER */);

                    validateLocalTx(*txEntry);

                    conclude(*txEntry);
                }
            }
        }
    } //end while(true)
}

bool
Validator::conclude(TxEntry& txEntry) {
    /*
     * log the commit result of a local tx.
     * log the commit/abort result of a distributed tx as its CI has been logged
     */
    if (txEntry.getShardSet().size() > 1
            || txEntry.getTxState() == TxEntry::TX_COMMIT) {
        //WAL and persist value LATER

        // update in-mem tuple store
        if (txEntry.getTxState() == TxEntry::TX_COMMIT) {
            updateReadsetEta(txEntry);
            updateWriteset(txEntry);
        }

        //reply to commit intent client
        //LATER
    }
    return true;
}
} // end Validator class

