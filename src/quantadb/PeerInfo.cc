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

#include <algorithm>
#include "PeerInfo.h"
#include "Logger.h"

namespace QDB {

PeerInfo::~PeerInfo() {
    auto itr = peerInfo.begin();
    while (itr != peerInfo.end()) {
        PeerEntry *peerEntry = (*itr).second;
        itr = peerInfo.unsafe_erase(itr);
        delete peerEntry;
    }
}

bool
PeerInfo::add(CTS cts, TxEntry *txEntry, Validator *validator) {
    mutexForPeerAdd.lock();
    PeerInfoIterator it = peerInfo.find(cts);
    if (it == peerInfo.end()) {
        RAMCLOUD_LOG(NOTICE, "addPeer %lu %lu txEntry %lu", (uint64_t)(cts >> 64),
                (uint64_t)(cts & (((__uint128_t)1<<64) -1)), (uint64_t)txEntry);

        PeerEntry* entry = new PeerEntry();
        entry->isValid = true;
        entry->txEntry = txEntry;
        entry->meta.cStamp = cts >> 64;
        if (txEntry != NULL) {
            if (txEntry->getCTS() != cts) abort();
            if (txEntry->getPeerSet().size() == 0) abort();
            entry->meta.pStamp = txEntry->getPStamp();
            entry->meta.sStamp = txEntry->getSStamp();
        }
        if (!peerInfo.insert(std::make_pair(cts, entry)).second) {
            RAMCLOUD_LOG(NOTICE, "addPeer failed %lu %lu txEntry %lu", (uint64_t)(cts >> 64),
                    (uint64_t)(cts & (((__uint128_t)1<<64) -1)), (uint64_t)txEntry);
            delete entry;
            return false;
        }
        validator->getCounters().addPeers.fetch_add(1);
    } else if (txEntry != NULL) {
        PeerEntry* existing = it->second;
        existing->mutexForPeerUpdate.lock();
        if (existing->isValid && existing->txEntry == NULL) {
            if (txEntry->getCTS() != cts) abort();
            existing->txEntry = txEntry;
            txEntry->setPStamp(std::max(txEntry->getPStamp(), existing->meta.pStamp));
            txEntry->setSStamp(std::min(txEntry->getSStamp(), existing->meta.sStamp));
            evaluate(existing, txEntry, validator);
            validator->getCounters().matchEarlyPeers++;

            RAMCLOUD_LOG(NOTICE, "matchPeer %lu %lu txEntry %lu", (uint64_t)(txEntry->getCTS() >> 64),
                    (uint64_t)(txEntry->getCTS() & (((__uint128_t)1<<64) -1)), (uint64_t)txEntry);
        } else if (txEntry != it->second->txEntry){
            RAMCLOUD_LOG(NOTICE, "duplicate %lu txEntry new %lu old %lu",
                    (uint64_t)(txEntry->getCTS() >> 64), (uint64_t)txEntry, (uint64_t)it->second->txEntry);
            exit(0); //should not have called add() with old CTS and should not have found non-zero txEntry
        }
        existing->mutexForPeerUpdate.unlock();
    }
    mutexForPeerAdd.unlock();
    return true;
}

//Fixme: unused currently
bool
PeerInfo::addPartial(CTS cts, uint64_t peerId, uint8_t peerTxState, uint64_t eta, uint64_t pi,
        Validator *validator) {
    //mutexForPeerAdd.lock();
    PeerInfoIterator it = peerInfo.find(cts);
    if (it == peerInfo.end()) {
        RAMCLOUD_LOG(NOTICE, "addPartial %lu %lu ", (uint64_t)(cts >> 64),
                (uint64_t)(cts & (((__uint128_t)1<<64) -1)));

        PeerEntry* entry = new PeerEntry();
        entry->isValid = true;
        entry->txEntry = NULL;
        entry->meta.cStamp = cts >> 64;
        if (!peerInfo.insert(std::make_pair(cts, entry)).second)
		abort();
        validator->getCounters().addPeers++;
        //mutexForPeerAdd.unlock();

        //Because the global mutex has been unlocked above, the CI might have been processed
        //by other threads before the following call is completed, so the peerEntry may or may not be
        //found, and the returning txEntry may or may not be found.
        bool isFound;
        update(cts, peerId, peerTxState, eta, pi, validator, isFound);
    } else {
        //mutexForPeerAdd.unlock();
    }
    return true;
}

bool
PeerInfo::remove(CTS cts, Validator *validator) {
    mutexForPeerAdd.lock();
    PeerInfoIterator it = peerInfo.find(cts);
    if (it != peerInfo.end()) {
        PeerEntry* peerEntry = it->second;
        if (peerEntry == NULL) abort();
        peerEntry->mutexForPeerUpdate.lock();
        RAMCLOUD_LOG(NOTICE, "remove cts %lu", (uint64_t)(cts >> 64));
        peerEntry->isValid = false;
        //delete peerEntry->txEntry;
        peerEntry->txEntry = NULL;
        //peerInfo.unsafe_erase(it);
        peerInfo.unsafe_erase(cts);
        peerEntry->mutexForPeerUpdate.unlock();
        //delete peerEntry;
        //validator->getCounters().deletedPeers++;
        mutexForPeerAdd.unlock();
        return true;
    }
    mutexForPeerAdd.unlock();
    abort();
    return false;
}

inline bool
PeerInfo::evaluate(PeerEntry *peerEntry, TxEntry *txEntry, Validator *validator) {
    RAMCLOUD_LOG(NOTICE, "evaluate cts %lu  states %u %u",
            (uint64_t)(txEntry->getCTS() >> 64), txEntry->getTxState(), txEntry->getTxCIState());

    //Currently only in the TX_CI_LISTENING state, the txEntry has updated its local
    //pstamp and sstamp and has been scheduled to use peer pstamp and sstamp to evaluate.
    if (txEntry->getTxCIState() == TxEntry::TX_CI_LISTENING) {
        if (txEntry->getTxState() == TxEntry::TX_ALERT) {
            //An alerted state is only allowed to transit into commit state by a peer in commit state
            //so that there will be no race condition into conflict state, where the local would commit
            //while the peer would abort. An alerted state can transit into abort state when all peers are
            //in alerted state or exclusion window is violated.
            if (peerEntry->peerTxState == TxEntry::TX_COMMIT) {
                txEntry->setTxState(TxEntry::TX_COMMIT);
                txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
            } else if (peerEntry->peerTxState == TxEntry::TX_ABORT
                    || peerEntry->peerAlertSet == txEntry->getPeerSet()) {
                txEntry->setTxState(TxEntry::TX_ABORT);
                txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
                validator->getCounters().alertAborts++;
            } else if (txEntry->isExclusionViolated()) {
                txEntry->setTxState(TxEntry::TX_ABORT);
                txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
            }
        } else if (txEntry->getTxState() == TxEntry::TX_PENDING) {
            if (txEntry->isExclusionViolated()) {
                txEntry->setTxState(TxEntry::TX_ABORT);
                txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
            } else if (txEntry->getPeerSet() == peerEntry->peerSeenSet
                    && peerEntry->peerAlertSet.empty()) {
                txEntry->setTxState(TxEntry::TX_COMMIT);
                txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
            }
        }
    }

    //Logging must precede sending tx CI reply
    //Note that our overall scheme does not need to log local transactions at all
    //By now the tuples should have successfully been preput into the KV store, so
    //logging the CI conclusion is considered sealing a tx commit.
    if (txEntry->getTxCIState() == TxEntry::TX_CI_CONCLUDED) {
        if (validator->logTx(LOG_ALWAYS, txEntry)) {
            txEntry->setTxCIState(TxEntry::TX_CI_SEALED);
            if (!validator->insertConcludeQueue(txEntry))
		abort();
        } else
            abort();
    }

    if ((txEntry->getTxState() == TxEntry::TX_ABORT && peerEntry->peerTxState == TxEntry::TX_COMMIT) ||
            (txEntry->getTxState() == TxEntry::TX_COMMIT && peerEntry->peerTxState == TxEntry::TX_OUTOFORDER) ||
            (txEntry->getTxState() == TxEntry::TX_COMMIT && peerEntry->peerTxState == TxEntry::TX_ABORT)) {
        abort(); //there must be a design problem -- debug
        txEntry->setTxState(TxEntry::TX_CONFLICT);
    }

    return true; //concluded
}

inline bool
PeerInfo::logAndSend(TxEntry *txEntry, Validator *validator) {
    validator->updateTxPStampSStamp(*txEntry);

    if (txEntry->getTxState() == TxEntry::TX_ALERT) {
        //this is in recovery case; skip logging and sending in pending state
        txEntry->setTxCIState(TxEntry::TX_CI_LISTENING);
    } else if (validator->logTx(LOG_ALWAYS, txEntry)) { //log CI before initial sending
        assert(txEntry->getTxState() == TxEntry::TX_PENDING);
        validator->sendSSNInfo(txEntry);
        txEntry->setTxCIState(TxEntry::TX_CI_LISTENING);
    } else {
        RAMCLOUD_LOG(NOTICE, "logTx failed: cts %lu", (uint64_t)(txEntry->getCTS() >> 64));
        abort();
        return false;
    }
    return true;
}

TxEntry*
PeerInfo::update(CTS cts, uint64_t peerId, uint8_t peerTxState, uint64_t pstamp, uint64_t sstamp,
        Validator *validator, bool &isFound) {
    TxEntry *txEntry= NULL;
    PeerInfoIterator it;

    //Make sure that the peerInfo by CTS is valid
    ///The lock/unlock of the global mutex and per-entry mutex is designed to avoid
    ///the race condition that iterator is referring to a peerEntry (or its txEntry)
    ///which has been freed right after find().
    mutexForPeerAdd.lock();
    it = peerInfo.find(cts);
    if (it != peerInfo.end()) {
        PeerEntry* peerEntry = it->second;
        if (peerEntry == NULL) abort();
        peerEntry->mutexForPeerUpdate.lock();
        mutexForPeerAdd.unlock();
        if (peerEntry->isValid) {
            peerEntry->meta.pStamp = std::max(peerEntry->meta.pStamp, pstamp);
            peerEntry->meta.sStamp = std::min(peerEntry->meta.sStamp, sstamp);
            peerEntry->peerSeenSet.insert(peerId);
            if (peerTxState == TxEntry::TX_ALERT) {
                peerEntry->peerTxState = TxEntry::TX_ALERT;
                peerEntry->peerAlertSet.insert(peerId);
            } else if (peerTxState == TxEntry::TX_ABORT || peerTxState == TxEntry::TX_OUTOFORDER) {
                assert(peerEntry->peerTxState != TxEntry::TX_COMMIT);
                peerEntry->peerTxState = TxEntry::TX_ABORT;
            } else if (peerTxState == TxEntry::TX_COMMIT) {
                assert(peerEntry->peerTxState != TxEntry::TX_ABORT);
                peerEntry->peerTxState = TxEntry::TX_COMMIT;
            }
            txEntry = peerEntry->txEntry;
            if (txEntry != NULL) {
                if (txEntry->getCTS() != cts) abort(); //make sure txEntry contents not corrupted
                txEntry->setPStamp(std::max(txEntry->getPStamp(), peerEntry->meta.pStamp));
                txEntry->setSStamp(std::min(txEntry->getSStamp(), peerEntry->meta.sStamp));

                if (txEntry->getTxCIState() == TxEntry::TX_CI_SCHEDULED) {
                    logAndSend(txEntry, validator);
                }

                evaluate(peerEntry, txEntry, validator);
            }
            isFound = true;
            RAMCLOUD_LOG(NOTICE, "updatePeer %lu %lu txEntry %lu peerId %lu cnt %lu", (uint64_t)(cts >> 64),
                    (uint64_t)(cts & (((__uint128_t)1<<64) -1)), (uint64_t)txEntry,
                    peerId, peerEntry->peerSeenSet.size());
        } else {
            isFound = true;
            RAMCLOUD_LOG(NOTICE, "updatePeer ignored: finished cts %lu %lu peerId %lu", (uint64_t)(cts >> 64),
                    (uint64_t)(cts & (((__uint128_t)1<<64) -1)), peerId);
        }
        peerEntry->mutexForPeerUpdate.unlock();
        return txEntry;
    }
    mutexForPeerAdd.unlock();
    isFound = false;
    RAMCLOUD_LOG(NOTICE, "updatePeer ignored: no cts %lu peerId %lu", (uint64_t)(cts >> 64), peerId);
    return txEntry;
}

bool
PeerInfo::send(Validator *validator) {
    uint64_t nsTime = validator->getClockValue();
    uint64_t currentTick = nsTime / tickUnit;

    while (!activeTxQueue.empty()) {
        CTS cts;
	if (!activeTxQueue.pop(cts))
	    abort();
	RAMCLOUD_LOG(NOTICE, "monitoring: cts %lu", (uint64_t)(cts >> 64));
	PeerInfoIterator it;
	it = peerInfo.find(cts);
	if (it != peerInfo.end()) {
            iteratorQueue.push(it->second);
        }
    }

    //the use of iterator of concurrent_unordered_map while another thread doing erase
    //is observed with a lock-up problem with mutexForPeerUpdate. For now let's protect
    //the whole section with mutexForPeerAdd.

    uint64_t count = iteratorQueue.size();
    while (count > 0 && !iteratorQueue.empty()) {
        PeerEntry *peerEntry = iteratorQueue.front();
        iteratorQueue.pop();
        count--;
        peerEntry->mutexForPeerUpdate.lock();
        if (!peerEntry->isValid) {
            peerEntry->mutexForPeerUpdate.unlock();
            delete peerEntry;
            validator->getCounters().deletedPeers++;
            continue;
        }

        TxEntry* txEntry = peerEntry->txEntry;

        if (currentTick > lastTick)
            RAMCLOUD_LOG(NOTICE, "finding: cts %lu states %u %u %u now %lu",
                (uint64_t)(txEntry->getCTS() >> 64), txEntry->getTxState(),
                txEntry->getTxCIState(), peerEntry->peerTxState, nsTime);

        if (txEntry->getTxCIState() == TxEntry::TX_CI_SCHEDULED) {
            logAndSend(txEntry, validator);
            evaluate(peerEntry, txEntry, validator);
        }

        if (txEntry->getTxCIState() == TxEntry::TX_CI_LISTENING) {
            if (txEntry->getTxState() != TxEntry::TX_ALERT
                && nsTime > (uint64_t)(txEntry->getCTS() >> 64)
                && nsTime - (uint64_t)(txEntry->getCTS() >> 64) > alertThreshold) {
                txEntry->setTxState(TxEntry::TX_ALERT);
            }

            if (txEntry->getTxState() == TxEntry::TX_ALERT) {
                //request missing SSN info from peers periodically;
                if (currentTick > lastTick) {
                    std::set<uint64_t>::iterator it;
                    for (it = txEntry->getPeerSet().begin(); it != txEntry->getPeerSet().end(); it++) {
                        validator->requestSSNInfo(txEntry, true, *it);
                    }
                }
            }
        }

        if (txEntry->getTxCIState() == TxEntry::TX_CI_QUEUED
                && txEntry->getTxState() == TxEntry::TX_OUTOFORDER) {
            //trigger conclusion, but keep state because this CI has never been added to activeTxSet
            txEntry->setSStamp(0);
            validator->sendSSNInfo(txEntry); //so that peers do not wait for alert
            txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
            evaluate(peerEntry, txEntry, validator);
        }

        if (txEntry->getTxCIState() < TxEntry::TX_CI_CONCLUDED) {
            iteratorQueue.push(peerEntry);
        }

        peerEntry->mutexForPeerUpdate.unlock();

        //Fixme: think about how to unlock the mutex before inserting into concludeQ within evaluate() so that
        //serialize() does not need to wait for mutex
    }
    lastTick = currentTick;
    return true;
}

uint32_t
PeerInfo::size() {
    return peerInfo.size();
}

bool
PeerInfo::monitor(CTS cts, Validator *validator) {
    RAMCLOUD_LOG(NOTICE, "monitor %lu %lu ", (uint64_t)(cts >> 64),
            (uint64_t)(cts & (((__uint128_t)1<<64) -1)));
    return activeTxQueue.push(cts);
}


} // end PeerInfo class

