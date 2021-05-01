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

PeerInfo::PeerInfo() {
    for (uint32_t i = 0; i < TBLSZ; i++)
        recycleQueue.push(i);
}

PeerInfo::~PeerInfo() {
}

bool
PeerInfo::poseEvent(uint32_t eventType, CTS cts, uint64_t peerId, uint8_t peerPosition, uint32_t peerTxState, uint64_t eta, uint64_t pi, TxEntry *txEntry, PeerEntry *peerEntry) {
    PeerEvent *peerEvent = new PeerEvent();
    peerEvent->eventType = eventType;
    peerEvent->cts = cts;
    peerEvent->peerId = peerId;
    peerEvent->peerPosition = peerPosition;
    peerEvent->peerTxState = peerTxState;
    peerEvent->peerPStamp = eta;
    peerEvent->peerSStamp = pi;
    peerEvent->txEntry = txEntry;
    peerEvent->peerEntry = peerEntry;
    RAMCLOUD_LOG(NOTICE, "pose event %u cts %lu txEntry %lu peerEntry %lu",
            eventType, (uint64_t)(cts >> 64), (uint64_t)txEntry, (uint64_t)peerEntry);
    bool ret = eventQueue.push(peerEvent);
    if (!ret)
        RAMCLOUD_LOG(ERROR, "queue full 3: cts %lu", (uint64_t)(cts >> 64));
    return ret;
}

bool
PeerInfo::processEvent(Validator *validator) {
    while (!eventQueue.empty()) {
        PeerEvent *peerEvent;
        if (!eventQueue.pop(peerEvent))
            abort();

        RAMCLOUD_LOG(NOTICE, "process event %u cts %lu txEntry %lu",
                peerEvent->eventType, (uint64_t)(peerEvent->cts >> 64), (uint64_t)peerEvent->txEntry);

        if (peerEvent->eventType == 1) { //insert without txEntry
            uint32_t myTxState;
            uint64_t myPStamp, mySStamp;
            uint8_t myPeerPosition;
            if (!update(peerEvent->cts, peerEvent->peerId, peerEvent->peerTxState,
                    peerEvent->peerPStamp, peerEvent->peerSStamp, peerEvent->peerPosition,
                    myTxState, myPStamp, mySStamp, myPeerPosition, validator)) {
                while (!add(peerEvent->cts, NULL, validator)) {
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                }
                update(peerEvent->cts, peerEvent->peerId, peerEvent->peerTxState,
                        peerEvent->peerPStamp, peerEvent->peerSStamp, peerEvent->peerPosition,
                        myTxState, myPStamp, mySStamp, myPeerPosition, validator);
            }
            validator->getCounters().peerEventUpds++;
        } else if (peerEvent->eventType == 3) { //insert with txEntry
            while (!add(peerEvent->cts, peerEvent->txEntry, validator)) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
            validator->getCounters().peerEventAdds++;
        } else if (peerEvent->eventType == 2) { //remove
            PeerInfoIterator it = peerInfo.find(peerEvent->cts);
            peerEntryTable[it->second].isConcluded = true;
            recycleQueue.push(it->second);
            RAMCLOUD_LOG(NOTICE, "recycle idx %u", it->second);
            validator->getCounters().peerEventDels++;
        }
        delete peerEvent;
    }
    return true;
}

bool
PeerInfo::add(CTS cts, TxEntry *txEntry, Validator *validator) {
    PeerInfoIterator it = peerInfo.find(cts);
    if (it == peerInfo.end()) {
        if (recycleQueue.empty()) {
            RAMCLOUD_LOG(ERROR, "addPeer failed %lu full", (uint64_t)(cts >> 64));
            return false;
        }
        uint32_t freeIdx = recycleQueue.front();
        PeerEntry* entry = &peerEntryTable[freeIdx];
        //entry->mutexForPeerUpdate.lock();

        if (!entry->isConcluded) {
            RAMCLOUD_LOG(ERROR, "addPeer failed %lu bad entry", (uint64_t)(cts >> 64));
            return false;
        }

        RAMCLOUD_LOG(NOTICE, "addPeer %lu %lu txEntry %lu idx %u", (uint64_t)(cts >> 64),
                (uint64_t)(cts & (((__uint128_t)1<<64) -1)), (uint64_t)txEntry, freeIdx);


        if (entry->txEntry) {
            RAMCLOUD_LOG(NOTICE, "remove old cts %lu for cts %lu idx %u",
                    (uint64_t)(entry->cts >> 64), (uint64_t)(cts >> 64), freeIdx);
            peerInfo.unsafe_erase(entry->cts);
            delete entry->txEntry;
            entry->txEntry = NULL;
            validator->getCounters().deletedPeers++;
        }
        entry->isConcluded = false;
        entry->cts = cts;
        entry->peerAlertSet = 0;;
        entry->peerSeenSet = 0;
        entry->peerTxState = TxEntry::TX_PENDING;
        entry->meta.cStamp = cts >> 64;
        entry->meta.pStamp = entry->meta.pStampPrev = 0;
        entry->meta.sStampPrev = entry->meta.sStamp = 0xffffffffffffffff;
        if (txEntry != NULL) {
            if (txEntry->getCTS() != cts) abort();
            entry->meta.pStamp = txEntry->getPStamp();
            entry->meta.sStamp = txEntry->getSStamp();
            entry->txEntry = txEntry;
            if (!peerInfo.insert(std::make_pair(cts, freeIdx)).second) {
                RAMCLOUD_LOG(ERROR, "addPeer failed %lu %lu txEntry %lu idx %u", (uint64_t)(cts >> 64),
                        (uint64_t)(cts & (((__uint128_t)1<<64) -1)), (uint64_t)txEntry, freeIdx);
                //entry->mutexForPeerUpdate.unlock();
                return false;
            }
            send(entry, validator);
        }

        //entry->mutexForPeerUpdate.unlock();
        recycleQueue.pop();
        validator->getCounters().addPeers.fetch_add(1);
    } else if (txEntry != NULL) {
        PeerEntry* existing = &peerEntryTable[it->second];
        existing->mutexForPeerUpdate.lock();
        if (txEntry->getCTS() != cts) {
            RAMCLOUD_LOG(ERROR, "inconsistent peer %lu txEntry %lu",
                    (uint64_t)(cts >> 64), (uint64_t)(txEntry->getCTS() >> 64));
            existing->mutexForPeerUpdate.unlock();
            return false;
        }
        if (existing->txEntry == NULL) {
            existing->txEntry = txEntry;
            txEntry->setPStamp(std::max(txEntry->getPStamp(), existing->meta.pStamp));
            txEntry->setSStamp(std::min(txEntry->getSStamp(), existing->meta.sStamp));
            send(existing, validator);
            evaluate(existing, txEntry, validator);
            validator->getCounters().matchEarlyPeers++;

            RAMCLOUD_LOG(NOTICE, "matchPeer %lu %lu txEntry %lu", (uint64_t)(txEntry->getCTS() >> 64),
                    (uint64_t)(txEntry->getCTS() & (((__uint128_t)1<<64) -1)), (uint64_t)txEntry);
        } else if (txEntry != existing->txEntry){
            RAMCLOUD_LOG(ERROR, "duplicate %lu txEntry new %lu old %lu",
                    (uint64_t)(txEntry->getCTS() >> 64), (uint64_t)txEntry, (uint64_t)existing->txEntry);
            existing->mutexForPeerUpdate.unlock();
            return false; //should not have called add() with old CTS and should not have found non-zero txEntry
        }
        existing->mutexForPeerUpdate.unlock();
    }
    return true;
}

inline bool
PeerInfo::evaluate(PeerEntry *peerEntry, TxEntry *txEntry, Validator *validator) {

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
                txEntry->setTxResult(TxEntry::TX_COMMIT_PEER);
            } else if (peerEntry->peerTxState == TxEntry::TX_ABORT) {
                txEntry->setTxState(TxEntry::TX_ABORT);
                txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
                validator->getCounters().alertAborts++;
                txEntry->setTxResult(TxEntry::TX_ABORT_PEER);
            } else if (peerEntry->peerAlertSet == txEntry->getPeerSet()) {
                txEntry->setTxState(TxEntry::TX_ABORT);
                txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
                validator->getCounters().alertAborts++;
                txEntry->setTxResult(TxEntry::TX_ABORT_ALERT);
            } else if (txEntry->isExclusionViolated()) {
                txEntry->setTxState(TxEntry::TX_ABORT);
                txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
                txEntry->setTxResult(TxEntry::TX_ABORT_PISI);
            }
        } else if (txEntry->getTxState() == TxEntry::TX_PENDING) {
            if (txEntry->isExclusionViolated()) {
                txEntry->setTxState(TxEntry::TX_ABORT);
                txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
                txEntry->setTxResult(TxEntry::TX_ABORT_PISI_INIT);
            } else if (txEntry->getPeerSet() == peerEntry->peerSeenSet
                    && !peerEntry->peerAlertSet) {
                txEntry->setTxState(TxEntry::TX_COMMIT);
                txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
                txEntry->setTxResult(TxEntry::TX_COMMIT_INIT);
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
            validator->conclude(txEntry);
        } else
            abort();
    }

    if ((txEntry->getTxState() == TxEntry::TX_ABORT && peerEntry->peerTxState == TxEntry::TX_COMMIT) ||
            (txEntry->getTxState() == TxEntry::TX_COMMIT && peerEntry->peerTxState == TxEntry::TX_OUTOFORDER) ||
            (txEntry->getTxState() == TxEntry::TX_COMMIT && peerEntry->peerTxState == TxEntry::TX_ABORT)) {
        abort(); //there must be a design problem -- debug
        txEntry->setTxState(TxEntry::TX_CONFLICT);
        txEntry->setTxResult(TxEntry::TX_ABORT_LATE);
    }
    RAMCLOUD_LOG(NOTICE, "evaluate cts %lu  states %u %u %u %lu %lu %lu",
            (uint64_t)(txEntry->getCTS() >> 64), txEntry->getTxState(), txEntry->getTxCIState(), peerEntry->peerTxState,
            txEntry->getPeerSet(), peerEntry->peerSeenSet, peerEntry->peerAlertSet);
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

bool
PeerInfo::update(CTS cts, uint64_t peerId, uint32_t peerTxState, uint64_t pstamp, uint64_t sstamp, uint8_t peerPosition,
        uint32_t &myTxState, uint64_t &myPStamp, uint64_t &mySStamp, uint8_t &myPeerPosition, Validator *validator) {
    TxEntry *txEntry= NULL;
    PeerInfoIterator it;

    //Make sure that the peerInfo by CTS is valid
    it = peerInfo.find(cts);
    if (it != peerInfo.end()) {
        PeerEntry* entry = &peerEntryTable[it->second];
        entry->mutexForPeerUpdate.lock();
        if (entry->cts != cts) {
            RAMCLOUD_LOG(ERROR, "updatePeer mismatch cts %lu old cts %lu idx %u",
                    (uint64_t)(cts >> 64), (uint64_t)(entry->cts >> 64), it->second);
            return false;
        }
        if (!entry->isConcluded) {
            entry->meta.pStamp = std::max(entry->meta.pStamp, pstamp);
            entry->meta.sStamp = std::min(entry->meta.sStamp, sstamp);
            entry->peerSeenSet |= (1 << peerPosition);
            if (peerTxState == TxEntry::TX_ALERT) {
                entry->peerTxState = TxEntry::TX_ALERT;
                entry->peerAlertSet |= (1 << peerPosition);
            } else if (peerTxState == TxEntry::TX_ABORT || peerTxState == TxEntry::TX_OUTOFORDER) {
                assert(entry->peerTxState != TxEntry::TX_COMMIT);
                entry->peerTxState = TxEntry::TX_ABORT;
            } else if (peerTxState == TxEntry::TX_COMMIT) {
                assert(entry->peerTxState != TxEntry::TX_ABORT);
                entry->peerTxState = TxEntry::TX_COMMIT;
            }

            txEntry = entry->txEntry;

            RAMCLOUD_LOG(NOTICE, "updatePeer %lu txEntry %lu peerState %u peerId %lu cnt %lu %lu",
                    (uint64_t)(cts >> 64),
                    (uint64_t)txEntry, entry->peerTxState,
                    peerId, entry->peerSeenSet, entry->peerAlertSet);

            if (txEntry != NULL) {
                if (txEntry->getCTS() != cts) abort(); //make sure txEntry contents not corrupted
                txEntry->setPStamp(std::max(txEntry->getPStamp(), entry->meta.pStamp));
                txEntry->setSStamp(std::min(txEntry->getSStamp(), entry->meta.sStamp));

                evaluate(entry, txEntry, validator);

                myTxState = txEntry->getTxState();
                myPStamp = txEntry->getPStamp();
                mySStamp = txEntry->getSStamp();
                myPeerPosition = txEntry->getPeerPosition();
            }
            if (myTxState == 0) {
            /*
                for (uint32_t i = 0; i < TBLSZ; i++) {
                    if (this->peerEntryTable[i].txEntry *&& (this->peerEntry[i].txEntry->getTxState() & 2) == 0*)
                        RAMCLOUD_LOG(NOTICE, "table %u: cts %lu valid %u state %u", i,
                                (uint64_t)(this->peerEntryTable[i].cts>>64),
                                this->peerEntryTable[i].isValid,
                                this->peerEntryTable[i].txEntry ? this->peerEntryTable[i].txEntry->getTxState() : 0);
                }
                */
            }
        } else {
            RAMCLOUD_LOG(NOTICE, "updatePeer ignored: finished cts %lu %lu peerId %lu", (uint64_t)(cts >> 64),
                    (uint64_t)(cts & (((__uint128_t)1<<64) -1)), peerId);
        }
        entry->mutexForPeerUpdate.unlock();
        return true;
    }
    RAMCLOUD_LOG(NOTICE, "updatePeer ignored: no cts %lu peerId %lu", (uint64_t)(cts >> 64), peerId);
    return false;
}

bool
PeerInfo::send(PeerEntry *peerEntry, Validator *validator) {

    TxEntry* txEntry = peerEntry->txEntry;

    if (txEntry->getTxCIState() == TxEntry::TX_CI_SCHEDULED) {
        logAndSend(txEntry, validator);
        evaluate(peerEntry, txEntry, validator);
    }

    if (txEntry->getTxCIState() == TxEntry::TX_CI_QUEUED
            && txEntry->getTxState() == TxEntry::TX_OUTOFORDER) {
        //trigger conclusion, but keep state because this CI has never been added to activeTxSet
        txEntry->setSStamp(0);
        validator->sendSSNInfo(txEntry); //so that peers do not wait for alert
        txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
        evaluate(peerEntry, txEntry, validator);
    }

    return true;
}

bool
PeerInfo::monitor(Validator *validator) {
    uint64_t nsTime = validator->getClockValue();
    uint64_t currentTick = nsTime / tickUnit;
    PeerEntry *peerEntry;
    uint32_t count = TBLSZ;

    if (currentTick <= lastTick)
        return true;

    while (count > 0) {
        count--;
        peerEntry = &peerEntryTable[count];
        if (peerEntry->isConcluded) {
            continue;
        }

        peerEntry->mutexForPeerUpdate.lock();
        TxEntry* txEntry = peerEntry->txEntry;
        if (txEntry == NULL) {
            peerEntry->mutexForPeerUpdate.unlock();
            continue;
        }

        if ((nsTime - (txEntry->getCTS() >> 64)) > 1000000000) {
            /*
            RAMCLOUD_LOG(NOTICE, "finding: cts %lu states %u %u %u %lu %lu now %lu",
                (uint64_t)(txEntry->getCTS() >> 64), txEntry->getTxState(),
                txEntry->getTxCIState(), peerEntry->peerTxState,
                peerEntry->peerSeenSet, peerEntry->peerAlertSet, nsTime);

            for (uint32_t i = 0; i < TBLSZ; i++) {
                if (this->peerEntryTable[i].txEntry && (this->peerEntry[i].txEntry->getTxState() & 2) == 0)
                    RAMCLOUD_LOG(NOTICE, "table %u: cts %lu state %u %u", i,
                            (uint64_t)(this->peerEntryTable[i].cts>>64),
                            this->peerEntryTable[i].isConcluded,
                            this->peerEntryTable[i].txEntry ? this->peerEntryTable[i].txEntry->getTxState() : 0);
            }*/
        }

        if (txEntry->getTxCIState() == TxEntry::TX_CI_LISTENING) {
            if (txEntry->getTxState() != TxEntry::TX_ALERT
                && nsTime > (uint64_t)(txEntry->getCTS() >> 64)
                && nsTime - (uint64_t)(txEntry->getCTS() >> 64) > alertThreshold) {
                txEntry->setTxState(TxEntry::TX_ALERT);
                RAMCLOUD_LOG(NOTICE, "Timeout: cts %lu states %u %u %u now %lu",
                        (uint64_t)(txEntry->getCTS() >> 64), txEntry->getTxState(),
                        txEntry->getTxCIState(), peerEntry->peerTxState, nsTime);
            }

            if (txEntry->getTxState() == TxEntry::TX_ALERT) {
                //request missing SSN info from peers periodically;
                validator->requestSSNInfo(txEntry, false, NULL);
            }
        }
        peerEntry->mutexForPeerUpdate.unlock();
    }
    lastTick = currentTick;

    //std::this_thread::sleep_for(std::chrono::microseconds(1000000));

    return true;
}

uint32_t
PeerInfo::size() {
    return peerInfo.size();
}

} // end PeerInfo class

