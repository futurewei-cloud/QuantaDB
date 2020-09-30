/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include <algorithm>
#include "PeerInfo.h"
#include "Logger.h"

namespace DSSN {

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
        PeerEntry* entry = new PeerEntry();
        entry->isValid = true;
        entry->txEntry = txEntry;
        entry->meta.cStamp = cts >> 64;
        if (txEntry != NULL) {
            assert(entry->meta.cStamp == txEntry->getCTS() >> 64);
            entry->meta.pStamp = txEntry->getPStamp();
            entry->meta.sStamp = txEntry->getSStamp();
        }
        assert(peerInfo.insert(std::make_pair(cts, entry)).second);
        validator->getCounters().addPeers++;
    } else if (txEntry != NULL) {
        PeerEntry* existing = it->second;
        existing->mutexForPeerUpdate.lock();
        existing->txEntry = txEntry;
        txEntry->setPStamp(std::max(txEntry->getPStamp(), existing->meta.pStamp));
        txEntry->setSStamp(std::min(txEntry->getSStamp(), existing->meta.sStamp));
        evaluate(existing, txEntry, validator);
        validator->getCounters().matchEarlyPeers++;
        existing->mutexForPeerUpdate.unlock();
    }
    assert(peerInfo.find(cts) != peerInfo.end());
    mutexForPeerAdd.unlock();
    return true;
}

TxEntry*
PeerInfo::getFirst(PeerInfoIterator &it) {
    it = peerInfo.begin();
    if (it != peerInfo.end()) {
        return it->second->txEntry; //this may or may not be NULL
    }
    return NULL;
}

TxEntry*
PeerInfo::getNext(PeerInfoIterator &it) {
    it++;
    if (it != peerInfo.end()) {
        return it->second->txEntry; //this may or may not be NULL
    }
    return NULL;
}

bool
PeerInfo::sweep(Validator *validator) {
    /*
    //delete peerInfo entry no longer needed
    ///further reference to the swept tx should be using CTS into the tx log
    uint32_t cnt = 0;
    auto itr = peerInfo.begin();
    while (itr != peerInfo.end()) {
        cnt++;
        PeerEntry *peerEntry = (*itr).second;
        if (!peerEntry->isValid && peerEntry->txEntry && peerEntry->txEntry->getTxCIState() >= TxEntry::TX_CI_SEALED) {
            itr = peerInfo.unsafe_erase(itr);
            delete peerEntry;
            validator->getCounters().deletedPeers++;
        } else
            itr++;
    }*/
    return true;
}

inline bool
PeerInfo::evaluate(PeerEntry *peerEntry, TxEntry *txEntry, Validator *validator) {
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);

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
    if (txEntry->getTxCIState() == TxEntry::TX_CI_CONCLUDED
            && validator->logTx(LOG_ALWAYS, txEntry)) {
        txEntry->setTxCIState(TxEntry::TX_CI_SEALED);
        peerEntry->isValid = false;
        assert(validator->insertConcludeQueue(txEntry));
    }

    if ((txEntry->getTxState() == TxEntry::TX_ABORT && peerEntry->peerTxState == TxEntry::TX_COMMIT) ||
            (txEntry->getTxState() == TxEntry::TX_COMMIT && peerEntry->peerTxState == TxEntry::TX_LATE) ||
            (txEntry->getTxState() == TxEntry::TX_COMMIT && peerEntry->peerTxState == TxEntry::TX_ABORT)) {
        abort(); //there must be a design problem -- debug
        txEntry->setTxState(TxEntry::TX_CONFLICT);
    }

    char buffer[512];
    sprintf(buffer, "%lu %lu my %lu %lu peer %lu", validator->getCounters().serverId, (uint64_t)(txEntry->getCTS() >> 64), txEntry->getTxState(), txEntry->getTxCIState(), peerEntry->peerTxState);
    RAMCLOUD_LOG(NOTICE, "%s", buffer);

    return true; //concluded
}

bool
PeerInfo::update(CTS cts, uint64_t peerId, uint8_t peerTxState, uint64_t pstamp, uint64_t sstamp, TxEntry *&txEntry, Validator *validator) {
    PeerInfoIterator it;
    it = peerInfo.find(cts);
    if (it != peerInfo.end()) {
        PeerEntry* peerEntry = it->second;
        assert(peerEntry != NULL);
        peerEntry->mutexForPeerUpdate.lock();
        if (!peerEntry->isValid) {
            txEntry = NULL;
            peerEntry->mutexForPeerUpdate.unlock();
            return true;
        }

        peerEntry->meta.pStamp = std::max(peerEntry->meta.pStamp, pstamp);
        peerEntry->meta.sStamp = std::min(peerEntry->meta.sStamp, sstamp);
        peerEntry->peerSeenSet.insert(peerId);
        if (peerTxState == TxEntry::TX_ALERT) {
            peerEntry->peerTxState = TxEntry::TX_ALERT;
            peerEntry->peerAlertSet.insert(peerId);
        } else if (peerTxState == TxEntry::TX_ABORT || peerTxState == TxEntry::TX_LATE) {
            assert(peerEntry->peerTxState != TxEntry::TX_COMMIT);
            peerEntry->peerTxState = TxEntry::TX_ABORT;
        } else if (peerTxState == TxEntry::TX_COMMIT) {
            assert(peerEntry->peerTxState != TxEntry::TX_ABORT);
            peerEntry->peerTxState = TxEntry::TX_COMMIT;
        }
        txEntry = it->second->txEntry;
        if (txEntry != NULL) {
            txEntry->setPStamp(std::max(txEntry->getPStamp(), peerEntry->meta.pStamp));
            txEntry->setSStamp(std::min(txEntry->getSStamp(), peerEntry->meta.sStamp));
            evaluate(peerEntry, txEntry, validator);
        }

        peerEntry->mutexForPeerUpdate.unlock();

        return true;
    }
    return false;
}

bool
PeerInfo::send(Validator *validator) {
    uint64_t nsTime = validator->getClockValue();
    uint64_t currentTick = nsTime / tickUnit;

    auto itr = peerInfo.begin();
    while (itr != peerInfo.end()) {
        PeerEntry *peerEntry = (*itr).second;
        TxEntry* txEntry = peerEntry->txEntry;
        itr++;

        peerEntry->mutexForPeerUpdate.lock();

        if (!peerEntry->isValid || txEntry == NULL) {
            peerEntry->mutexForPeerUpdate.unlock();
            continue;
        }

        if (txEntry->getTxCIState() == TxEntry::TX_CI_SCHEDULED) {
            validator->updateTxPStampSStamp(*txEntry);

            //log CI before sending
            if (validator->logTx(LOG_ALWAYS, txEntry)) {
                assert(txEntry->getTxState() == TxEntry::TX_PENDING);
                validator->sendSSNInfo(txEntry);
                txEntry->setTxCIState(TxEntry::TX_CI_LISTENING);
                evaluate(peerEntry, txEntry, validator);
            }
        }

        if (txEntry->getTxCIState() == TxEntry::TX_CI_LISTENING) {
            if (txEntry->getTxState() != TxEntry::TX_ALERT
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

        peerEntry->mutexForPeerUpdate.unlock();
    }
    lastTick = currentTick;

    return true;
}

uint32_t
PeerInfo::size() {
    return peerInfo.size();
}

} // end PeerInfo class

