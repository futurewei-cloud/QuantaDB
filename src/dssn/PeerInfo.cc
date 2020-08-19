/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include <algorithm>
#include "PeerInfo.h"

namespace DSSN {

bool
PeerInfo::add(CTS cts, TxEntry *txEntry, Validator *validator) {
    std::lock_guard<std::mutex> lock(mutexForPeerAdd);
    PeerInfoIterator it = peerInfo.find(cts);
    if (it == peerInfo.end()) {
        PeerEntry* entry = new PeerEntry();
        entry->txEntry = txEntry;
        entry->meta.cStamp = cts >> 64;
        if (txEntry != NULL) {
            assert(entry->meta.cStamp == txEntry->getCTS() >> 64);
            entry->meta.pStamp = txEntry->getPStamp();
            entry->meta.sStamp = txEntry->getSStamp();
        }
        peerInfo.insert(std::make_pair(cts, entry));
    } else if (txEntry != NULL) {
        PeerEntry* existing = it->second;
        std::lock_guard<std::mutex> lock(existing->mutexForPeerUpdate);
        existing->txEntry = txEntry;
        txEntry->setPStamp(std::max(txEntry->getPStamp(), existing->meta.pStamp));
        txEntry->setSStamp(std::min(txEntry->getSStamp(), existing->meta.sStamp));
        evaluate(it->second, TxEntry::TX_PENDING, txEntry, validator);
    }
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
/*
bool
PeerInfo::sweep() {
    //delete peerInfo entry no longer needed
    ///further reference to the swept tx should be using CTS into the tx log
    PeerEntry *prev = NULL;
    std::for_each(peerInfo.begin(), peerInfo.end(), [&] (const std::pair<CTS, PeerEntry *>& pr) {
        PeerEntry *peerEntry = pr.second;
        if (peerEntry->txEntry && peerEntry->txEntry->getTxCIState() >= TxEntry::TX_CI_SEALED) {
            if (prev) {
                peerInfo.unsafe_erase(prev->meta.cStamp);
                delete prev;
            }
            prev = peerEntry;
        }
    });
    if (prev) {
        peerInfo.unsafe_erase(prev->meta.cStamp);
        delete prev;
    }
    return true;
}*/

bool
PeerInfo::sweep() {
    //delete peerInfo entry no longer needed
    ///further reference to the swept tx should be using CTS into the tx log
    auto itr = peerInfo.begin();
    while (itr != peerInfo.end()) {
        PeerEntry *peerEntry = (*itr).second;
        if (peerEntry->txEntry && peerEntry->txEntry->getTxCIState() >= TxEntry::TX_CI_SEALED) {
            itr = peerInfo.unsafe_erase(itr);
            delete peerEntry;
        } else
            itr++;
    }
    return true;
}

bool
PeerInfo::evaluate(PeerEntry *peerEntry, uint8_t peerTxState, TxEntry *txEntry, Validator *validator) {

    if (txEntry->getTxCIState() < TxEntry::TX_CI_CONCLUDED) {
        if (txEntry->isExclusionViolated()) {
            txEntry->setTxState(TxEntry::TX_ABORT);
            txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
        } else if (txEntry->getTxState() == TxEntry::TX_ALERT
                && peerTxState == TxEntry::TX_COMMIT) {
            txEntry->setTxState(TxEntry::TX_COMMIT);
            txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
        } else if (txEntry->getTxState() == TxEntry::TX_ALERT
                && (peerTxState == TxEntry::TX_ABORT
                        || peerEntry->peerAlertSet == txEntry->getPeerSet())) {
            txEntry->setTxState(TxEntry::TX_ABORT);
            txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
            validator->getCounters().alertAborts++;
        } else if (txEntry->getPeerSet() == peerEntry->peerSeenSet
                && peerEntry->peerAlertSet.empty()) {
            txEntry->setTxState(TxEntry::TX_COMMIT);
            txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
        } else {
            return false; //inconclusive yet
        }
    }

    //Logging must precede sending tx CI reply
    //Note that our overall scheme does not need to log local transactions at all
    //By now the tuples should have successfully been preput into the KV store, so
    //logging the CI conclusion is considered sealing a tx commit.
    if (txEntry->getTxCIState() == TxEntry::TX_CI_CONCLUDED
            && validator->logTx(LOG_ALWAYS, txEntry)) {
        txEntry->setTxCIState(TxEntry::TX_CI_SEALED);
        assert(validator->insertConcludeQueue(txEntry));
        validator->sendTxCommitReply(txEntry);
    }

    if ((txEntry->getTxState() == TxEntry::TX_ABORT && peerTxState == TxEntry::TX_COMMIT) ||
            (txEntry->getTxState() == TxEntry::TX_COMMIT && peerTxState == TxEntry::TX_ABORT)) {
        txEntry->setTxState(TxEntry::TX_CONFLICT);
        assert(0); //there must be a design problem -- debug
    }

    return true; //concluded
}

bool
PeerInfo::update(CTS cts, uint64_t peerId, uint8_t peerTxState, uint64_t pstamp, uint64_t sstamp, TxEntry *&txEntry, Validator *validator) {
    PeerInfoIterator it;
    it = peerInfo.find(cts);
    if (it != peerInfo.end()) {
        std::lock_guard<std::mutex> lock(it->second->mutexForPeerUpdate);
        PeerEntry* peerEntry = it->second;
        assert(peerEntry != NULL);
        peerEntry->meta.pStamp = std::max(peerEntry->meta.pStamp, pstamp);
        peerEntry->meta.sStamp = std::min(peerEntry->meta.sStamp, sstamp);
        peerEntry->peerSeenSet.insert(peerId);
        if (peerTxState == TxEntry::TX_ALERT)
            peerEntry->peerAlertSet.insert(peerId);
        else
            peerEntry->peerAlertSet.erase(peerId);
        txEntry = it->second->txEntry;
        if (txEntry != NULL) {
            txEntry->setPStamp(std::max(txEntry->getPStamp(), peerEntry->meta.pStamp));
            txEntry->setSStamp(std::min(txEntry->getSStamp(), peerEntry->meta.sStamp));
            evaluate(peerEntry, peerTxState, txEntry, validator);
        }
        return true;
    }
    return false;
}

bool
PeerInfo::send(Validator *validator) {
    uint64_t nsTime = validator->getClockValue();
    uint64_t currentTick = nsTime / tickUnit;
    std::for_each(peerInfo.begin(), peerInfo.end(), [&] (const std::pair<CTS, PeerEntry *>& pr) {
        PeerEntry *peerEntry = pr.second;
        TxEntry* txEntry = pr.second->txEntry;

        if (txEntry == NULL)
            return;

        if (txEntry->getTxCIState() == TxEntry::TX_CI_SCHEDULED) {
            std::lock_guard<std::mutex> lock(peerEntry->mutexForPeerUpdate);

            validator->updateTxPStampSStamp(*txEntry);

            //log CI before sending
            if (validator->logTx(LOG_ALWAYS, txEntry)) {
                assert(txEntry->getTxState() == TxEntry::TX_PENDING);
                validator->sendSSNInfo(txEntry);
                txEntry->setTxCIState(TxEntry::TX_CI_LISTENING);
            }
        }

        if (txEntry->getTxCIState() == TxEntry::TX_CI_LISTENING
                && txEntry->getTxState() != TxEntry::TX_ALERT
                && nsTime - (uint64_t)(txEntry->getCTS() >> 64) > alertThreshold) {
            std::lock_guard<std::mutex> lock(peerEntry->mutexForPeerUpdate);
            txEntry->setTxState(TxEntry::TX_ALERT);
        }

        if (txEntry->getTxState() == TxEntry::TX_ALERT) {
            //request missing SSN info from peers periodically;
            if (currentTick > lastTick) {
                std::set<uint64_t>::iterator it;
                for (it = txEntry->getPeerSet().begin(); it != txEntry->getPeerSet().end(); it++) {
                    if (peerEntry->peerSeenSet.find(*it) == peerEntry->peerSeenSet.end()) {
                        validator->requestSSNInfo(txEntry, *it);
                    }
                }
            }
        }
    });
    lastTick = currentTick;

    return true;
}

uint32_t
PeerInfo::size() {
    return peerInfo.size();
}

} // end PeerInfo class

