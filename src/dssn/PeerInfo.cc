/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include <algorithm>
#include "PeerInfo.h"

namespace DSSN {

bool
PeerInfo::add(CTS cts, TxEntry *txEntry, Validator *validator) {
    std::lock_guard<std::mutex> lock(mutexForPeerAdd); //Fixme: two mutex -- deadlock???
    PeerInfoIterator it = peerInfo.find(cts);
    if (it == peerInfo.end()) {
        PeerEntry* entry = new PeerEntry();
        entry->txEntry = txEntry;
        entry->meta.cStamp = cts;
        if (txEntry != NULL) {
            assert(entry->meta.cStamp = txEntry->getCTS());
            entry->meta.pStamp = txEntry->getPStamp();
            entry->meta.sStamp = txEntry->getSStamp();
        }
        peerInfo.insert(std::make_pair(cts, entry));
    } else {
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
        return it->second->txEntry;
    }
    return NULL;
}

TxEntry*
PeerInfo::getNext(PeerInfoIterator &it) {
    it++;
    if (it != peerInfo.end()) {
        return it->second->txEntry;
    }
    return NULL;
}

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
}

bool
PeerInfo::evaluate(PeerEntry *peerEntry, uint8_t peerTxState, TxEntry *txEntry, Validator *validator) {
    if (txEntry->isExclusionViolated()) {
        txEntry->setTxState(TxEntry::TX_ABORT);
        if (peerTxState == TxEntry::TX_COMMIT) {
            txEntry->setTxState(TxEntry::TX_CONFLICT);
            assert(0); //Fixme: recover or debug
        }
        txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
    } else if (txEntry->getPeerSet() == peerEntry->peerSeenSet && !txEntry->isExclusionViolated()) {
        txEntry->setTxState(TxEntry::TX_COMMIT);
        if (peerTxState == TxEntry::TX_ABORT) {
            txEntry->setTxState(TxEntry::TX_CONFLICT);
            assert(0); //Fixme: recover or debug
        }
        txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
    } else
        return false; //inconclusive yet

    //Logging must precede sending tx CI reply
    //Note that our overall scheme does not need to log local transactions at all
    //By now the tuples should have successfully been preput into the KV store, so
    //logging the CI conclusion is considered sealing a tx commit.
    if (txEntry->getTxCIState() == TxEntry::TX_CI_CONCLUDED
            && true /* Fixme: txLog.add(txEntry) */) {
        txEntry->setTxCIState(TxEntry::TX_CI_SEALED);
        assert(validator->insertConcludeQueue(txEntry));
        validator->sendTxCommitReply(txEntry);
    }
    return true; //concluded
}

bool
PeerInfo::update(CTS cts, uint64_t peerId, uint8_t peerTxState, uint64_t pstamp, uint64_t sstamp, TxEntry *&txEntry, Validator *validator) {
    PeerInfoIterator it;
    it = peerInfo.find(cts);
    if (it != peerInfo.end()) {
        std::lock_guard<std::mutex> lock(it->second->mutexForPeerUpdate);
        PeerEntry* entry = it->second;
        entry->meta.pStamp = std::max(entry->meta.pStamp, pstamp);
        entry->meta.sStamp = std::max(entry->meta.sStamp, sstamp);
        entry->peerSeenSet.insert(peerId);
        txEntry = it->second->txEntry;
        if (txEntry != NULL) {
            txEntry->setPStamp(std::max(txEntry->getPStamp(), entry->meta.pStamp));
            txEntry->setSStamp(std::min(txEntry->getSStamp(), entry->meta.sStamp));
            evaluate(entry, peerTxState, txEntry, validator);
        }
        return true;
    }
    return false;
}

uint32_t
PeerInfo::size() {
    return peerInfo.size();
}

} // end PeerInfo class

