/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include <algorithm>
#include "PeerInfo.h"

namespace DSSN {

bool
PeerInfo::add(TxEntry *txEntry) {
    peerInfo.insert(std::make_pair(txEntry->getCTS(), txEntry));
    return true;
}

TxEntry*
PeerInfo::getFirst(PeerInfoIterator &it) {
    it = peerInfo.begin();
    if (it != peerInfo.end()) {
        return it->second;
    }
    return NULL;
}

TxEntry*
PeerInfo::getNext(PeerInfoIterator &it) {
    it++;
    if (it != peerInfo.end()) {
        return it->second;
    }
    return NULL;
}

bool
PeerInfo::sweep() {
    //sweep concluded peerInfo entry
    ///further reference to the swept tx should be using CTS into the tx log
    TxEntry *prev = NULL;
    std::for_each(peerInfo.begin(), peerInfo.end(), [&] (const std::pair<CTS, TxEntry *>& pr) {
        TxEntry *txEntry = pr.second;
        if (txEntry->getTxCIState() == TxEntry::TX_CI_CONCLUDED) {
            if (prev) {
                peerInfo.unsafe_erase(prev->getCTS());
            }
            prev = txEntry;
        }
    });
    if (prev) {
        peerInfo.unsafe_erase(prev->getCTS());
    }


    return true;
}

bool
PeerInfo::update(CTS cts, uint64_t peerId, uint64_t pstamp, uint64_t sstamp, TxEntry *&txEntry) {
    tbb::concurrent_unordered_map<CTS, TxEntry *>::iterator it;
    it = peerInfo.find(cts);
    if (it != peerInfo.end()) {
        txEntry = it->second;
        std::lock_guard<std::mutex> lock(txEntry->getMutex());
        txEntry->setPStamp(std::max(txEntry->getPStamp(), pstamp));
        txEntry->setSStamp(std::min(txEntry->getSStamp(), sstamp));
        txEntry->insertPeerSeenSet(peerId);
        return true;
    }
    return false;
}

uint32_t
PeerInfo::size() {
    return peerInfo.size();
}

} // end PeerInfo class

