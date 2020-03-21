/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "PeerInfo.h"

namespace DSSN {

bool
PeerInfo::add(TxEntry *txEntry) {
	peerInfo.insert(std::make_pair(txEntry->getCTS(), txEntry));
	return true;
}

bool
PeerInfo::sweep() {
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
PeerInfo::update(CTS cts, uint64_t peerId, uint64_t eta, uint64_t pi, TxEntry *&txEntry) {
	tbb::concurrent_unordered_map<CTS, TxEntry *>::iterator it;
	it = peerInfo.find(cts);
	if (it != peerInfo.end()) {
		txEntry = it->second;
		std::lock_guard<std::mutex> lock(txEntry->getMutex());
		txEntry->setEta(std::max(txEntry->getEta(), eta));
		txEntry->setPi(std::min(txEntry->getPi(), pi));
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

