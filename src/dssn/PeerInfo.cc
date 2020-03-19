/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "PeerInfo.h"

namespace DSSN {

bool
PeerInfo::add(TxEntry *txEntry) {
	tbb::concurrent_hash_map<CTS, TxEntry *, tbb::tbb_hash_compare<CTS>>::accessor ac;
	peerInfo.insert(ac, txEntry->getCTS());
	ac->second = txEntry;
	return true;
}

bool
PeerInfo::remove(TxEntry *txEntry) {
	return peerInfo.erase(txEntry->getCTS());
}

bool
PeerInfo::update(CTS cts, uint64_t peerId, uint64_t eta, uint64_t pi) {
	tbb::concurrent_hash_map<CTS, TxEntry *, tbb::tbb_hash_compare<CTS>>::accessor ac;
	if (peerInfo.find(ac, cts)) {
		TxEntry *txEntry = ac->second;
		txEntry->setEta(std::max(txEntry->getEta(), eta));
		txEntry->setPi(std::min(txEntry->getPi(), pi));
		txEntry->insertPeerSeenSet(peerId);
		return true;
	}
	return false;
}

} // end PeerInfo class

