/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "TxEntry.h"

namespace DSSN {

typedef RAMCloud::Object Object;
typedef RAMCloud::KeyLength KeyLength;

template <typename T>
static inline T volatile_read(T volatile &x) {
  return *&x;
}


bool
TxEntry::isExclusionViolated() {
	return (this->pi <= this->eta);
}

bool
TxEntry::updateEtaPi() {
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

	this->pi = std::min(this->pi, this->cts);

	auto &readSet = this->readSet;
	for (uint32_t i = 0; i < readSet.size(); i++) {
		auto &tuple = readSet[i];
		// get the key; use the key into the in-memory KV store to retrieve its pi
		// the key is a composite of key, table id, tenant id, etc.
		KeyLength kLen;
		auto key = tuple->getKey(0, &kLen);
		// ...
		uint64_t vPi = 1; //LATER get it from Object

		this->pi = std::min(this->pi, vPi);

		if (this->isExclusionViolated()) {
			this->txState = TX_ABORT;
			return false;
		}
	}

	auto  &writeSet = this->writeSet;
	for (uint32_t i = 0; i < readSet.size(); i++) {
		auto &tuple = writeSet[i];
		// get the key; use the key into the in-memory KV store to retrieve its pi
		KeyLength kLen;
		auto key = tuple->getKey(0, &kLen);
		// ...
		uint64_t vPrevEta = 1;

		this->eta = std::max(this->eta, vPrevEta);

		if (this->isExclusionViolated()) {
			this->txState = TX_ABORT;
			return false;
		}
	}

	return true;
}

bool
TxEntry::validate() {
	//calculate local eta and pi
	this->updateEtaPi();

	//update commit intent state
	this->commitIntentState = TX_CI_INPROGRESS;

	//if CS txn,
	//check exclusion; update state if needed
	//send out eta and pi
	//wait and check for all peers
	//if timed-out, loop to recover

	//check exclusion
	//if failed, update states;
	//if passed, log and commit and update states.



	const std::vector<uint64_t>& shardSet = this->getShardSet();
	if (shardSet.size() > 1) {
		;
	}
};

} // end TxEntry

