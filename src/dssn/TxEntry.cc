/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "TxEntry.h"
#include "HOTKV.h"
#include "KVInterface.h"

namespace DSSN {

/*
template <typename T>
static inline T volatile_read(T volatile &x) {
  return *&x;
}*/

inline static std::string formTupleKey(Object& tuple) {
	KeyLength* kLen;
	uint8_t* key = tuple.getKey(0, kLen);
	if (key == NULL) // there is a bug if it happens
		return std::numeric_limits<uint64_t>::max();
	uint64_t tableId = tuple.getTableId();
	std::vector<uint8_t> ckey(sizeof(uint64_t) + *kLen);
	*(uint64_t *)ckey = tableId;
	for (uint32_t i = 0; i < *kLen; i++)
		ckey[sizeof(uint64_t) + i] = key[i];
	return std::string(ckey.begin(), ckey.end());
}

static uint64_t
TxEntry::getTuplePi(Object& object) {
	dssnMeta meta;
	TxEntry::tupleStore.getMeta(formTupleKey(object), meta);
	return meta.cStamp;
}

static uint64_t
TxEntry::getTuplePrevEta(Object& object) {
	dssnMeta meta;
	TxEntry::tupleStore.getMeta(formTupleKey(object), meta);
	return meta.sStampPrev;
}

static uint64_t
TxEntry::getTuplePrevPi(Object& object) {
	return 0; //not used yet
}

static bool
TxEntry::maximizeTupleEta(Object& object, uint64_t eta) {
	dssnMeta meta;
	std::string key = formTupleKey(object);
	TxEntry::tupleStore.getMeta(key, meta);
	meta.pStamp = std::max(eta, meta.pStamp);
	TxEntry::tupleStore.updateMeta(key, meta);
	return true;
}

static bool
TxEntry::setTupleValue(Object& object) {
	return true;
}

TxEntry::TxEntry() {
	this->pi = std::numeric_limits<uint64_t>::max();
	this->eta = 0;
	this->txState = TX_PENDING;
	this->commitIntentState = TX_CI_UNQUEUED;
	this->cts = 0;
}

bool
TxEntry::updateTxEtaPi() {
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
		uint64_t vPi = TxEntry::getTuplePi(*readSet.at(i));
		this->pi = std::min(this->pi, vPi);
		if (this->isExclusionViolated()) {
			this->txState = TX_ABORT;
			return false;
		}
	}

	auto  &writeSet = this->writeSet;
	for (uint32_t i = 0; i < readSet.size(); i++) {
		uint64_t vPrevEta = TxEntry::getTuplePrevEta(*writeSet.at(i));
		this->eta = std::max(this->eta, vPrevEta);
		if (this->isExclusionViolated()) {
			this->txState = TX_ABORT;
			return false;
		}
	}

	return true;
}

bool
TxEntry::updateReadsetSSNData() {
	auto &readSet = this->readSet;
	for (uint32_t i = 0; i < readSet.size(); i++) {
		TxEntry::maximizeTupleEta(*readSet.at(i), this->cts);
	}
	return true;
}

bool
TxEntry::updateWritesetSSNData() {
	auto &writeSet = this->writeSet;
	for (uint32_t i = 0; i < writeSet.size(); i++) {
		dssnMeta meta;
		std::string key = formTupleKey(*writeSet.at(i));
		TxEntry::tupleStore.getMeta(key, meta);

		/* because we have a single version HOT, copy current data to prev data */
		meta.pStampPrev = meta.pStamp;

		/* SSN required operations */
		meta.sStampPrev = this->pi;
		meta.cStamp = meta.pStamp = this->cts;
		meta.sStamp = std::numeric_limits<uint64_t>::max();

		TxEntry::tupleStore.updateMeta(key, meta);
	}
	return true;
}

bool
TxEntry::validate() {
	//calculate local eta and pi
	updateTxEtaPi();

	//update commit intent state
	commitIntentState = TX_CI_INPROGRESS;

	if (getShardSet().size() > 1) { // cross-shard transaction
		//send out eta and pi
		//wait and check for all peers
		//if timed-out, loop to recover
	}

	//check exclusion
	//if failed, update states;
	//if passed, log and commit and update states.
	if (isExclusionViolated()) {
		txState = TX_ABORT;
	} else {
		/*
		 * Validation is passed. Record decision in this sequence:
		 * WAL, update state, update tuple store (in-mem and then pmem).
		 */

		// WAL - write-ahead log, for failure recovery

		// update state
		txState = TX_COMMIT;
		updateReadsetSSNData();

		// update in-mem tuple store
		updateWritesetSSNData();

	}

	return true;
};

} // end TxEntry

