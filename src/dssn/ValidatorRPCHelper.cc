/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "ValidatorRPCHelper.h"

namespace DSSN {

const uint64_t maxTimeStamp = std::numeric_limits<uint64_t>::max();
const uint64_t minTimeStamp = 0;

Status
ValidatorRPCHelper::rejectOperation(const RejectRules* rejectRules, uint64_t version)
{
	if (version == VERSION_NONEXISTENT) {
		if (rejectRules->doesntExist)
			return RAMCloud::STATUS_OBJECT_DOESNT_EXIST;
		return RAMCloud::STATUS_OK;
	}
	if (rejectRules->exists)
		return RAMCloud::STATUS_OBJECT_EXISTS;
	if (rejectRules->versionLeGiven && version <= rejectRules->givenVersion)
		return RAMCloud::STATUS_WRONG_VERSION;
	if (rejectRules->versionNeGiven && version != rejectRules->givenVersion)
		return RAMCloud::STATUS_WRONG_VERSION;
	return RAMCloud::STATUS_OK;
}

Status
ValidatorRPCHelper::readObject(uint64_t tableId, Key& key, Buffer* outBuffer,
        RejectRules* rejectRules, uint64_t* outVersion, uint64_t& cStamp, uint64_t& sStamp)
{
    Buffer buffer;
    uint64_t version = 1; //fixed for now. it is supposed to be retrieved from KV store

    KLayout k(key.getStringKeyLength() + sizeof(tableId)); //make room composite key in KVStore
    std::memcpy(k.key.get(), &tableId, sizeof(tableId));
    std::memcpy(k.key.get() + sizeof(tableId), key.getStringKey(), key.getStringKeyLength());
    KVLayout *kv;
    bool found = validator.read(k, kv);
    if (!found)
        return RAMCloud::STATUS_OBJECT_DOESNT_EXIST;

    uint8_t* p = static_cast<uint8_t*>(buffer.alloc(kv->getVLayout()->valueLength));
    std::memcpy(p, kv->getVLayout()->valuePtr, kv->getVLayout()->valueLength);
    cStamp = kv->getMeta().cStamp;
    sStamp = kv->getMeta().sStamp;

    if (outVersion != NULL)
        *outVersion = version;

    if (rejectRules != NULL) {
        Status status = rejectOperation(rejectRules, version);
        if (status != RAMCloud::STATUS_OK)
            return status;
    }

    Object object(buffer);
    object.appendValueToBuffer(outBuffer);

    return RAMCloud::STATUS_OK;
}

Status
ValidatorRPCHelper::writeObject(Object& newObject, RejectRules* rejectRules,
                uint64_t* outVersion, Buffer* removedObjBuffer, uint64_t& pStampPrev)
{
    uint16_t keyLength = 0;
    const void *keyString = newObject.getKey(0, &keyLength);

    uint64_t currentVersion = VERSION_NONEXISTENT;

    uint64_t tableId = newObject.getTableId();
    KLayout k(keyLength + sizeof(tableId)); //make room composite key in KVStore
    std::memcpy(k.key.get(), &tableId, sizeof(tableId));
    std::memcpy(k.key.get() + sizeof(tableId), keyString, keyLength);
    KVLayout *kv;
    bool found = validator.read(k, kv);
    if (found) {
    	currentVersion = kv->getMeta().cStamp;
    	pStampPrev = kv->getMeta().pStampPrev;
    }

    if (rejectRules != NULL) {
        Status status = rejectOperation(rejectRules, currentVersion);
        if (status != RAMCloud::STATUS_OK) {
            if (outVersion != NULL)
                *outVersion = currentVersion;
            return status;
        }
    }

    if (outVersion != NULL)
        *outVersion = currentVersion;

    return RAMCloud::STATUS_OK;
}

Status
ValidatorRPCHelper::updatePeerInfo(uint64_t cts, uint64_t peerId, uint64_t eta, uint64_t pi) {
	TxEntry *txEntry = NULL;
	if (validator.updatePeerInfo(cts, peerId, eta, pi, txEntry)) {
		if (txEntry->isExclusionViolated())
			txEntry->setTxState(TxEntry::TX_ABORT);
		else if (txEntry->isAllPeerSeen() && !txEntry->isExclusionViolated())
			txEntry->setTxState(TxEntry::TX_COMMIT);
		else
			return RAMCloud::STATUS_OK; //inconclusive yet

		//Fixme: should WAL here so that peers requesting info of this txEntry
		// can be serviced by PeerInfo-referenced txEntry or WAL.
		validator.insertConcludeQueue(txEntry);
		txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED); //allow this to be swept

        //reply to commit intent client
	}
	return RAMCloud::STATUS_OK;
}


} // end ValidatorRPCHelper class

