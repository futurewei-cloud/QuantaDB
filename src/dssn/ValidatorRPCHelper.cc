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
        RejectRules* rejectRules, uint64_t* outVersion,
        bool valueOnly)
{
    Buffer buffer;
    uint64_t version = 0;

    KLayout k(key.keyLength + sizeof(tableId)); //make room composite key in KVStore
    std::memcpy(k.key.get(), &tableId, sizeof(tableId));
    std::memcpy(k.key.get() + sizeof(tableId), key.key, key.keyLength);
    KVLayout *kv;
    uint8_t *valuePtr;
    bool found = validator.read(k, kv, *&valuePtr);
    if (!found)
        return RAMCloud::STATUS_OBJECT_DOESNT_EXIST;

    //bool found = lookup(lock, key, type, buffer, &version, &reference);
    //get the buffer right with kv and valuePtr

    if (outVersion != NULL)
        *outVersion = version;

    if (rejectRules != NULL) {
        Status status = rejectOperation(rejectRules, version);
        if (status != RAMCloud::STATUS_OK)
            return status;
    }

    Object object(buffer);
    if (valueOnly) {
        object.appendValueToBuffer(outBuffer);
    } else {
        object.appendKeysAndValueToBuffer(*outBuffer);
    }
    uint32_t valueLength = object.getValueLength();

    return RAMCloud::STATUS_OK;
}

} // end ValidatorRPCHandler class

