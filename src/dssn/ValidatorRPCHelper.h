/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef VALIDATOR_RPC_HELPER_H
#define VALIDATOR_RPC_HELPER_H

#include "Object.h"
#include "Buffer.h"
#include "Status.h"
#include "Validator.h"

namespace DSSN {
typedef RAMCloud::Object Object;
typedef RAMCloud::KeyLength KeyLength;
typedef RAMCloud::Key Key;
typedef RAMCloud::Buffer Buffer;
typedef RAMCloud::RejectRules RejectRules;
typedef RAMCloud::Status Status;

/**
 * This class covers all RPC handlers of the validator.
 *
 * Currently it uses some RAMCloud classes. In a way, it translates between
 * the RAMCloud namespace classes and DSSN namespace classes.
 *
 */
class ValidatorRPCHelper {
    PROTECTED:

    Validator& validator;

    Status
    rejectOperation(const RejectRules* rejectRules, uint64_t version);

    PUBLIC:
	ValidatorRPCHelper(Validator &validator) : validator(validator) {}

	/**
	 * Read an object.
	 *
	 * \param key
	 *      Key of the object being read.
	 * \param outBuffer
	 *      Buffer to populate with the value of the object, if found.
	 * \param rejectRules
	 *      If non-NULL, use the specified rules to perform a conditional read. See
	 *      the RejectRules class documentation for more details.
	 * \param outVersion
	 *      If non-NULL and the object is found, the version is returned here. If
	 *      the reject rules failed the read, the current object's version is still
	 *      returned.
	 * \param cStamp
	 *      Object DSSN commit time stamp
	 * \param sStamp
	 *      Object DSSN successor time stamp
	 * \return
	 *      Returns STATUS_OK if the lookup succeeded and the reject rules did not
	 *      preclude this read. Other status values indicate different failures
	 *      (object not found, tablet doesn't exist, reject rules applied, etc).
	 */
	Status
	readObject(uint64_t tableId, Key& key, Buffer* outBuffer,
	                RejectRules* rejectRules, uint64_t* outVersion,
	                uint64_t& cStamp, uint64_t& sStamp);


	/**
	 * Write an object. Because DSSN validator does not keep any pre-commit
	 * read set and write set, this routine simply returns the DSSN meta data
	 * to the caller.
	 *
	 * \param newObject
	 *      The new object to be written to the log. The object does not have
	 *      a valid version and timestamp. So this function will update the version,
	 *      timestamp and the checksum of the object before writing to the log.
	 * \param rejectRules
	 *      Specifies conditions under which the write should be aborted with an
	 *      error. May be NULL if no special reject conditions are desired.
	 *
	 * \param[out] outVersion
	 *      If non-NULL, the version number of the new object is returned here. If
	 *      the operation was successful this will be the new version for the
	 *      object; if this object has ever existed previously the new version is
	 *      guaranteed to be greater than any previous version of the object. If the
	 *      operation failed then the version number returned is the current version
	 *      of the object, or VERSION_NONEXISTENT if the object does not exist.
	 * \param[out] pStampPrev
	 *      Object DSSN previous predecessor time stamp
	 * \return
	 *      STATUS_OK if the object was written. Otherwise, for example,
	 *      STATUS_UKNOWN_TABLE may be returned.
	 */
	Status
	writeObject(Object& newObject, RejectRules* rejectRules,
	                uint64_t* outVersion, Buffer* removedObjBuffer,
	                uint64_t& pStampPrev);


	Status
	updatePeerInfo(uint64_t cts, uint64_t peerId, uint64_t eta, uint64_t pi); //Fixme: change arguments
}; // end ValidatorRPCHandler class

} // end namespace DSSN

#endif  /* VALIDATOR_RPC_HELPER_H */

