/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef VALIDATOR_RPC_HANDLER_H
#define VALIDATOR_RPC_HANDLER_H

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
    ValidatorRPCHelper::rejectOperation(const RejectRules* rejectRules, uint64_t version);

    PUBLIC:
	ValidatorRPCHelper(Validator &validator) { this->validator = validator; }

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
	 * \param valueOnly
	 *      If true, then only the value portion of the object is written to
	 *      outBuffer. Otherwise, keys and value are written to outBuffer.
	 * \return
	 *      Returns STATUS_OK if the lookup succeeded and the reject rules did not
	 *      preclude this read. Other status values indicate different failures
	 *      (object not found, tablet doesn't exist, reject rules applied, etc).
	 */
	Status
	readObject(uint64_t tableId, Key& key, Buffer* outBuffer,
	                RejectRules* rejectRules, uint64_t* outVersion,
	                bool valueOnly);

}; // end ValidatorRPCHandler class

} // end namespace DSSN

#endif  /* VALIDATOR_RPC_HANDLER_H */

