/* Copyright (c) 2020 Futurewei Technologies, Inc.
 *
 * All rights reserved.
 */

#include "DSSNService.h"
#include "WireFormat.h"
#include "MasterService.h"  //TODO: Remove

#include "Validator.h"

namespace DSSN {

DSSNService::DSSNService(Context* context, ServerList* serverList,
			 const ServerConfig* serverConfig)
    : context(context)
    , serverList(serverList)
    , serverConfig(serverConfig)
{
    kvStore = new HashmapKVStore();
    validator = new Validator(*kvStore);
    validator->start();
    tabletManager = new TabletManager();
    context->services[WireFormat::DSSN_SERVICE] = this;
}

DSSNService::~DSSNService()
{
    context->services[WireFormat::DSSN_SERVICE] = NULL;
    delete kvStore;
    delete validator;
    delete tabletManager;
}

void
DSSNService::dispatch(WireFormat::Opcode opcode, Rpc* rpc)
{
    switch (opcode){
        case WireFormat::TxCommitDSSN::opcode:
	    callHandler<WireFormat::TxCommitDSSN, DSSNService,
			&DSSNService::txCommit>(rpc);
	    break;
        case WireFormat::MultiOpDSSN::opcode:
	    callHandler<WireFormat::MultiOpDSSN, DSSNService,
			&DSSNService::multiOp>(rpc);
	    break;
        case WireFormat::ReadDSSN::opcode:
	    callHandler<WireFormat::ReadDSSN, DSSNService,
			&DSSNService::read>(rpc);
	    break;
        case WireFormat::ReadKeysAndValueDSSN::opcode:
	    callHandler<WireFormat::ReadKeysAndValueDSSN, DSSNService,
			&DSSNService::readKeysAndValue>(rpc);
	    break;
        case WireFormat::RemoveDSSN::opcode:
	    callHandler<WireFormat::RemoveDSSN, DSSNService,
		      &DSSNService::remove>(rpc);
	    break;
        case WireFormat::TakeTabletOwnershipDSSN::opcode:
	    callHandler<WireFormat::TakeTabletOwnershipDSSN, DSSNService,
		      &DSSNService::takeTabletOwnership>(rpc);
	    break;
        case WireFormat::WriteDSSN::opcode:
	    callHandler<WireFormat::WriteDSSN, DSSNService,
			&DSSNService::write>(rpc);
	    break;
        case WireFormat::TxDecisionDSSN::opcode:  //TODO: remove
	    callHandler<WireFormat::TxDecisionDSSN, DSSNService,
			&DSSNService::txDecision>(rpc);
	    break;
        case WireFormat::DSSN_NOTIFY_TEST:
	    RAMCLOUD_LOG(NOTICE, "Received notify test messages");
	    break;
        default:
	    throw UnimplementedRequestError(HERE);
    }
}

void
DSSNService::read(const WireFormat::ReadDSSN::Request* reqHdr,
		  WireFormat::ReadDSSN::Response* respHdr,
		  Rpc* rpc)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);

    uint32_t reqOffset = sizeof32(*reqHdr);
    const void* stringKey = rpc->requestPayload->getRange(
            reqOffset, reqHdr->keyLength);

    if (stringKey == NULL) {
        respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
        return;
    }

    uint64_t tableId = reqHdr->tableId;
    KLayout k(reqHdr->keyLength + sizeof(tableId)); //make room composite key in KVStore
    std::memcpy(k.key.get(), &tableId, sizeof(tableId));
    std::memcpy(k.key.get() + sizeof(tableId), stringKey,  reqHdr->keyLength);

    KVLayout *kv = kvStore->fetch(k);
    if (!kv || kv->getVLayout().isTombstone) {
        respHdr->common.status = RAMCloud::STATUS_OBJECT_DOESNT_EXIST;
        return;
    }

    uint32_t initialLength = rpc->replyPayload->size();

    Buffer buffer;
    if (kv->getVLayout().valueLength > 0) {
        buffer.alloc(kv->getVLayout().valueLength);
        buffer.append(kv->getVLayout().valuePtr, kv->getVLayout().valueLength);
    }

    Key key(tableId, stringKey, reqHdr->keyLength);
    Object object(key, kv->getVLayout().valuePtr, kv->getVLayout().valueLength, 0, 0, buffer);
    object.appendValueToBuffer(rpc->replyPayload);

    respHdr->meta.pstamp = kv->getVLayout().meta.pStamp;
    respHdr->meta.sstamp = kv->getVLayout().meta.sStamp;
    respHdr->meta.cstamp = kv->getVLayout().meta.cStamp;
    respHdr->length = rpc->replyPayload->size() - initialLength;
}

void
DSSNService::readKeysAndValue(const WireFormat::ReadKeysAndValueDSSN::Request* reqHdr,
			      WireFormat::ReadKeysAndValueDSSN::Response* respHdr,
			      Rpc* rpc)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);

    uint32_t reqOffset = sizeof32(*reqHdr);
    const void* stringKey = rpc->requestPayload->getRange(
            reqOffset, reqHdr->keyLength);

    if (stringKey == NULL) {
        respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
        rpc->sendReply();
        return;
    }

    uint64_t tableId = reqHdr->tableId;
    KLayout k(reqHdr->keyLength + sizeof(tableId)); //make room composite key in KVStore
    std::memcpy(k.key.get(), &tableId, sizeof(tableId));
    std::memcpy(k.key.get() + sizeof(tableId), stringKey,  reqHdr->keyLength);

    KVLayout *kv = kvStore->fetch(k);
    if (!kv) {
        respHdr->common.status = RAMCloud::STATUS_OBJECT_DOESNT_EXIST;
        return;
    }

    Key key(tableId, stringKey, reqHdr->keyLength);
    uint32_t initialLength = rpc->replyPayload->size();

    Buffer buffer;
    if (kv->getVLayout().valueLength > 0) {
        buffer.alloc(kv->getVLayout().valueLength);
        buffer.append(kv->getVLayout().valuePtr, kv->getVLayout().valueLength);
    }

    Object object(key, kv->getVLayout().valuePtr, kv->getVLayout().valueLength, 0, 0, buffer);
    object.appendKeysAndValueToBuffer(*(rpc->replyPayload));

    respHdr->meta.pstamp = kv->getVLayout().meta.pStamp;
    respHdr->meta.sstamp = kv->getVLayout().meta.sStamp;
    respHdr->meta.cstamp = kv->getVLayout().meta.cStamp;
    respHdr->length = rpc->replyPayload->size() - initialLength;
}

void
DSSNService::multiOp(const WireFormat::MultiOpDSSN::Request* reqHdr,
		     WireFormat::MultiOpDSSN::Response* respHdr,
		     Rpc* rpc)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);
    switch (reqHdr->type) {
        case WireFormat::MultiOp::OpType::INCREMENT:
            multiIncrement(reqHdr, respHdr, rpc);
            break;
        case WireFormat::MultiOp::OpType::READ:
            multiRead(reqHdr, respHdr, rpc);
            break;
        case WireFormat::MultiOp::OpType::REMOVE:
            multiRemove(reqHdr, respHdr, rpc);
            break;
        case WireFormat::MultiOp::OpType::WRITE:
            multiWrite(reqHdr, respHdr, rpc);
            break;
        default:
            LOG(ERROR, "Unimplemented multiOp (type = %u) received!",
                    (uint32_t) reqHdr->type);
            prepareErrorResponse(rpc->replyPayload,
                    STATUS_UNIMPLEMENTED_REQUEST);
            break;
    }
}

void
DSSNService::multiIncrement(const WireFormat::MultiOp::Request* reqHdr,
			    WireFormat::MultiOp::Response* respHdr,
			    Rpc* rpc)
{
    RAMCloud::MasterService *s = (RAMCloud::MasterService *)context->services[WireFormat::MASTER_SERVICE];
    s->multiIncrement(reqHdr, respHdr, rpc);
}

void
DSSNService::multiRead(const WireFormat::MultiOp::Request* reqHdr,
		       WireFormat::MultiOp::Response* respHdr,
		       Rpc* rpc)
{
    uint32_t numRequests = reqHdr->count;
    uint32_t reqOffset = sizeof32(*reqHdr);

    respHdr->count = numRequests;
    uint32_t oldResponseLength = rpc->replyPayload->size();

    // std::cout << "MultiRead nReq=" << numRequests << " InitRespLen=" << oldResponseLength << std::endl;

    // Each iteration extracts one request from request rpc, finds the
    // corresponding object, and appends the response to the response rpc.
    for (uint32_t i = 0; ; i++) {
        // If the RPC response has exceeded the legal limit, truncate it
        // to the last object that fits below the limit (the client will
        // retry the objects we don't return).
        uint32_t newLength = rpc->replyPayload->size();
        if (newLength > Transport::MAX_RPC_LEN) {
            rpc->replyPayload->truncate(oldResponseLength);
            respHdr->count = i-1;
            break;
        } else {
            oldResponseLength = newLength;
        }
        if (i >= numRequests) {
            // The loop-termination check is done here rather than in the
            // "for" statement above so that we have a chance to do the
            // size check above even for every object inserted, including
            // the last object and those with STATUS_OBJECT_DOESNT_EXIST.
            break;
        }

        const WireFormat::MultiOp::Request::ReadPart *currentReq =
                rpc->requestPayload->getOffset<
                WireFormat::MultiOp::Request::ReadPart>(reqOffset);
        reqOffset += sizeof32(WireFormat::MultiOp::Request::ReadPart);

        const void* stringKey = rpc->requestPayload->getRange(
                reqOffset, currentReq->keyLength);
        reqOffset += currentReq->keyLength;

        if (stringKey == NULL) {
            respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
            break;
        }

        WireFormat::MultiOp::Response::ReadPart* currentResp =
               rpc->replyPayload->emplaceAppend<
               WireFormat::MultiOp::Response::ReadPart>();

        // ---- get value of the current key -----
        uint64_t tableId = currentReq->tableId;
        KLayout k(currentReq->keyLength + sizeof(tableId)); //make room composite key in KVStore
        std::memcpy(k.key.get(), &tableId, sizeof(tableId));
        std::memcpy(k.key.get() + sizeof(tableId), stringKey,  currentReq->keyLength);

        KVLayout *kv = kvStore->fetch(k);

        // std::string ky((const char*)stringKey, currentReq->keyLength); //XXX
        // std::cout << "tabldId:" << tableId << " key: " << ky ;  // XXX

        if (!kv || kv->getVLayout().isTombstone) {
            currentResp->status = RAMCloud::STATUS_OBJECT_DOESNT_EXIST;

            // if (kv) std::cout << " v: is tomb" << std::endl; // XXX
            // else    std::cout << " v: not found" << std::endl; // XXX

            continue;
        }

        uint32_t initialLength = rpc->replyPayload->size();

        // std::string v((const char*)kv->getVLayout().valuePtr, kv->getVLayout().valueLength); //XXX
        // std::cout << " vallen: " << kv->getVLayout().valueLength << " val: " << v ; //XXX
        // std::cout << " replyPayloadSize: " << initialLength; // XXX

        Key key(tableId, stringKey, currentReq->keyLength);
        Buffer buffer;
        if (kv->getVLayout().valueLength > 0) {
            buffer.alloc(kv->getVLayout().valueLength);
            buffer.append(kv->getVLayout().valuePtr, kv->getVLayout().valueLength);
        }
        Object object(key, kv->getVLayout().valuePtr, kv->getVLayout().valueLength, 0, 0, buffer);
        object.appendKeysAndValueToBuffer(*(rpc->replyPayload));

        currentResp->meta.pstamp = kv->getVLayout().meta.pStamp; // eta
        currentResp->meta.sstamp = kv->getVLayout().meta.sStamp; // pi
        currentResp->length = rpc->replyPayload->size() - initialLength;

        // std::cout << "repLen: " << currentResp->length << std::endl; // XXX
    }
}

void
DSSNService::multiRemove(const WireFormat::MultiOp::Request* reqHdr,
			 WireFormat::MultiOp::Response* respHdr,
			 Rpc* rpc)
{
    RAMCloud::MasterService *s = (RAMCloud::MasterService *)context->services[WireFormat::MASTER_SERVICE];
    s->multiRemove(reqHdr, respHdr, rpc);
}

void
DSSNService::multiWrite(const WireFormat::MultiOp::Request* reqHdr,
			WireFormat::MultiOp::Response* respHdr,
			Rpc* rpc)
{
    uint32_t numRequests = reqHdr->count;
    uint32_t reqOffset = sizeof32(*reqHdr);
    respHdr->count = numRequests;

    // Store info about objects being removed (overwritten)
    // so that we can later remove index entries corresponding to them.
    // This is space inefficient as it occupies numRequests times size of
    // Buffer on stack.
    Buffer oldObjectBuffers[numRequests];

    // Each iteration extracts one request from the rpc, writes the object
    // if possible, and appends a status and version to the response buffer.
    for (uint32_t i = 0; i < numRequests; i++) {
        const WireFormat::MultiOp::Request::WritePart *currentReq =
                rpc->requestPayload->getOffset<
                WireFormat::MultiOp::Request::WritePart>(reqOffset);

        if (currentReq == NULL) {
            respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
            break;
        }

        reqOffset += sizeof32(WireFormat::MultiOp::Request::WritePart);

        if (rpc->requestPayload->size() < reqOffset + currentReq->length) {
            respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
            break;
        }
        WireFormat::MultiOp::Response::WritePart* currentResp =
                rpc->replyPayload->emplaceAppend<
                WireFormat::MultiOp::Response::WritePart>();

        Object object(currentReq->tableId, 0, 0, *(rpc->requestPayload),
                reqOffset, currentReq->length);

        // ---- write the object ----
        KeyLength pKeyLen;
        uint32_t pValLen;
        uint64_t tableId = object.getTableId();
        const void* pVal = object.getValue(&pValLen);
        const void* pKey = object.getKey(0, &pKeyLen);

        KVLayout pkv(pKeyLen + sizeof(tableId)); //make room composite key in KVStore
        std::memcpy(pkv.getKey().key.get(), &tableId, sizeof(tableId));
        std::memcpy(pkv.getKey().key.get() + sizeof(tableId), pKey, pKeyLen);
        pkv.v.valueLength = pValLen;
        pkv.v.valuePtr = (uint8_t*)const_cast<void*>(pVal);
    
        KVLayout *kv = kvStore->fetch(pkv.k);
 
        if (kv == NULL) {
            KVLayout *nkv = kvStore->preput(pkv);
            kvStore->putNew(nkv, 0, 0xffffffffffffffff);
        } else {
            void * pval = new char[pValLen];
            std::memcpy(pval, pVal, pValLen);
            kvStore->put(kv, 0, 0xffffffffffffffff, (uint8_t*)pval, pValLen);
        }
        currentResp->status = STATUS_OK;
        // ---- write one object done ----

        reqOffset += currentReq->length;
    }

    // By design, our response will be shorter than the request. This ensures
    // that the response can go back in a single RPC.
    assert(rpc->replyPayload->size() <= Transport::MAX_RPC_LEN);

    rpc->sendReply();
}
  
void
DSSNService::remove(const WireFormat::RemoveDSSN::Request* reqHdr,
		    WireFormat::RemoveDSSN::Response* respHdr,
		    Rpc* rpc)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);

    assert(reqHdr->rpcId > 0);

    const void* stringKey = rpc->requestPayload->getRange(
            sizeof32(*reqHdr), reqHdr->keyLength);

    if (stringKey == NULL) {
        respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
        rpc->sendReply();
        return;
    }

    uint64_t tableId = reqHdr->tableId;
    KLayout k(reqHdr->keyLength + sizeof(tableId)); //make room composite key in KVStore
    std::memcpy(k.key.get(), &tableId, sizeof(tableId));
    std::memcpy(k.key.get() + sizeof(tableId), stringKey,  reqHdr->keyLength);
    Key key(reqHdr->tableId, stringKey, reqHdr->keyLength);

    KVLayout *kv = kvStore->fetch(k);
    if (!kv) {
        return; // nothing to be removed
    }

    kv->getVLayout().isTombstone = true;
}

void
DSSNService::write(const WireFormat::WriteDSSN::Request* reqHdr,
		  WireFormat::WriteDSSN::Response* respHdr,
		  Rpc* rpc)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);

    // A temporary object that has an invalid version and timestamp
    // is created here to make sure the object format does not leak
    // outside the object class.
    Object object(reqHdr->tableId, 0, 0, *(rpc->requestPayload),
            sizeof32(*reqHdr));

    KeyLength pKeyLen;
    const void* pKey = object.getKey(0, &pKeyLen);
    uint32_t pValLen;
    const void* pVal = object.getValue(&pValLen);
    uint64_t tableId = object.getTableId();

    #if (0) // No table check. DSSN does not (yet) support the RamCloud style Table Mgmt
    Key key(tableId, pKey, pKeyLen);
    // If the tablet doesn't exist in the NORMAL state, we must plead ignorance.
    TabletManager::Tablet tablet;
    if (!tabletManager->getTablet(key, &tablet)) {
        respHdr->common.status = RAMCloud::STATUS_UNKNOWN_TABLET;
        return;
    }

    if (tablet.state != TabletManager::NORMAL) {
        if (tablet.state == TabletManager::LOCKED_FOR_MIGRATION)
            throw RetryException(HERE, 1000, 2000,
                    "Tablet is currently locked for migration!");
        respHdr->common.status = RAMCloud::STATUS_UNKNOWN_TABLET;
        return;
    }
    #endif // 0

    KVLayout pkv(pKeyLen + sizeof(tableId)); //make room composite key in KVStore
    std::memcpy(pkv.getKey().key.get(), &tableId, sizeof(tableId));
    std::memcpy(pkv.getKey().key.get() + sizeof(tableId), pKey, pKeyLen);
    pkv.v.valueLength = pValLen;
    pkv.v.valuePtr = (uint8_t*)const_cast<void*>(pVal);

    // std::string k((const char*)pKey, (uint32_t)pKeyLen); //DBG
    // std::string v((const char*)pVal, (uint32_t)pValLen);
    // std::cout << "write: key: " << k << " vallen: " << pValLen << " val: " << v << std::endl; 

    KVLayout *kv = kvStore->fetch(pkv.k);

    if (kv == NULL) {
        KVLayout *nkv = kvStore->preput(pkv);
        kvStore->putNew(nkv, 0, 0xffffffffffffffff);
    } else {
        void * pval = new char[pValLen];
        std::memcpy(pval, pVal, pValLen);
        kvStore->put(kv, 0, 0xffffffffffffffff, (uint8_t*)pval, pValLen);
    }
}
  
void
DSSNService::takeTabletOwnership(const WireFormat::TakeTabletOwnershipDSSN::Request* reqHdr,
				 WireFormat::TakeTabletOwnershipDSSN::Response* respHdr,
				 Rpc* rpc)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);

    bool added = tabletManager->addTablet(reqHdr->tableId,
            reqHdr->firstKeyHash, reqHdr->lastKeyHash,
            TabletManager::NORMAL);
    assert (added);
}

void
DSSNService::txCommit(const WireFormat::TxCommitDSSN::Request* reqHdr,
		      WireFormat::TxCommitDSSN::Response* respHdr,
		      Rpc* rpc)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);


    uint32_t reqOffset = sizeof32(*reqHdr);

    uint32_t participantCount = reqHdr->participantCount;
    WireFormat::TxParticipant *participants =
    		(WireFormat::TxParticipant*)rpc->requestPayload->getRange(reqOffset,
    				sizeof32(WireFormat::TxParticipant) * participantCount);
    reqOffset += sizeof32(WireFormat::TxParticipant) * participantCount;

    uint32_t numRequests = reqHdr->opCount;
    uint32_t numReadRequests = reqHdr->readOpCount;
    assert(numRequests > 0);
    assert(numReadRequests <= numRequests);

    const WireFormat::TxPrepare::OpType *type =
            rpc->requestPayload->getOffset<
            WireFormat::TxPrepare::OpType>(reqOffset);

    if (*type != WireFormat::TxPrepare::READONLY) {
    	//read-only transaction needs no validation
        respHdr->common.status = STATUS_RETRY;
        respHdr->vote = WireFormat::TxPrepare::COMMITTED;
        rpc->sendReply();
        return;
    }

    TxEntry *txEntry = new TxEntry(numReadRequests, numRequests - numReadRequests);
    txEntry->setCTS(reqHdr->lease.timestamp);
    txEntry->setPStamp(reqHdr->meta.pstamp);
    txEntry->setSStamp(reqHdr->meta.sstamp);
    //Fixme: we only focus on local tx for now, so we will leave the peer set empty
	for (uint32_t i = 1/*Fixme to 0 later*/; i < participantCount; i++) {
		txEntry->insertPeerSet(participants[i].dssnServerId);
	}
    uint32_t readSetIdx = 0;
    uint32_t writeSetIdx = 0;

    for (uint32_t i = 0; i < numRequests; i++) {
        Tub<PreparedOp> op;
        uint64_t tableId, rpcId;
        RejectRules rejectRules;

        respHdr->common.status = STATUS_OK;
        respHdr->vote = WireFormat::TxPrepare::PREPARED;

        Buffer buffer;
        const WireFormat::TxPrepare::OpType *type =
                rpc->requestPayload->getOffset<
                WireFormat::TxPrepare::OpType>(reqOffset);
        if (*type == WireFormat::TxPrepare::READ) {
            const WireFormat::TxPrepare::Request::ReadOp *currentReq =
                    rpc->requestPayload->getOffset<
                    WireFormat::TxPrepare::Request::ReadOp>(reqOffset);

            reqOffset += sizeof32(WireFormat::TxPrepare::Request::ReadOp);

            if (currentReq == NULL || rpc->requestPayload->size() <
                                      reqOffset + currentReq->keyLength) {
                respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
                respHdr->vote = WireFormat::TxPrepare::ABORT;
                break;
            }
            tableId = currentReq->tableId;
            rpcId = currentReq->rpcId;
            rejectRules = currentReq->rejectRules;

            const void* stringKey = rpc->requestPayload->getRange(
            		reqOffset, currentReq->keyLength);
            reqOffset += currentReq->keyLength;

            if (stringKey == NULL) {
            	respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
                respHdr->vote = WireFormat::TxPrepare::ABORT;
            	break;
            }

            KVLayout pkv(currentReq->keyLength + sizeof(tableId)); //make room composite key in KVStore
            std::memcpy(pkv.getKey().key.get(), &tableId, sizeof(tableId));
            std::memcpy(pkv.getKey().key.get() + sizeof(tableId), stringKey, currentReq->keyLength);
            KVLayout *nkv = kvStore->preput(pkv);
            txEntry->insertReadSet(nkv, readSetIdx++);
            assert(readSetIdx <= numReadRequests);

        } else if (*type == WireFormat::TxPrepare::REMOVE) {
            const WireFormat::TxPrepare::Request::RemoveOp *currentReq =
                    rpc->requestPayload->getOffset<
                    WireFormat::TxPrepare::Request::RemoveOp>(reqOffset);

            reqOffset += sizeof32(WireFormat::TxPrepare::Request::RemoveOp);

            if (currentReq == NULL || rpc->requestPayload->size() <
                                      reqOffset + currentReq->keyLength) {
                respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
                respHdr->vote = WireFormat::TxPrepare::ABORT;
                break;
            }
            tableId = currentReq->tableId;
            rpcId = currentReq->rpcId;
            rejectRules = currentReq->rejectRules;

            const void* stringKey = rpc->requestPayload->getRange(
            		reqOffset, currentReq->keyLength);
            reqOffset += currentReq->keyLength;

            if (stringKey == NULL) {
            	respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
                respHdr->vote = WireFormat::TxPrepare::ABORT;
            	break;
            }

            //a remove is treated as a tombstone entry without value
            KVLayout pkv(currentReq->keyLength + sizeof(tableId)); //make room composite key in KVStore
            std::memcpy(pkv.getKey().key.get(), &tableId, sizeof(tableId));
            std::memcpy(pkv.getKey().key.get() + sizeof(tableId), stringKey, currentReq->keyLength);
            pkv.v.isTombstone = true;
            KVLayout *nkv = kvStore->preput(pkv);
            txEntry->insertWriteSet(nkv, writeSetIdx++);
            assert(writeSetIdx <= (numRequests - numReadRequests));
        } else if (*type == WireFormat::TxPrepare::WRITE) {
            const WireFormat::TxPrepare::Request::WriteOp *currentReq =
                    rpc->requestPayload->getOffset<
                    WireFormat::TxPrepare::Request::WriteOp>(reqOffset);

            reqOffset += sizeof32(WireFormat::TxPrepare::Request::WriteOp);

            if (currentReq == NULL || rpc->requestPayload->size() <
                                      reqOffset + currentReq->length) {
                respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
                respHdr->vote = WireFormat::TxPrepare::ABORT;
                break;
            }
            tableId = currentReq->tableId;
            rpcId = currentReq->rpcId;
            rejectRules = currentReq->rejectRules;
            op.construct(*type, 0 /*irrelevant*/, 0 /*irrelevant*/,
                                     rpcId,
                                     tableId, 0, 0,
                                     *(rpc->requestPayload), reqOffset,
                                     currentReq->length);

            KeyLength keyLen;
            const void* pKey = op.get()->object.getKey(0, &keyLen);
            uint32_t valLen;
            const void* pVal = op.get()->object.getValue(&valLen);
            uint64_t tableId = op.get()->object.getTableId();

            if (keyLen == 0) {
            	respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
                respHdr->vote = WireFormat::TxPrepare::ABORT;
            	break;
            }

            KVLayout pkv(keyLen + sizeof(tableId)); //make room composite key in KVStore
            std::memcpy(pkv.getKey().key.get(), &tableId, sizeof(tableId));
            std::memcpy(pkv.getKey().key.get() + sizeof(tableId), pKey, keyLen);
            if (valLen == 0) {
                pkv.getVLayout().isTombstone = true;
            } else {
            	pkv.getVLayout().valueLength = valLen;
            	pkv.getVLayout().valuePtr = (uint8_t*)const_cast<void*>(pVal);
            }
            KVLayout *nkv = kvStore->preput(pkv);
            txEntry->insertWriteSet(nkv, writeSetIdx++);
            assert(writeSetIdx <= (numRequests - numReadRequests));
        } else if (*type == WireFormat::TxPrepare::READONLY) {
            assert(0);
        } else {
            respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
            respHdr->vote = WireFormat::TxPrepare::ABORT;
            break;
        }

    }

    if (respHdr->common.status == STATUS_OK) {
    	validator->insertTxEntry(txEntry);

    	//Fixme: for now just loop to wait for result
    	while (txEntry->getTxCIState() != TxEntry::TX_CI_FINISHED) { //Fixme: need volatile?
    		std::this_thread::yield();
    	}

    	if (txEntry->getTxState() == TxEntry::TX_ABORT) {
            respHdr->vote = WireFormat::TxPrepare::ABORT;
    	} else if (txEntry->getTxState() == TxEntry::TX_COMMIT) {
            respHdr->vote = WireFormat::TxPrepare::COMMITTED;
    	} else
    		assert(0);
    }

    delete txEntry;

    rpc->sendReply();
}

void
DSSNService::txDecision(const WireFormat::TxDecisionDSSN::Request* reqHdr,
			WireFormat::TxDecisionDSSN::Response* respHdr,
			Rpc* rpc)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);
    RAMCloud::MasterService *s = (RAMCloud::MasterService *)context->services[WireFormat::MASTER_SERVICE];
    s->txDecision(reqHdr, respHdr, rpc);
}

}
