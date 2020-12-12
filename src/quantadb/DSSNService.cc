/* Copyright 2020 Futurewei Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include "DSSNService.h"
#include "WireFormat.h"
#include "MasterService.h"  //TODO: Remove
#include "Validator.h"

namespace QDB {

DSSNService::DSSNService(Context* context, ServerList* serverList,
        const ServerConfig* serverConfig)
: context(context)
, serverList(serverList)
, serverConfig(serverConfig)
{
    RAMCLOUD_LOG(NOTICE, "%s", getServerAddress().c_str());  //Mike, please remove it.
    kvStore = new HashmapKVStore();
    validator = new Validator(*kvStore, this, serverConfig->master.isTesting);
    tabletManager = new TabletManager();
    mMonitor = new DSSNServiceMonitor(this, context->metricExposer);
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
      {
	Metric* m = mMonitor->getOpMetric(DSSNServiceCommit);
        OpTrace t(m);
        callHandler<WireFormat::TxCommitDSSN, DSSNService,
        &DSSNService::txCommit>(rpc);
      }
      break;
    case WireFormat::MultiOpDSSN::opcode:
        callHandler<WireFormat::MultiOpDSSN, DSSNService,
        &DSSNService::multiOp>(rpc);
        break;
    case WireFormat::ReadDSSN::opcode:
      {
	Metric* m = mMonitor->getOpMetric(DSSNServiceRead);
        OpTrace t(m);
        callHandler<WireFormat::ReadDSSN, DSSNService,
        &DSSNService::read>(rpc);
      }
      break;
    case WireFormat::ReadKeysAndValueDSSN::opcode:
      {
	Metric* m = mMonitor->getOpMetric(DSSNServiceReadKV);
        OpTrace t(m);
        callHandler<WireFormat::ReadKeysAndValueDSSN, DSSNService,
        &DSSNService::readKeysAndValue>(rpc);
      }
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
        RAMCLOUD_LOG(NOTICE, "Received notify test message");
	rpc->setNoRsp();
        break;
    case WireFormat::DSSNSendInfoAsync::opcode:
        handleSendInfoAsync(rpc);
	rpc->setNoRsp();
        break;
    case WireFormat::DSSNRequestInfoAsync::opcode:
        handleRequestInfoAsync(rpc);
	rpc->setNoRsp();
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

    KVLayout *kv;
    if (!validator->read(k, kv)) {
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

    KVLayout *kv;
    if (!validator->read(k, kv)) {
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
      {
	Metric* m = mMonitor->getOpMetric(DSSNServiceReadMulti);
        OpTrace t(m);
        multiRead(reqHdr, respHdr, rpc);
      }
      break;
    case WireFormat::MultiOp::OpType::REMOVE:
        multiRemove(reqHdr, respHdr, rpc);
        break;
    case WireFormat::MultiOp::OpType::WRITE:
      {
	Metric* m = mMonitor->getOpMetric(DSSNServiceWriteMulti);
        OpTrace t(m);
        multiWrite(reqHdr, respHdr, rpc);
      }
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

        KVLayout *kv;
        if (!validator->read(k, kv)) {
            currentResp->status = RAMCloud::STATUS_OBJECT_DOESNT_EXIST;
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
        currentResp->meta.cstamp = kv->getVLayout().meta.cStamp; // cts
        currentResp->length = rpc->replyPayload->size() - initialLength;

        // std::cout << "repLen: " << currentResp->length << std::endl; // XXX
    }
}

void
DSSNService::multiRemove(const WireFormat::MultiOp::Request* reqHdr,
        WireFormat::MultiOp::Response* respHdr,
        Rpc* rpc)
{
    assert(0); //disallow backdoor remove for now
    /*
    uint32_t numRequests = reqHdr->count;
    uint32_t reqOffset = sizeof32(*reqHdr);

    respHdr->count = numRequests;

    // Each iteration extracts one request from request rpc, deletes the
    // corresponding object if possible, and appends the response to the
    // response rpc.
    for (uint32_t i = 0; i < numRequests; i++) {
        const WireFormat::MultiOp::Request::RemovePart *currentReq =
                rpc->requestPayload->getOffset<
                WireFormat::MultiOp::Request::RemovePart>(reqOffset);

        if (currentReq == NULL) {
            respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
            break;
        }

        reqOffset += sizeof32(WireFormat::MultiOp::Request::RemovePart);
        const void* stringKey = rpc->requestPayload->getRange(
                reqOffset, currentReq->keyLength);
        reqOffset += currentReq->keyLength;

        if (stringKey == NULL) {
            respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
            break;
        }

        WireFormat::MultiOp::Response::RemovePart* currentResp =
                rpc->replyPayload->emplaceAppend<
                WireFormat::MultiOp::Response::RemovePart>();

        // ---- remove one object -----
        uint64_t tableId = currentReq->tableId;
        KLayout k(currentReq->keyLength + sizeof(tableId)); //make room composite key in KVStore
        std::memcpy(k.key.get(), &tableId, sizeof(tableId));
        std::memcpy(k.key.get() + sizeof(tableId), stringKey,  currentReq->keyLength);

        KVLayout *kv = kvStore->fetch(k);
        if (kv) {
            kv->getVLayout().isTombstone = true;
        }
        currentResp->status = STATUS_OK; 
        // ----
    }
    */
}

void
DSSNService::multiWrite(const WireFormat::MultiOp::Request* reqHdr,
        WireFormat::MultiOp::Response* respHdr,
        Rpc* rpc)
{
    uint32_t numRequests = reqHdr->count;
    uint32_t reqOffset = sizeof32(*reqHdr);
    respHdr->count = numRequests;
    TxEntry* txEntry = new TxEntry(0, numRequests);
    RpcHandle* handle = rpc->enableAsync();
    txEntry->setRpcHandle(handle);

    // Each iteration extracts one request from the rpc, writes the object
    // if possible, and appends a status and version to the response buffer.
    for (uint32_t index = 0; index < numRequests; index++) {
        const WireFormat::MultiOp::Request::WritePart *currentReq =
                rpc->requestPayload->getOffset<
                WireFormat::MultiOp::Request::WritePart>(reqOffset);

        if (currentReq == NULL) {
            respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
            handle->sendReplyAsync();
            delete txEntry;
            return;
        }

        reqOffset += sizeof32(WireFormat::MultiOp::Request::WritePart);

        if (rpc->requestPayload->size() < reqOffset + currentReq->length) {
            respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
            handle->sendReplyAsync();
            delete txEntry;
            return;
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

        KVLayout pkv(pKeyLen + sizeof(tableId)); //make room for composite key in KVStore
        std::memcpy(pkv.getKey().key.get(), &tableId, sizeof(tableId));
        std::memcpy(pkv.getKey().key.get() + sizeof(tableId), pKey, pKeyLen);
        pkv.v.valueLength = pValLen;
        pkv.v.valuePtr = (uint8_t*)const_cast<void*>(pVal);

        if (pValLen == 0) {
            pkv.getVLayout().isTombstone = true;
        } else {
            pkv.getVLayout().valueLength = pValLen;
            pkv.getVLayout().valuePtr = (uint8_t*)const_cast<void*>(pVal);
        }
        KVLayout *nkv = kvStore->preput(pkv);
        if (nkv != NULL) {
            txEntry->insertWriteSet(nkv, index);
            currentResp->status = STATUS_OK;
        } else {
            respHdr->common.status = STATUS_INTERNAL_ERROR;
            handle->sendReplyAsync();
            delete txEntry;
            return;
        }

        // ---- write one object done ----

        reqOffset += currentReq->length;
    }

    // By design, our response will be shorter than the request. This ensures
    // that the response can go back in a single RPC.
    assert(rpc->replyPayload->size() <= Transport::MAX_RPC_LEN);
    Transport::ServerRpc* srpc = handle->getServerRpc();
    srpc->endRpcPreProcessingTimer();
    if (validator->insertTxEntry(txEntry)) {
        while (validator->testRun()) {
            if (txEntry->getTxCIState() < TxEntry::TX_CI_FINISHED)
                continue;
            //reply already sent in testRun(), free memory now
            delete txEntry;
            return;
        }

        return; //delay reply and freeing memory
    }
    respHdr->common.status = STATUS_INTERNAL_ERROR;
    handle->sendReplyAsync();
    delete txEntry;
}

void
DSSNService::remove(const WireFormat::RemoveDSSN::Request* reqHdr,
        WireFormat::RemoveDSSN::Response* respHdr,
        Rpc* rpc)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);

    assert(0); //disallow backdoor remove for now
    /*
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

    KVLayout *kv = kvStore->fetch(k);
    if (!kv) {
        return; // nothing to be removed
    }

    kv->getVLayout().isTombstone = true;
    */
}

void
DSSNService::write(const WireFormat::WriteDSSN::Request* reqHdr,
        WireFormat::WriteDSSN::Response* respHdr,
        Rpc* rpc)
{
    //Fixme: later replace this with a single-write transaction
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

    //prepare a single write local tx
    TxEntry *txEntry = new TxEntry(0, 1);
    RpcHandle* handle = rpc->enableAsync();
    txEntry->setRpcHandle(handle);
    if (pValLen == 0) {
        pkv.getVLayout().isTombstone = true;
    } else {
        pkv.getVLayout().valueLength = pValLen;
        pkv.getVLayout().valuePtr = (uint8_t*)const_cast<void*>(pVal);
    }
    KVLayout *nkv = kvStore->preput(pkv);
    if (nkv != NULL) {
        txEntry->insertWriteSet(nkv, 0);
	Transport::ServerRpc* srpc = handle->getServerRpc();
	srpc->endRpcPreProcessingTimer();
        if (validator->insertTxEntry(txEntry)) {
            while (validator->testRun()) {
                if (txEntry->getTxCIState() < TxEntry::TX_CI_FINISHED)
                    continue;
                //reply already sent in testRun(), free memory now
                delete txEntry;
                return;
            }

            return; //delay reply and freeing memory
        }
    }
    respHdr->common.status = STATUS_INTERNAL_ERROR;
    handle->sendReplyAsync();
    delete txEntry;
}

void
DSSNService::takeTabletOwnership(const WireFormat::TakeTabletOwnershipDSSN::Request* reqHdr,
        WireFormat::TakeTabletOwnershipDSSN::Response* respHdr,
        Rpc* rpc)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);
    /**
     * Since DSSN is not currently managing the table state,
     * let Ramcloud backend handle it.
     **/
    RAMCloud::MasterService *s = (RAMCloud::MasterService *)context->services[WireFormat::MASTER_SERVICE];
    s->takeTabletOwnership(reqHdr, respHdr, rpc);
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

    /* Fixme: add a better indicator of read-only tx later
    const WireFormat::TxPrepare::OpType *type =
            rpc->requestPayload->getOffset<
            WireFormat::TxPrepare::OpType>(reqOffset);

    if (*type == WireFormat::TxPrepare::READONLY) {
    	assert(0);
    	//read-only transaction needs no validation
        respHdr->common.status = STATUS_RETRY;
        respHdr->vote = WireFormat::TxPrepare::COMMITTED;
        rpc->sendReply();
        return;
    }*/

    validator->getCounters().serverId = getServerId();

    //We are over-provisioning the read set to accommodate the potential RMWs
    TxEntry *txEntry = new TxEntry(numRequests, numRequests - numReadRequests);

    RpcHandle* handle = rpc->enableAsync();
    txEntry->setCTS(reqHdr->meta.cts);
    txEntry->setPStamp(reqHdr->meta.pstamp);
    txEntry->setSStamp(reqHdr->meta.sstamp);
    txEntry->setRpcHandle(handle);
    for (uint32_t i = 0; i < participantCount; i++) {
        if (participants[i].dssnServerId != getServerId())
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
        if (*type == WireFormat::TxPrepare::READ
                || *type == WireFormat::TxPrepare::READONLY) {
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
            pkv.meta().cStamp = currentReq->GetCStamp();
            KVLayout *nkv = kvStore->preput(pkv);
            if (nkv == NULL) {
                respHdr->common.status = STATUS_NO_TABLE_SPACE;
                respHdr->vote = WireFormat::TxPrepare::ABORT;
                validator->getCounters().preputErrors++;
                break;
            }
            txEntry->insertReadSet(nkv, readSetIdx++);
            assert(readSetIdx <= numRequests);

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
            if (nkv == NULL) {
                respHdr->common.status = STATUS_NO_TABLE_SPACE;
                respHdr->vote = WireFormat::TxPrepare::ABORT;
                break;
            }
            txEntry->insertWriteSet(nkv, writeSetIdx++);
            assert(writeSetIdx <= (numRequests - numReadRequests));
        } else if (*type == WireFormat::TxPrepare::WRITE ||
		   *type == WireFormat::TxPrepare::READ_MODIFY_WRITE) {
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
            reqOffset += currentReq->length;

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
            if (nkv == NULL) {
                respHdr->common.status = STATUS_NO_TABLE_SPACE;
                respHdr->vote = WireFormat::TxPrepare::ABORT;
                break;
            }
            txEntry->insertWriteSet(nkv, writeSetIdx++);
            assert(writeSetIdx <= (numRequests - numReadRequests));

            /*
             * The SSN paper presents an algorithm that is based on the
             * the SSN theorem and the assumption of an ERMIA-like version chain,
             * which contains both the committed versions and the pending versions.
             * Because of that, it can and needs to detect read-modify-write RMW
             * transaction and remove RMW tuples from the read set to avoid
             * its implementation-specific self-inflicted pi equal to eta violation.
             * In our case, we have the coordinator tracking the pending versions
             * and validator tracking only the committed versions. The coordinator
             * identifies the RMW tuples and signals them to the validator, like here.
             * The validator can simply add these RMW tuples to both the write set
             * and the read set. As the validator uses only the committed versions
             * to do validation, there will not be self-inflicted pi equal to eta violation.
             */
            if (*type == WireFormat::TxPrepare::READ_MODIFY_WRITE) {
                txEntry->insertReadSet(nkv, readSetIdx++);
                assert(readSetIdx <= numRequests);
                nkv->meta().cStamp = currentReq->GetCStamp();
            }
        } else {
            respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
            respHdr->vote = WireFormat::TxPrepare::ABORT;
            break;
        }

    }

    if (respHdr->common.status == STATUS_OK) {
        //This is a workaround to correct the effect of over-provisioning the readSet.
        //A proper solution is to have the txCommit message to pass in the exact readSet size.
        txEntry->correctReadSet(readSetIdx);

        Transport::ServerRpc* srpc = handle->getServerRpc();
        srpc->endRpcPreProcessingTimer();
        if (validator->insertTxEntry(txEntry)) {
            while (validator->testRun()) {
                if (txEntry->getTxCIState() < TxEntry::TX_CI_FINISHED)
                    continue;
                //reply already sent in testRun(), free memory now
                delete txEntry;
                return;
            }

            return; //delay reply and freeing memory
        }
        respHdr->vote = WireFormat::TxPrepare::ABORT;
    }
    handle->sendReplyAsync(); //optional but can make send quicker
    delete txEntry;
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

bool
DSSNService::sendTxCommitReply(TxEntry *txEntry)
{
    Metric* m = mMonitor->getOpMetric(DSSNServiceSendTxReply);
    OpTrace t(m);
    RpcHandle *handle = static_cast<RpcHandle *>(txEntry->getRpcHandle());
    assert(handle != NULL);

    Transport::ServerRpc* rpc = handle->getServerRpc();
 
    const WireFormat::RequestCommon* header = rpc->requestPayload.getStart<WireFormat::RequestCommon>();
    WireFormat::Opcode opcode = WireFormat::Opcode(header->opcode);
    if (opcode == WireFormat::WriteDSSN::opcode) {
        //The RC write is treated as a single-write local transaction
        if (txEntry->getTxState() != TxEntry::TX_COMMIT) {
            WireFormat::WriteDSSN::Response* respHdr =
                    rpc->replyPayload.getStart<WireFormat::WriteDSSN::Response>();
            respHdr->common.status = STATUS_TX_WRITE_ABORT;
        }
        handle->sendReplyAsync();
        txEntry->setRpcHandle(NULL);
        return true;
    } else if (opcode == WireFormat::MultiOp::opcode) {
        //The RC multi-write is treated as one multi-write local transaction
        if (txEntry->getTxState() != TxEntry::TX_COMMIT) {
            WireFormat::MultiOp::Response* respHdr =
                    rpc->replyPayload.getStart<WireFormat::MultiOp::Response>();
            respHdr->common.status = STATUS_TX_WRITE_ABORT;
        }
        handle->sendReplyAsync();
        txEntry->setRpcHandle(NULL);
        return true;
    }

    WireFormat::TxCommitDSSN::Response* respHdr =
            rpc->replyPayload.getStart<WireFormat::TxCommitDSSN::Response>();
    if (txEntry->getTxState() == TxEntry::TX_COMMIT)
        respHdr->vote = WireFormat::TxPrepare::COMMITTED;
    else if (txEntry->getTxState() == TxEntry::TX_ABORT)
        respHdr->vote = WireFormat::TxPrepare::ABORT;
    else if (txEntry->getTxState() == TxEntry::TX_CONFLICT) {
        assert(0);
        respHdr->vote = WireFormat::TxPrepare::ABORT_REQUESTED; //Fixme: Need a code to signal conflict
    } else {
        assert(0);
        respHdr->vote = WireFormat::TxPrepare::ABORT_REQUESTED;
    }
    handle->sendReplyAsync();
    txEntry->setRpcHandle(NULL);
    return true;
}

bool
DSSNService::sendDSSNInfo(__uint128_t cts, TxEntry *txEntry, bool isSpecific, uint64_t target)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);
    Metric* m = mMonitor->getOpMetric(DSSNServiceSendDSSNInfo);
    OpTrace t(m);
    WireFormat::DSSNSendInfoAsync::Request req;
    req.senderPeerId = getServerId();
    assert(txEntry != NULL);
    req.cts = txEntry->getCTS();
    assert(cts == req.cts);
    req.pstamp = txEntry->getPStamp();
    req.sstamp = txEntry->getSStamp();
    req.txState = txEntry->getTxState();

    char *msg = reinterpret_cast<char *>(&req) + sizeof(WireFormat::Notification::Request);
    uint32_t length = sizeof(req) - sizeof(WireFormat::Notification::Request);
    if (isSpecific) {
        ServerId sid(target);
        Notifier::notify(context, WireFormat::DSSN_SEND_INFO_ASYNC,
                msg, length, sid);
    } else if (txEntry != NULL) {
        std::set<uint64_t>::iterator it;
        for (it = txEntry->getPeerSet().begin(); it != txEntry->getPeerSet().end(); it++) {
            ServerId sid(*it);
            Notifier::notify(context, WireFormat::DSSN_SEND_INFO_ASYNC,
                    msg, length, sid);
        }
    }
    return true;
}

bool
DSSNService::sendDSSNInfo(__uint128_t cts, uint8_t txState, uint64_t pStamp, uint64_t sStamp, uint64_t target)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);

    WireFormat::DSSNSendInfoAsync::Request req;
    req.senderPeerId = getServerId();

    req.cts = cts;
    req.pstamp = pStamp;
    req.sstamp = sStamp;
    req.txState = txState;

    char *msg = reinterpret_cast<char *>(&req) + sizeof(WireFormat::Notification::Request);
    uint32_t length = sizeof(req) - sizeof(WireFormat::Notification::Request);
    ServerId sid(target);
    Notifier::notify(context, WireFormat::DSSN_SEND_INFO_ASYNC,
            msg, length, sid);

    return true;
}

bool
DSSNService::requestDSSNInfo(TxEntry *txEntry, bool isSpecific, uint64_t target)
{
    Metric* m = mMonitor->getOpMetric(DSSNServiceRequestDSSNInfo);
    OpTrace t(m);
    WireFormat::DSSNRequestInfoAsync::Request req;
    req.cts = txEntry->getCTS();
    req.pstamp = txEntry->getPStamp();
    req.sstamp = txEntry->getSStamp();;
    req.senderPeerId = getServerId();
    req.txState = txEntry->getTxState();

    char *msg = reinterpret_cast<char *>(&req) + sizeof(WireFormat::Notification::Request);
    uint32_t length = sizeof(req) - sizeof(WireFormat::Notification::Request);
    if (isSpecific) {
        ServerId sid(target);
        assert(target != getServerId());
        Notifier::notify(context, WireFormat::DSSN_REQUEST_INFO_ASYNC,
                msg, length, sid);
        RAMCLOUD_LOG(NOTICE, "notify cts %lu to peer %lu", (uint64_t)(txEntry->getCTS() >> 64), target);
    } else {
        std::set<uint64_t>::iterator it;
        for (it = txEntry->getPeerSet().begin(); it != txEntry->getPeerSet().end(); it++) {
            ServerId sid(*it);
            Notifier::notify(context, WireFormat::DSSN_REQUEST_INFO_ASYNC,
                    msg, length, sid);
        }
    }
    return true;
}

void
DSSNService::handleSendInfoAsync(Rpc* rpc)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);

    assert(rpc->replyPayload->size() == 0);
    WireFormat::DSSNSendInfoAsync::Request* reqHdr =
            rpc->requestPayload->getStart<WireFormat::DSSNSendInfoAsync::Request>();
    if (reqHdr == NULL)
        throw MessageTooShortError(HERE);
    assert(reqHdr->senderPeerId != getServerId());
    uint64_t myPStamp, mySStamp;
    uint32_t myTxState;
    validator->receiveSSNInfo(reqHdr->senderPeerId, reqHdr->cts,
            reqHdr->pstamp, reqHdr->sstamp, reqHdr->txState,
            myPStamp, mySStamp, myTxState);
}

void
DSSNService::handleRequestInfoAsync(Rpc* rpc)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);

    assert(rpc->replyPayload->size() == 0);
    WireFormat::DSSNRequestInfoAsync::Request* reqHdr =
            rpc->requestPayload->getStart<WireFormat::DSSNRequestInfoAsync::Request>();
    if (reqHdr == NULL)
        throw MessageTooShortError(HERE);
    validator->replySSNInfo(reqHdr->senderPeerId, reqHdr->cts, reqHdr->pstamp, reqHdr->sstamp, reqHdr->txState);
}

void
DSSNService::recordTxCommitDispatch(TxEntry *txEntry)
{
#ifdef MONITOR
    RpcHandle *handle = static_cast<RpcHandle *>(txEntry->getRpcHandle());
    Transport::ServerRpc* rpc = handle->getServerRpc();
    rpc->endRpcProcessingQueueTimer();
#endif
}

} //end DSSNService class
