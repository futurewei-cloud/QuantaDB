/* Copyright (c) 2020 Futurewei Technologies, Inc.
 *
 * All rights reserved.
 */

#include "DSSNService.h"
#include "WireFormat.h"
#include "MasterService.h"  //TODO: Remove

namespace DSSN {

DSSNService::DSSNService(Context* context, ServerList* serverList,
			 const ServerConfig* serverConfig)
    : context(context)
    , serverList(serverList)
    , serverConfig(serverConfig)
	, validator(kvStore)
{
    context->services[WireFormat::DSSN_SERVICE] = this;
}

DSSNService::~DSSNService()
{
    context->services[WireFormat::DSSN_SERVICE] = NULL;
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
        rpc->sendReply();
        return;
    }

    uint64_t tableId = reqHdr->tableId;
    KLayout k(reqHdr->keyLength + sizeof(tableId)); //make room composite key in KVStore
    std::memcpy(k.key.get(), &tableId, sizeof(tableId));
    std::memcpy(k.key.get() + sizeof(tableId), stringKey,  reqHdr->keyLength);

    KVLayout *kv = kvStore.fetch(k);
    if (!kv) {
        respHdr->common.status = RAMCloud::STATUS_OBJECT_DOESNT_EXIST;
        return;
    }

    Buffer buffer;
    uint32_t initialLength = rpc->replyPayload->size();

    uint8_t* p = static_cast<uint8_t*>(buffer.alloc(kv->getVLayout()->valueLength));
    std::memcpy(p, kv->getVLayout()->valuePtr, kv->getVLayout()->valueLength);

    Object object(tableId, 0, 0, buffer);
    object.appendValueToBuffer(rpc->replyPayload);

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

    KVLayout *kv = kvStore.fetch(k);
    if (!kv) {
        respHdr->common.status = RAMCloud::STATUS_OBJECT_DOESNT_EXIST;
        return;
    }

    Buffer buffer;
    uint32_t initialLength = rpc->replyPayload->size();

    uint8_t* p = static_cast<uint8_t*>(buffer.alloc(kv->getVLayout()->valueLength));
    std::memcpy(p, kv->getVLayout()->valuePtr, kv->getVLayout()->valueLength);

    Object object(tableId, 0, 0, buffer);
    object.appendKeysAndValueToBuffer(*(rpc->replyPayload));

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
    RAMCloud::MasterService *s = (RAMCloud::MasterService *)context->services[WireFormat::MASTER_SERVICE];
    s->multiRead(reqHdr, respHdr, rpc);
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
    RAMCloud::MasterService *s = (RAMCloud::MasterService *)context->services[WireFormat::MASTER_SERVICE];
    s->multiWrite(reqHdr, respHdr, rpc);
}
  
void
DSSNService::remove(const WireFormat::RemoveDSSN::Request* reqHdr,
		    WireFormat::RemoveDSSN::Response* respHdr,
		    Rpc* rpc)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);
    RAMCloud::MasterService *s = (RAMCloud::MasterService *)context->services[WireFormat::MASTER_SERVICE];
    s->remove(reqHdr, respHdr, rpc);
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
    const void* pVal = object.getKeysAndValue();
    uint32_t pValLen = object.getKeysAndValueLength();

    uint64_t tableId = object.getTableId();
    KVLayout pkv(pKeyLen + sizeof(tableId)); //make room composite key in KVStore
    std::memcpy(pkv.getKey(), &tableId, sizeof(tableId));
    std::memcpy(pkv.getKey() + sizeof(tableId), pKey, pKeyLen);
    pkv.v.valueLength = pValLen;
    pkv.v.valuePtr = (uint8_t*)const_cast<void*>(pVal);

    KVLayout *kv = kvStore.fetch(pkv.k);

    if (kv == NULL) {
        KVLayout *nkv = kvStore.preput(pkv);
        kvStore.putNew(nkv, 0, 0xffffffffffffffff);
    } else {
        void * pval = new char[pValLen];
        std::memcpy(pval, pVal, pValLen);
        kvStore.put(kv, 0, 0xffffffffffffffff, (uint8_t*)pval, pValLen);
    }
}
  
void
DSSNService::takeTabletOwnership(const WireFormat::TakeTabletOwnershipDSSN::Request* reqHdr,
				 WireFormat::TakeTabletOwnershipDSSN::Response* respHdr,
				 Rpc* rpc)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);
    RAMCloud::MasterService *s = (RAMCloud::MasterService *)context->services[WireFormat::MASTER_SERVICE];
    s->takeTabletOwnership(reqHdr, respHdr, rpc);
}

void
DSSNService::txCommit(const WireFormat::TxCommitDSSN::Request* reqHdr,
		      WireFormat::TxCommitDSSN::Response* respHdr,
		      Rpc* rpc)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);
    RAMCloud::MasterService *s = (RAMCloud::MasterService *)context->services[WireFormat::MASTER_SERVICE];
    s->txPrepare(reqHdr, respHdr, rpc);
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
