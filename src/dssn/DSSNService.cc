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
    RAMCloud::MasterService *s = (RAMCloud::MasterService *)context->services[WireFormat::MASTER_SERVICE];
    s->read(reqHdr, respHdr, rpc);
}

void
DSSNService::readKeysAndValue(const WireFormat::ReadKeysAndValueDSSN::Request* reqHdr,
			      WireFormat::ReadKeysAndValueDSSN::Response* respHdr,
			      Rpc* rpc)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);
    RAMCloud::MasterService *s = (RAMCloud::MasterService *)context->services[WireFormat::MASTER_SERVICE];
    s->readKeysAndValue(reqHdr, respHdr, rpc);
}

void
DSSNService::multiOp(const WireFormat::MultiOpDSSN::Request* reqHdr,
		     WireFormat::MultiOpDSSN::Response* respHdr,
		     Rpc* rpc)
{
    RAMCLOUD_LOG(NOTICE, "%s", __FUNCTION__);
    RAMCloud::MasterService *s = (RAMCloud::MasterService *)context->services[WireFormat::MASTER_SERVICE];
    s->multiOp(reqHdr, respHdr, rpc);
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
    RAMCloud::MasterService *s = (RAMCloud::MasterService *)context->services[WireFormat::MASTER_SERVICE];
    s->write(reqHdr, respHdr, rpc);
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
