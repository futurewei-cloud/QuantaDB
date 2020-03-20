/* Copyright (c) 2020 Futurewei Technologies, Inc.
 *
 * All rights reserved.
 */

#ifndef DSSNSERVICE_H
#define DSSNSERVICE_H

#include "Service.h"
#include "ServerConfig.h"
#include "ServerList.h"

namespace DSSN {
using namespace RAMCloud;
  
class DSSNService : public Service {
 public:
   explicit DSSNService(Context* context, ServerList* serverList,
		       const ServerConfig* serverConfig);
   ~DSSNService();
   void dispatch(WireFormat::Opcode opcode, Rpc* rpc);
 private:
   void multiOp(const WireFormat::MultiOpDSSN::Request* reqHdr,
                WireFormat::MultiOpDSSN::Response* respHdr,
                Rpc* rpc);
   void read(const WireFormat::ReadDSSN::Request* reqHdr,
	     WireFormat::ReadDSSN::Response* respHdr,
	     Rpc* rpc);
   void readKeysAndValue(const WireFormat::ReadKeysAndValueDSSN::Request* reqHdr,
			 WireFormat::ReadKeysAndValueDSSN::Response* respHdr,
			 Rpc* rpc);
   void remove(const WireFormat::RemoveDSSN::Request* reqHdr,
	       WireFormat::RemoveDSSN::Response* respHdr,
	       Rpc* rpc);
   void takeTabletOwnership(const WireFormat::TakeTabletOwnershipDSSN::Request* reqHdr,
			    WireFormat::TakeTabletOwnershipDSSN::Response* respHdr,
			    Rpc* rpc);
   void txCommit(const WireFormat::TxCommitDSSN::Request* reqHdr,
		 WireFormat::TxCommitDSSN::Response* respHdr,
		 Rpc* rpc);
   Context* context;
   ServerList* serverList;
   const ServerConfig* serverConfig;
   DISALLOW_COPY_AND_ASSIGN(DSSNService);
};


}

#endif /* DSSNSERVICE_H */
