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

#ifndef DSSNSERVICE_H
#define DSSNSERVICE_H

#include "AdminService.h"
#include "DSSNServiceMonitor.h"
#include "Service.h"
#include "ServerConfig.h"
#include "ServerList.h"
#include "Validator.h"
#include "TabletManager.h"
#include "Notifier.h"

namespace QDB {
using namespace RAMCloud;

class Validator; //forward declaration to resolve interdependency

class DSSNService : public Service {
 public:
   explicit DSSNService(Context* context, ServerList* serverList,
		       const ServerConfig* serverConfig);
   ~DSSNService();
   void dispatch(WireFormat::Opcode opcode, Rpc* rpc);

   bool sendTxCommitReply(TxEntry *txEntry);

   bool sendDSSNInfo(__uint128_t cts, TxEntry *txEntry, bool isSpecific = false, uint64_t target = 0);
   bool sendDSSNInfo(__uint128_t cts, uint8_t txState, uint64_t pStamp, uint64_t sStamp, uint64_t target);
   void recordTxCommitDispatch(TxEntry *txEntry);
   bool requestDSSNInfo(TxEntry *txEntry, bool isSpecific = false, uint64_t target = 0);

   const std::string& getServerAddress() {
       static ServiceLocator sl(serverConfig->localLocator);
       return sl.getOption("host");
   }
 private:
   inline uint64_t getServerId() {
       AdminService* admin = context->getAdminService();
       if (admin) {
	   return admin->serverId.getId();
       }
       return 0;
   }
   void multiOp(const WireFormat::MultiOpDSSN::Request* reqHdr,
                WireFormat::MultiOpDSSN::Response* respHdr,
                Rpc* rpc);
   void multiIncrement(const WireFormat::MultiOp::Request* reqHdr,
                WireFormat::MultiOp::Response* respHdr,
                Rpc* rpc);
   void multiRead(const WireFormat::MultiOp::Request* reqHdr,
                WireFormat::MultiOp::Response* respHdr,
                Rpc* rpc);
   void multiRemove(const WireFormat::MultiOp::Request* reqHdr,
                WireFormat::MultiOp::Response* respHdr,
                Rpc* rpc);
   void multiWrite(const WireFormat::MultiOp::Request* reqHdr,
                WireFormat::MultiOp::Response* respHdr,
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
   void write(const WireFormat::WriteDSSN::Request* reqHdr,
	      WireFormat::WriteDSSN::Response* respHdr,
	      Rpc* rpc);
   void takeTabletOwnership(const WireFormat::TakeTabletOwnershipDSSN::Request* reqHdr,
			    WireFormat::TakeTabletOwnershipDSSN::Response* respHdr,
			    Rpc* rpc);
   void txCommit(const WireFormat::TxCommitDSSN::Request* reqHdr,
		 WireFormat::TxCommitDSSN::Response* respHdr,
		 Rpc* rpc);
   void txDecision(  //TODO: remove
		   const WireFormat::TxDecisionDSSN::Request* reqHdr,
		   WireFormat::TxDecisionDSSN::Response* respHdr,
		   Rpc* rpc);
   void handleSendInfoAsync(Rpc* rpc);
   void handleRequestInfoAsync(Rpc* rpc);
   Context* context;
   ServerList* serverList;
   const ServerConfig* serverConfig;
   DISALLOW_COPY_AND_ASSIGN(DSSNService);

   HashmapKVStore* kvStore;
   Validator* validator;
   TabletManager *tabletManager;
   DSSNServiceMonitor *mMonitor;
};


}

#endif /* DSSNSERVICE_H */
