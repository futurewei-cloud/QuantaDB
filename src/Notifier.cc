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

#include "Context.h"
#include "CoordinatorServerList.h"
#include "CoordinatorSession.h"
#include "FailSession.h"
#include "Notifier.h"

namespace RAMCloud {

void
Notifier::notify(Context* context,
		 const WireFormat::Opcode type, const void* message,
		 const uint32_t length, ServerId& id)
{
    NotificationRpc* rpc = NULL;
    uint64_t sId = id.getId();
    if (sId == 0) {
        RAMCLOUD_LOG(ERROR, "Invalid participate server id: %lu",
		     sId);
	return;
    }
    if (context->serverList) {
        Transport::SessionRef s = context->serverList->getSession(id);
	if (s == FailSession::get()) {
	    RAMCLOUD_LOG(ERROR, "can't locate participate server id: %lu",
			 id.getId());
	}
	rpc = new NotificationRpc(context, s,
				  type, message,length);
	rpc->wait();
	delete rpc;
    } else {
        RAMCLOUD_LOG(ERROR, "the serverList is not initialized");
    }
}

void
Notifier::notify(Context* context,
		 const WireFormat::Opcode type, const void* message,
		 const uint32_t length, Transport::SessionRef* endpoint,
		 const char* serviceLocator)
{
    NotificationRpc* rpc = NULL;
    if (endpoint != NULL) {
        rpc = new NotificationRpc(context, *endpoint, type, message, length);
    } else if (serviceLocator != NULL) {
        rpc = new NotificationRpc(context, serviceLocator, type, message,
				  length);
    } else {
        assert(!"Service endpoint is required");
    }
    rpc->wait();
    delete rpc;
}

NotificationRpc::NotificationRpc(Context* context, const char* serviceLocator,
				 const WireFormat::Opcode type,
				 const void* message,
				 const uint32_t length)
    : RpcWrapper(0, NULL, false)
    , context(context)
{
    try {
        session = context->transportManager->getSession(serviceLocator);
    } catch (const TransportException& e) {
        session = FailSession::get();
    }

    WireFormat::Notification::Request* reqHdr(allocHeader<WireFormat::Notification>(type));
    reqHdr->length = length;
    request.appendExternal(message, length);
    send();
}

NotificationRpc::NotificationRpc(Context* context,
				 const Transport::SessionRef s,
				 const WireFormat::Opcode type,
				 const void* message,
				 const uint32_t length)
    : RpcWrapper(0, NULL, false)
    , context(context)
{
    WireFormat::Notification::Request* reqHdr(allocHeader<WireFormat::Notification>(type));
    reqHdr->length = length;
    request.appendExternal(message, length);
    session = s;
    send();
}


void
NotificationRpc::wait()
{
    waitInternal(context->dispatch);
}

}
