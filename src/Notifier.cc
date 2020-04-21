/* Copyright (c) 2020 Futurewei Technologies, Inc.
 *
 * All rights reserved.
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
		 const uint32_t length, ServerId id)
{
    NotificationRpc* rpc = NULL;
    Transport::SessionRef s = context->serverList->getSession(id);
    rpc = new NotificationRpc(context, s,
			      type, message,length);
    rpc->wait();
    delete rpc;
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
