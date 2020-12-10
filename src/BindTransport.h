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

/* Copyright (c) 2010-2015 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "Common.h"
#include "BitOps.h"
#include "RpcLevel.h"
#include "Service.h"
#include "ServerRpcPool.h"
#include "TransportManager.h"
#include "WorkerManager.h"

#ifndef RAMCLOUD_BINDTRANSPORT_H
#define RAMCLOUD_BINDTRANSPORT_H

namespace RAMCloud {

/**
 * This class defines an implementation of Transport that allows unit
 * tests to run without a network or a remote counterpart (it injects RPCs
 * directly into a Service instance's #dispatch() method).
 */
struct BindTransport : public Transport {
    /**
     * The following class mirrors the ServerRpc class defined in other
     * transports, and allows unit testing of functions that expect a subclass
     * of Transport::ServerRpc to be available, such as
     * MasterService::migrateTablet.
     */
    class ServerRpc : public Transport::ServerRpc {
        public:
            ServerRpc() { sentReply = false; }
            void sendReply() { sentReply = true; } 
	    bool isReplySent() { return sentReply; }
            string getClientServiceLocator() {return std::string();}
	    void clear() {
                sentReply = false;
	        requestPayload.reset();
	        replyPayload.reset();
	    }
        private:
	    bool sentReply;
            DISALLOW_COPY_AND_ASSIGN(ServerRpc);
    };

    explicit BindTransport(Context* context)
        : context(context), servers(), abortCounter(0), errorMessage(),
          cRpcProcCpuCycle{0}, totalRpcs{0}, serverRpcPool()
    { }

    string
    getServiceLocator() {
        return "mock:";
    }

    /**
     * Make a collection of services available through this transport.
     *
     * \param context
     *      Defines one or more services (those in context->services).
     * \param locator
     *      Locator to associate with the services: open a session with
     *      this locator, and RPCs will find their way to the services in
     *      context->services.
     */
    void registerServer(Context* context, const string locator) {
        servers[locator] = context;
    }

    Transport::SessionRef
    getSession(const ServiceLocator* serviceLocator, uint32_t timeoutMs = 0) {
        const string& locator = serviceLocator->getOriginalString();
        ServerMap::iterator it = servers.find(locator);
        if (it == servers.end()) {
            throw TransportException(HERE, format("Unknown mock host: %s",
                                                  locator.c_str()));
        }
        return new BindSession(*this, it->second, locator);
    }

    Transport::SessionRef
    getSession() {
        return context->transportManager->getSession("mock:");
    }

    struct BindServerRpc : public ServerRpc {
        BindServerRpc() {}
        void sendReply() {}
        DISALLOW_COPY_AND_ASSIGN(BindServerRpc);
    };

    struct BindSession : public Session {
      public:
        explicit BindSession(BindTransport& transport, Context* context,
                             const string& locator)
            : Session(locator), transport(transport), context(context),
            lastRequest(NULL), lastResponse(NULL), lastNotifier(NULL),
            dontNotify(false)
        {}

        void abort() {}
        void cancelRequest(RpcNotifier* notifier) {}
        string getRpcInfo()
        {
            if (lastNotifier == NULL)
                return "no active RPCs via BindTransport";
            return format("%s via BindTransport",
                    WireFormat::opcodeSymbol(lastRequest));
        }
        void sendRequest(Buffer* request, Buffer* response,
                         RpcNotifier* notifier)
        {
	    if (notifier->requiredRsp()){
	        response->reset();
	    }
            lastRequest = request;
            lastResponse = response;
            lastNotifier = notifier;

            // The worker and ServerRpc are included to more fully simulate a
            // real call to Service method, since they are public members of
            // their respective classes.
            ServerRpc* serverRpc = transport.serverRpcPool.construct();
	    RpcHandle RpcHandle(serverRpc);
            ServerRpcPoolGuard<ServerRpc> serverRpcKiller(
                    transport.serverRpcPool, serverRpc);
            Worker w(NULL);
	    serverRpc->clear();
            w.rpc = &RpcHandle;
	    const WireFormat::RequestCommon* header = request->getStart<WireFormat::RequestCommon>();
	    uint32_t opcode = WireFormat::ILLEGAL_RPC_TYPE;
	    if (header && header->opcode < WireFormat::ILLEGAL_RPC_TYPE) {
	        opcode = header->opcode;
	    }
	    // Transfer the request buffer to serverRpc
	    Buffer* req = &serverRpc->requestPayload;
	    Buffer* rsp = &serverRpc->replyPayload;
	    req->appendCopy(request->getRange(0, request->size()),
			   request->size());

            Service::Rpc rpc(&w, req, rsp);

            if (transport.abortCounter > 0) {
                transport.abortCounter--;
                if (transport.abortCounter == 0) {
                    // Simulate a failure of the server to respond.
                    notifier->failed();
                    return;
                }
            }
            if (transport.errorMessage != "") {
                notifier->failed();
                transport.errorMessage = "";
                return;
            }
	    uint64_t start = Cycles::rdtsc();
            Service::handleRpc(context, &rpc);
	    //For async processing, wait for processing function to generate reply
	    if (RpcHandle.isAsyncEnabled()) {
	        while (!serverRpc->isReplySent());
	    }
	    transport.cRpcProcCpuCycle[opcode] += Cycles::rdtsc() - start;
	    transport.totalRpcs[opcode]++;
	    // Transfer from the serverRpc buffer to app buffer
	    response->appendCopy(rsp->getRange(0, rsp->size()),
				rsp->size());
            if (!dontNotify) {
                notifier->completed();
                lastNotifier = NULL;
            }
        }
	void clearStats()
	{
	    for (uint32_t i = 0; i < WireFormat::totalOps; i++)
	    {
	        transport.cRpcProcCpuCycle[i] = 0;
		transport.totalRpcs[i] = 0;
	    }
	}

	void dumpStats()
	{
	    printf("=============Rpc Processing Latency================\n");
	    printf("%-32s    %-9s  %s\n", "Type", "latency(usec)", "count");
	    for (uint32_t i = 0; i < WireFormat::totalOps; i++)
	    {
	        if (transport.totalRpcs[i]) {
		    printf("%-32s : %7.2f         %-8lu\n", WireFormat::opcodeSymbol(i), static_cast<double>(Cycles::toNanoseconds(transport.cRpcProcCpuCycle[i]/transport.totalRpcs[i]))/1000, transport.totalRpcs[i]); 
		}
	    }
	}
        BindTransport& transport;

        // Context to use for dispatching RPCs sent to this session.
        Context* context;

        // The request and response buffers from the last call to sendRequest
        // for this session.
        Buffer *lastRequest, *lastResponse;

        // Notifier from the last call to sendRequest, if that call hasn't
        // yet been responded to.
        RpcNotifier *lastNotifier;

        // If the following variable is set to true by testing code, then
        // sendRequest does not immediately signal completion of the RPC.
        // It does complete the RPC, but returns without calling the
        // notifier, leaving it to testing code to invoke the notifier
        // explicitly to complete the call (the testing code can also
        // modify the response).
        bool dontNotify;

        DISALLOW_COPY_AND_ASSIGN(BindSession);
    };

    // Shared RAMCloud information.
    Context* context;

    // Maps from a service locator to a Context corresponding to a
    // server, which can be used to dispatch RPCs to that server.
    typedef std::map<const string, Context*> ServerMap;
    ServerMap servers;

    // The following value is used to simulate server timeouts.
    int abortCounter;

    /**
     * If this is set to a non-empty value then the next RPC will
     * fail immediately.
     */
    string errorMessage;
    /**
     * The following counters track the RPC processing latency for a given message type
     *
     */
    uint64_t cRpcProcCpuCycle[WireFormat::totalOps];
    uint64_t totalRpcs[WireFormat::totalOps];
    /**
     * This is used to create mock subclasses of Transport::ServerRpc.
     */
    ServerRpcPool<ServerRpc> serverRpcPool;

    DISALLOW_COPY_AND_ASSIGN(BindTransport);
};

}  // namespace RAMCloud

#endif
