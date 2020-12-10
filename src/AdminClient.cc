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

/* Copyright (c) 2011-2016 Stanford University
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

#include <stdarg.h>

#include "Common.h"
#include "ClientException.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "AdminClient.h"
#include "ShortMacros.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Issue a trivial RPC to test that a server exists and is responsive.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param targetId
 *      Identifies the server to which the RPC should be sent.
 * \param callerId
 *      If this is a valid ServerId, then the callee will check to see
 *      if this id exists in its server list as an active cluster member.
 *      If not, CallerNotInClusterException will be thrown. If this is an
 *      invalid ServerId (default), then no check is made.
 *
 * \return
 *      If \a serverId had a server list, then its version number is returned;
 *      otherwise zero is returned.
 *
 * \throw ServerNotUpException
 *      The intended server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 * \throw CallerNotInClusterException
 *      CallerId was specified, but the target server doesn't think
 *      it is part of the cluster anymore.
 */
void
AdminClient::ping(Context* context, ServerId targetId, ServerId callerId)
{
    PingRpc rpc(context, targetId, callerId);
    rpc.wait();
}

/**
 * Constructor for PingRpc: initiates an RPC in the same way as
 * #AdminClient::ping, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param targetId
 *      Identifies the server to which the RPC should be sent.
 * \param callerId
 *      If this is a valid ServerId, then the callee will check to see
 *      if this id exists in its server list as an active cluster member.
 *      If not, CallerNotInClusterException will be thrown. If this is an
 *      invalid ServerId (default), then no check is made.
 */
PingRpc::PingRpc(Context* context, ServerId targetId, ServerId callerId)
    : ServerIdRpcWrapper(context, targetId,
            sizeof(WireFormat::Ping::Response))
{
    WireFormat::Ping::Request* reqHdr(
            allocHeader<WireFormat::Ping>());
    reqHdr->callerId = callerId.getId();
    send();
}

/**
 * Wait for a ping RPC to complete, but only wait for a given amount of
 * time, and return if no response is received by then.
 *
 * \param timeoutNanoseconds
 *      If no response is received within this many nanoseconds, then
 *      give up.
 *
 * \return
 *      True is returned if a response was received within
 *      \c timeoutNanoseconds; otherwise, false is returned.
 *
 * \throw ServerNotUpException
 *      The target server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 * \throw CallerNotInClusterException
 *      CallerId was specified, but the target server doesn't think
 *      it is part of the cluster anymore.
 */
bool
PingRpc::wait(uint64_t timeoutNanoseconds)
{
    uint64_t abortTime = Cycles::rdtsc() +
            Cycles::fromNanoseconds(timeoutNanoseconds);
    if (!waitInternal(context->dispatch, abortTime)) {
        TEST_LOG("timeout");
        return false;
    }
    if (serverCrashed) {
        TEST_LOG("server doesn't exist");
        return false;
    }
    if (responseHeader->status != STATUS_OK)
        ClientException::throwException(HERE, responseHeader->status);
    WireFormat::Ping::Response* rsp = static_cast<WireFormat::Ping::Response *>(response->getRange(0,
					  sizeof(WireFormat::Ping::Response)));
    remoteServerTime = rsp->currentTime;
    return true;
}

/**
 * Ask one service to ping another service (useful for checking possible
 * connectivity issues).
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param proxyId
 *      Identifies the server to which the RPC should be sent; this server
 *      will ping \a targetId.
 * \param targetId
 *      Identifies the server that \a proxyId will ping.
 * \param timeoutNanoseconds
 *      The maximum amount of time (in nanoseconds) that \a proxyId
 *      will wait for \a targetId to respond.
 *
 * \result
 *      The amount of time it took \a targetId to respond to the ping
 *      request from \a proxyId.  If no response was received within
 *      \a timeoutNanoseconds, then all ones is returned.
 *
 * \throw ServerNotUpException
 *      Generated if \a proxyId is not part of the cluster; if it ever
 *      existed, it has since crashed.
 */
uint64_t
AdminClient::proxyPing(Context* context, ServerId proxyId, ServerId targetId,
        uint64_t timeoutNanoseconds)
{
    ProxyPingRpc rpc(context, proxyId, targetId, timeoutNanoseconds);
    return rpc.wait();
}

/**
 * Constructor for ProxyPingRpc: initiates an RPC in the same way as
 * #AdminClient::proxyPing, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param proxyId
 *      Identifies the server to which the RPC should be sent; this server
 *      will ping \a targetId.
 * \param targetId
 *      Identifies the server that \a proxyId will ping.
 * \param timeoutNanoseconds
 *      The maximum amount of time (in nanoseconds) that \a proxyId
 *      will wait for \a targetId to respond.
 */
ProxyPingRpc::ProxyPingRpc(Context* context, ServerId proxyId,
        ServerId targetId, uint64_t timeoutNanoseconds)
    : ServerIdRpcWrapper(context, proxyId,
            sizeof(WireFormat::ProxyPing::Response))
{
    WireFormat::ProxyPing::Request* reqHdr(
            allocHeader<WireFormat::ProxyPing>());
    reqHdr->serverId = targetId.getId();
    reqHdr->timeoutNanoseconds = timeoutNanoseconds;
    send();
}

/**
 * Wait for a proxyPing RPC to complete.
 *
 * \return
 *      The amount of time it took the target server to respond to the ping
 *      request.  If the proxy didn't receive a response within the timeout
 *      period, then all ones is returned.
 *
 * \throw ServerNotUpException
 *      The target server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
uint64_t
ProxyPingRpc::wait()
{
    waitAndCheckErrors();
    const WireFormat::ProxyPing::Response* respHdr(
            getResponseHeader<WireFormat::ProxyPing>());
    return respHdr->replyNanoseconds;
}


/**
 * This RPC is used to invoke a variety of miscellaneous operations on a server,
 * such as starting and stopping special timing mechanisms, dumping metrics, and
 * so on. Most of these operations are used only for testing. Each operation is
 * defined by a specific opcode (controlOp) and an arbitrary chunk of input
 * data. Not all operations require input data, and different operations use the
 * input data in different ways. Each operation can also return an optional
 * result of arbitrary size.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the master to be controlled.
 * \param controlOp
 *      This defines the specific operation to be performed on the
 *      remote server.
 * \param inputData
 *      Input data, such as additional parameters, specific for the
 *      particular operation to be performed. Not all operations use
 *      this information.
 * \param inputLength
 *      Size in bytes of the contents for the inputData.
 * \param[out] outputData
 *      A buffer that contains the return results, if any, from execution of the
 *      control operation on the remote server.
 */
void
AdminClient::serverControl(Context* context, ServerId serverId,
        WireFormat::ControlOp controlOp, const void* inputData,
        uint32_t inputLength, Buffer* outputData)
{
    ServerControlRpc rpc(context, serverId, controlOp,
                         inputData, inputLength,
                         outputData);
    rpc.wait();
}

/**
 * Constructor for ServerControlRpc: initiates an RPC in the same way as
 * #AdminClient::serverControl, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the master to be controlled.
 * \param controlOp
 *      This defines the specific operation to be performed on the
 *      remote server.
 * \param inputData
 *      Input data, such as additional parameters, specific for the
 *      particular operation to be performed. Not all operations use
 *      this information.
 * \param inputLength
 *      Size in bytes of the contents for the inputData.
 * \param[out] outputData
 *      A buffer that contains the return results, if any, from execution of the
 *      control operation on the remote server.
 */
ServerControlRpc::ServerControlRpc(Context* context, ServerId serverId,
        WireFormat::ControlOp controlOp, const void* inputData,
        uint32_t inputLength, Buffer* outputData)
    : ServerIdRpcWrapper(context, serverId,
        sizeof(WireFormat::ServerControl::Response), outputData)
{
    if (outputData) outputData->reset();
    WireFormat::ServerControl::Request*
                        reqHdr(allocHeader<WireFormat::ServerControl>());

    reqHdr->type = WireFormat::ServerControl::SERVER_ID;
    reqHdr->controlOp = controlOp;

    reqHdr->keyLength = 0;
    reqHdr->inputLength = inputLength;
    request.append(inputData, inputLength);
    send();
}

/**
 * Wait for a serverControl RPC to complete.
 *
 * \throw ServerNotUpException
 *      The target server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
void
ServerControlRpc::wait()
{
    waitAndCheckErrors();
    const WireFormat::ServerControl::Response* respHdr(
            getResponseHeader<WireFormat::ServerControl>());
    // Truncate the response Buffer so that it consists of nothing
    // but the object data.
    response->truncateFront(sizeof(*respHdr));
    assert(respHdr->outputLength == response->size());
}

/**
 * The standard wait method truncates the header from the response buffer.
 * This waitRaw method won't perform the truncation, leaving the response
 * buffer unmodified (raw).
 *
 * Called by CoordinatorService::serverControlAll.
 *
 * \return
 *      Method will return false if the rpc does not have a response because
 *      the server has crashed. Returns true, otherwise.
 */
bool
ServerControlRpc::waitRaw()
{
    waitInternal(context->dispatch);
    return !serverCrashed;
}

/**
 * Wrapper function to send a LOG_MESSAGE server control with a format string.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the master to be controlled.
 * \param level
 *      Which LogLevel the message should be added to.
 * \param[in] fmt
 *      A printf-style format string for the message.
 * \param[in] ...
 *      The arguments to the format string.
 */
void
AdminClient::logMessage(Context* context, ServerId serverId,
        LogLevel level, const char* fmt, ...)
{
    Buffer toSend;
    Buffer outputData;

    va_list args;
    va_start(args, fmt);
    string toAdd = vformat(fmt, args);
    va_end(args);

    toSend.emplaceAppend<LogLevel>(level);
    toSend.appendCopy(toAdd.data(), (uint32_t) toAdd.size());

    ServerControlRpc rpc(context, serverId, WireFormat::LOG_MESSAGE,
            toSend.getStart<char>(), toSend.size(), &outputData);
    rpc.wait();
}

/**
 * Retrieves the id of the server at the other end of a given session.
 * This method is used primarily by AbstractServerList when opening a
 * connection to a particular server id; it is intended to detect situations
 * where a new incarnation of a server (with a new ServerId) uses the same
 * service locator as its predecessor.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param session
 *      Open connection to another server.
 *
 * \result
 *      The ServerId returned by the server. If the server has not yet
 *      completed its enlistment, then it doesn't have an id so it returns
 *      an invalid id.
 *
 * \throw TransportException
 *      Thrown if there was a transport-level error that prevented us from
 *      communicating with the server.
 */
ServerId
AdminClient::getServerId(Context* context, Transport::SessionRef session)
{
    GetServerIdRpc rpc(context, session);
    return rpc.wait();
}

/**
 * Constructor for GetServerIdRpc: initiates an RPC and returns once the
 * RPC has been initiated, without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param session
 *      Session over which the RPC should be sent.
 */
GetServerIdRpc::GetServerIdRpc(Context* context, Transport::SessionRef session)
    : RpcWrapper(sizeof(WireFormat::GetServerId::Response))
    , context(context)
{
    allocHeader<WireFormat::GetServerId>();
    this->session = session;
    send();
}

/**
 * Wait for a getServerId RPC to complete.
 *
 * \result
 *      The ServerId returned by the server. If the server has not yet
 *      completed its enlistment, then it doesn't have an id so it returns
 *      an invalid id.
 *
 * \throw TransportException
 *      Thrown if there was a transport-level error that prevented us from
 *      communicating with the server.
 */
ServerId
GetServerIdRpc::wait()
{
    waitInternal(context->dispatch);
    if (getState() != FINISHED) {
        throw TransportException(HERE, "getServerId RPC failed");
    }
    const WireFormat::GetServerId::Response* respHdr(
            getResponseHeader<WireFormat::GetServerId>());
    return ServerId(respHdr->serverId);
}

}  // namespace RAMCloud
