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

/* Copyright (c) 2010-2016 Stanford University
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
#include "MockService.h"
#include "MockTransport.h"
#include "RawMetrics.h"
#include "Service.h"
#include "WorkerManager.h"

namespace RAMCloud {

class ServiceTest : public ::testing::Test {
  public:
    Context context;
    Service service;
    Buffer request, response;
    Worker worker;
    Service::Rpc rpc;
    TestLog::Enable logEnabler;

    ServiceTest()
        : context()
        , service()
        , request()
        , response()
        , worker(&context)
        , rpc(&worker, &request, &response)
        , logEnabler()
    {
        context.services[1] = &service;
    }

    ~ServiceTest()
    {
    }
};

TEST_F(ServiceTest, getString_basics) {
    Buffer buffer;
    buffer.fillFromString("abcdefg");
    const char* result = Service::getString(&buffer, 3, 5);
    EXPECT_STREQ("defg", result);
}
TEST_F(ServiceTest, getString_lengthZero) {
    Buffer buffer;
    Status status = Status(0);
    try {
        Service::getString(&buffer, 0, 0);
    } catch (RequestFormatError& e) {
        status = e.status;
    }
    EXPECT_EQ(9, status);
}
TEST_F(ServiceTest, getString_bufferTooShort) {
    Buffer buffer;
    buffer.fillFromString("abcde");
    Status status = Status(0);
    try {
        Service::getString(&buffer, 2, 5);
    } catch (MessageTooShortError& e) {
        status = e.status;
    }
    EXPECT_EQ(7, status);
}
TEST_F(ServiceTest, getString_stringNotTerminated) {
    Buffer buffer;
    buffer.fillFromString("abcde");
    Status status = Status(0);
    try {
        Service::getString(&buffer, 1, 3);
    } catch (RequestFormatError& e) {
        status = e.status;
    }
    EXPECT_EQ(9, status);
}

TEST_F(ServiceTest, dispatch_ping) {
    request.fillFromString("7 0 0 0 0 0");
    service.dispatch(WireFormat::Ping::opcode, &rpc);
    EXPECT_TRUE(TestUtil::matchesPosixRegex("Service::ping invoked",
            TestLog::get()));
}
TEST_F(ServiceTest, dispatch_unknown) {
    request.fillFromString("0 0");
    union {
        WireFormat::Opcode x;
        int y;
    } t;
    t.y = 12345;
    EXPECT_THROW(
        service.dispatch(t.x, &rpc),
        UnimplementedRequestError);
}

TEST_F(ServiceTest, handleRpc_messageTooShortForCommon) {
    request.fillFromString("x");
    Service::handleRpc(&context, &rpc);
    EXPECT_STREQ("STATUS_MESSAGE_TOO_SHORT", TestUtil::getStatus(&response));
}
TEST_F(ServiceTest, handleRpc_undefinedType) {
    metrics->rpc.illegal_rpc_typeCount = 0;
    WireFormat::RequestCommon* header =
            request.emplaceAppend<WireFormat::RequestCommon>();
    header->opcode = WireFormat::ILLEGAL_RPC_TYPE;
    header->service = WireFormat::ServiceType(1);
    Service::handleRpc(&context, &rpc);
    EXPECT_STREQ("STATUS_UNIMPLEMENTED_REQUEST",
            TestUtil::getStatus(&response));
    EXPECT_EQ(1U, metrics->rpc.illegal_rpc_typeCount);
}
TEST_F(ServiceTest, handleRpc_retryException) {
    MockService service;
    context.services[0] = &service;
    request.fillFromString("1 2 54322 3 4");
    Service::handleRpc(&context, &rpc);
    EXPECT_EQ("17 100 200 18 server overloaded/0",
            TestUtil::toString(&response));
}
TEST_F(ServiceTest, handleRpc_retryExceptionAfterSendingReply) {
    MockService service;
    context.services[0] = &service;
    request.fillFromString("1 2 54322 3 4");
    worker.state = Worker::POSTPROCESSING;
    string message = "no exception";
    try {
        Service::handleRpc(&context, &rpc);
    } catch (Exception & e) {
        message = e.message;
    }
    EXPECT_EQ("Retry exception thrown after reply sent for unknown(1) RPC",
            message);
}
TEST_F(ServiceTest, handleRpc_clientException) {
    MockService service;
    context.services[0] = &service;
    request.fillFromString("1 2 54321 3 4");
    Service::handleRpc(&context, &rpc);
    EXPECT_STREQ("STATUS_REQUEST_FORMAT_ERROR", TestUtil::getStatus(&response));
}
TEST_F(ServiceTest, handleRpc_clientExceptionAfterSendingReply) {
    MockService service;
    context.services[0] = &service;
    request.fillFromString("1 2 54321 3 4");
    worker.state = Worker::POSTPROCESSING;
    string message = "no exception";
    try {
        Service::handleRpc(&context, &rpc);
    } catch (Exception & e) {
        message = e.message;
    }
    EXPECT_EQ("STATUS_REQUEST_FORMAT_ERROR exception thrown after reply "
            "sent for unknown(1) RPC",
            message);
}

TEST_F(ServiceTest, prepareErrorResponse_bufferNotEmpty) {
    response.fillFromString("1 abcdef");
    Service::prepareErrorResponse(&response, STATUS_WRONG_VERSION);
    EXPECT_STREQ("STATUS_WRONG_VERSION", TestUtil::getStatus(&response));
    EXPECT_STREQ("abcdef",
            static_cast<const char*>(response.getRange(4, 7)));
}
TEST_F(ServiceTest, prepareErrorResponse_bufferEmpty) {
    Service::prepareErrorResponse(&response, STATUS_WRONG_VERSION);
    EXPECT_EQ(sizeof(WireFormat::ResponseCommon), response.size());
    EXPECT_STREQ("STATUS_WRONG_VERSION", TestUtil::getStatus(&response));
}

TEST_F(ServiceTest, prepareRetryResponse_withMessage) {
    response.fillFromString("abcdef");
    Service::prepareRetryResponse(&response, 1000, 2000, "test message");
    EXPECT_EQ("17 1000 2000 13 test message/0", TestUtil::toString(&response));
}
TEST_F(ServiceTest, prepareRetryResponse_noMessage) {
    response.fillFromString("abcdef");
    Service::prepareRetryResponse(&response, 100, 200, NULL);
    EXPECT_EQ("17 100 200 0", TestUtil::toString(&response));
}

TEST_F(ServiceTest, callHandler_messageTooShort) {
    request.fillFromString("");
    EXPECT_THROW(
        (service.callHandler<WireFormat::Ping, Service, &Service::ping>(&rpc)),
        MessageTooShortError);
}
TEST_F(ServiceTest, callHandler_normal) {
    request.fillFromString("7 0 0 0 0 0");
    service.callHandler<WireFormat::Ping, Service, &Service::ping>(&rpc);
    EXPECT_TRUE(TestUtil::matchesPosixRegex("ping", TestLog::get()));
}

// Fake RPC class for testing checkServerId.
class DummyService : public Service {
  public:
    struct Rpc1 {
        static const WireFormat::Opcode opcode = WireFormat::Opcode(4);
        static const WireFormat::ServiceType service =
                WireFormat::MASTER_SERVICE;
        struct Request {
            WireFormat::RequestCommonWithId common;
        } __attribute__((packed));
        struct Response {
            WireFormat::ResponseCommon common;
        } __attribute__((packed));
    };
    void serviceMethod1(const Rpc1::Request* reqHdr, Rpc1::Response* respHdr,
                Rpc* rpc)
    {
        // No-op.
    }
};

TEST_F(ServiceTest, checkServerId) {
    request.fillFromString("9 1 2");
    DummyService service;

    // First try: service's id is invalid, so mismatch should be ignored.
    string message("no exception");
    try {
        service.callHandler<DummyService::Rpc1, DummyService,
                &DummyService::serviceMethod1>(&rpc);
    } catch (WrongServerException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("no exception", message);

    // Second try: should generate an exception.
    service.serverId = ServerId(1, 3);
    response.reset();
    message = "no exception";
    try {
        service.callHandler<DummyService::Rpc1, DummyService,
                &DummyService::serviceMethod1>(&rpc);
    } catch (WrongServerException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("STATUS_WRONG_SERVER", message);

    // Third try: ids match.
    service.serverId = ServerId(1, 2);
    response.reset();
    message = "no exception";
    try {
        service.callHandler<DummyService::Rpc1, DummyService,
                &DummyService::serviceMethod1>(&rpc);
    } catch (WrongServerException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("no exception", message);

    // Fourth try: serverId in RPC is invalid, so mismatch should be ignored.
    response.reset();
    request.reset();
    request.fillFromString("9 0 -1");
    message = "no exception";
    try {
        service.callHandler<DummyService::Rpc1, DummyService,
                &DummyService::serviceMethod1>(&rpc);
    } catch (WrongServerException& e) {
        message = e.toSymbol();
    }
    EXPECT_EQ("no exception", message);
}

TEST_F(ServiceTest, sendReply) {
    MockService service;
    service.gate = -1;
    service.sendReply = true;
    Context context;
    context.workerManager = new WorkerManager(&context);
    WorkerManager* manager = context.workerManager;
    MockTransport transport(&context);
    context.services[WireFormat::BACKUP_SERVICE] = &service;
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "0x10008 3 4");
    manager->handleRpc(rpc);

    // Verify that the reply has been sent even though the worker has not
    // returned yet.
    for (int i = 0; i < 1000; i++) {
        context.dispatch->poll();
        if (manager->busyThreads[0]->rpc == NULL) {
            break;
        }
        usleep(1000);
    }
    EXPECT_EQ((RpcHandle*) NULL, manager->busyThreads[0]->rpc);
    EXPECT_EQ(Worker::POSTPROCESSING, manager->busyThreads[0]->state.load());
    EXPECT_EQ("serverReply: 0x10009 4 5", transport.outputLog);
    service.gate = 3;
}

}  // namespace RAMCloud
