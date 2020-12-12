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

/* Copyright (c) 2011 Facebook
 * Copyright (c) 2011-2016 Stanford University
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

#include "CacheTrace.h"
#include "Context.h"
#include "Cycles.h"
#include "CoordinatorServerList.h"
#include "CoordinatorSession.h"
#include "Dispatch.h"
#include "DispatchExec.h"
#include "ObjectFinder.h"
#include "PortAlarm.h"
#include "ShortMacros.h"
#include "SessionAlarm.h"
#include "TableManager.h"
#include "TransportManager.h"
#include "WorkerManager.h"
#include "ClusterTimeService.h"

namespace RAMCloud {

/**
 * Used by the unit tests to signal to MockContextMember's constructor that it
 * should throw an exception.
 */
int mockContextMemberThrowException = 0;

/**
 * This is a member of the Context that is used for testing purposes only.
 */
class MockContextMember {
  PUBLIC:
    explicit MockContextMember(int id)
        : id(id)
    {
        TEST_LOG("%d", id);
        if (mockContextMemberThrowException == id) {
            mockContextMemberThrowException = 0;
            throw Exception(HERE, format(
                "Mock context member %d asked to throw exception", id).c_str());
        }
    }
    ~MockContextMember() {
        TEST_LOG("%d", id);
    }
    int id;
};

/**
 * Create a new context.
 * This should be called when creating a RamCloud instance and in the main
 * function of RAMCloud daemons.
 * \param hasDedicatedDispatchThread
 *      Argument passed on to Dispatch's constructor.
 * \param options
 *      Holds the value of options provided by the user on the command line;
 *      may be NULL to indicate that no options are available. The caller
 *      must ensure that the lifetime of the options covers the lifetime
 *      of this Context.
 */
Context::Context(bool hasDedicatedDispatchThread,
        CommandLineOptions* options)
    : mockContextMember1(NULL)
    , dispatch(NULL)
    , mockContextMember2(NULL)
    , transportManager(NULL)
    , dispatchExec(NULL)
    , sessionAlarmTimer(NULL)
    , portAlarmTimer(NULL)
    , coordinatorSession(NULL)
    , cacheTrace(NULL)
    , objectFinder(NULL)
    , options(options)
    , workerManager(NULL)
    , externalStorage(NULL)
    , serverList(NULL)
    , coordinatorServerList(NULL)
    , tableManager(NULL)
    , recoveryManager(NULL)
#if HOMA_BENCHMARK
    , masterZeroCopyRegion(NULL)
#else
    , masterZeroCopyRegion(Memory::xmalloc(HERE, Transport::MAX_RPC_LEN))
#endif
    , metricExposer(NULL)
{
    try {
        Cycles::init();
#if TESTING
        mockContextMember1 = new MockContextMember(1);
#endif
        cacheTrace = new CacheTrace();
        objectFinder = new ObjectFinder(this);
        dispatch = new Dispatch(hasDedicatedDispatchThread);
#if TESTING
        mockContextMember2 = new MockContextMember(2);
#endif
        transportManager = new TransportManager(this);
        dispatchExec = new DispatchExec(dispatch);
        sessionAlarmTimer = new SessionAlarmTimer(this);
        portAlarmTimer = new PortAlarmTimer(this);

        // Until we find the solution to prevent active ports
        // which have just nothing to send, we disable portAlarm
        // portAlarmTimer = new PortAlarmTimer(this);

        coordinatorSession = new CoordinatorSession(this);
	ctsClock = new QDB::ClusterTimeService();

        for (int i = 0; i < WireFormat::INVALID_SERVICE; i++) {
            services[i] = NULL;
        }
    } catch (...) {
        destroy();
        throw;
    }
}

/**
 * Destructor; just calls #destroy().
 */
Context::~Context()
{
    destroy();
}

/**
 * A helper function that is essentially the destructor. This is also called by
 * the constructor if an exception is thrown, in which case some of the members
 * may not yet be constructed.
 */
void
Context::destroy()
{
    // The pointers are set to NULL here after they're deleted to make it
    // easier to catch bugs in which outer members try to access inner members.
    // Note: the order of deletion matters!

    // Force ObjectManager to drop all of its cached sessions; otherwise
    // they won't get destroyed until after their transports have been deleted.
    if (objectFinder)
        objectFinder->reset();
    delete objectFinder;
    objectFinder = NULL;

    delete coordinatorSession;
    coordinatorSession = NULL;

    delete workerManager;
    workerManager = NULL;

    delete transportManager;
    transportManager = NULL;

#if TESTING
    delete mockContextMember2;
    mockContextMember2 = NULL;
#endif

    delete dispatch;
    dispatch = NULL;

    delete dispatchExec;
    dispatchExec = NULL;

    delete cacheTrace;
    cacheTrace = NULL;

#if TESTING
    delete mockContextMember1;
    mockContextMember1 = NULL;
#endif

    serverList = NULL;
    coordinatorServerList = NULL;

    tableManager = NULL;

#if !HOMA_BENCHMARK
    free(const_cast<void*>(masterZeroCopyRegion));
    masterZeroCopyRegion = NULL;
#endif
}

} // namespace RAMCloud
