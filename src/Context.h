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
 * Copyright (c) 2011-2015 Stanford University
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

#ifndef RAMCLOUD_CONTEXT_H
#define RAMCLOUD_CONTEXT_H

#include "Common.h"
#include "WireFormat.h"
#include <prometheus/exposer.h>

namespace QDB {
    class ClusterTimeService;
}

namespace RAMCloud {

// forward declarations
class AbstractServerList;
class AdminService;
class BackupService;
class CacheTrace;
class CommandLineOptions;
class CoordinatorServerList;
class CoordinatorService;
class CoordinatorSession;
class Dispatch;
class DispatchExec;
class DSSNService;
class ExternalStorage;
class Logger;
class MasterRecoveryManager;
class MasterService;
class MockContextMember;
class ObjectFinder;
class PortAlarmTimer;
class Service;
class WorkerManager;
class SessionAlarmTimer;
class TableManager;
class TransportManager;

/**
 * Context is a container for global variables.
 *
 * Its main purpose is to allow multiple instances of these variables to
 * coexist in the same address space. This is useful for a variety of reasons,
 * for example:
 *  - Accessing RAMCloud from multiple threads in a multi-threaded client
 *    application without locking.
 *  - Running a simulator of RAMCloud in which multiple clients and servers
 *    share a single process.
 *  - Running unit tests in parallel in the same process.
 *
 * Context also defines an explicit order in which these variables are
 * constructed and destroyed. Without such an ordering, it's easy to run into
 * memory corruption problems (e.g., RAM-212).
 *
 * Expected usage: on client machines there will be one Context per RamCloud
 * object, which also means one Context per thread.  On server machines there
 * is a single Context object shared among all the threads.
 */
class Context {
  PUBLIC:
    explicit Context(bool hasDedicatedDispatchThread = false,
            CommandLineOptions* options = NULL);
    ~Context();

    // Rationale:
    // - These are pointers to the heap to work around circular dependencies in
    //   header files.
    // - They are exposed publicly rather than via accessor methods for
    //   convenience in caller code.
    // - They are not managed by smart pointers (such as std::unique_ptr)
    //   because they need to be constructed and destroyed while inside this
    //   context (since later members may depend on earlier ones). That's
    //   pretty awkward to achieve with smart pointers in the face of
    //   exceptions.

    MockContextMember* mockContextMember1; ///< for testing purposes
    Dispatch* dispatch;
    MockContextMember* mockContextMember2; ///< for testing purposes
    TransportManager* transportManager;
    DispatchExec* dispatchExec;
    SessionAlarmTimer* sessionAlarmTimer;
    PortAlarmTimer*    portAlarmTimer;
    CoordinatorSession* coordinatorSession;
    CacheTrace* cacheTrace;
    ObjectFinder* objectFinder; // On Client and Master, this locator tells
                                // which master is the owner of an object.

    // Holds command-line options, or NULL if no options are available.
    // Memory for this is managed by whoever created this object; we can
    // assume this is static.
    CommandLineOptions* const options;

    // Variables below this point are used only in servers.  They are
    // always NULL on clients.

    // If this variable is non-NULL, it belongs to the Context and will
    // be freed when the Context is destroyed.
    WorkerManager* workerManager;

    // The following array is indexed by WireFormat::ServiceType, and
    // holds pointers to all of the services currently known in this
    // context.  NULL means "no such service". Services register themselves
    // here. The reference objects are not owned by this class (i.e. they
    // will not be freed here).
    Service* services[WireFormat::INVALID_SERVICE];

    // Valid only on the coordinator; used to save coordinator state so it
    // can be recovered after coordinator crashes.
    ExternalStorage* externalStorage;

    // The following variable is available on all servers (masters, backups,
    // coordinator). It provides facilities that are common to both ServerList
    // and CoordinatorServerList. Owned elsewhere; not freed by this class.
    AbstractServerList* serverList;

    // On coordinators, the following variable refers to the same object as
    // \c serverList; it provides additional features used by coordinators to
    // manage cluster membership.  NULL except on coordinators.  Owned
    // elsewhere; not freed by this class.
    CoordinatorServerList* coordinatorServerList;

    // On coordinators, it has the information about tablet map and
    // provides functions to modify and read this tablet map.
    // NULL except on coordinators.  Owned elsewhere; not freed by this class.
    TableManager* tableManager;

    // Handles all master recovery details on behalf of the coordinator.
    // NULL except on coordinators. Owned elsewhere;
    // not freed by this class.
    MasterRecoveryManager* recoveryManager;

    // On masters, it points to a permanently mapped region of read-only
    // memory that can be used as zero-copy source buffer for transmission;
    // its size is guaranteed to be large enough to hold the largest
    // request/reply message of an RPC.
    const void* masterZeroCopyRegion;
    // The prometheus client handler which opens server socket for incoming
    // metrics scrape
    prometheus::Exposer* metricExposer;

    /// Clock from the Cluster Time Service
    QDB::ClusterTimeService* ctsClock;
    /**
     * Returns the BackupService associated with this context, if
     * there is one, or NULL if there is none.
     */
    BackupService*
    getBackupService() {
        return reinterpret_cast<BackupService*>(
                services[WireFormat::BACKUP_SERVICE]);
    }

    /**
     * Returns the CoordinatorService associated with this context, if
     * there is one, or NULL if there is none.
     */
    CoordinatorService*
    getCoordinatorService() {
        return reinterpret_cast<CoordinatorService*>(
                services[WireFormat::COORDINATOR_SERVICE]);
    }

    /**
     * Returns the MasterService associated with this context, if there is one,
     * or NULL if there is none.
     */
    MasterService*
    getMasterService() {
        return reinterpret_cast<MasterService*>(
                services[WireFormat::MASTER_SERVICE]);
    }
    /**
     * Returns the DSSNService associated with this context, if there is one,
     * or NULL if there is none.
     */
    DSSNService*
    getDSSNService() {
        return reinterpret_cast<DSSNService*>(
                services[WireFormat::DSSN_SERVICE]);
    }

    /**
     * Returns the AdminService associated with this context, if there is one,
     * or NULL if there is none.
     */
    AdminService*
    getAdminService() {
        return reinterpret_cast<AdminService*>(
                services[WireFormat::ADMIN_SERVICE]);
    }
  PRIVATE:
    void destroy();
    DISALLOW_COPY_AND_ASSIGN(Context);
};

} // end RAMCloud

#endif  // RAMCLOUD_DISPATCH_H
