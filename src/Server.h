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

/* Copyright (c) 2012-2016 Stanford University
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

#ifndef RAMCLOUD_SERVER_H
#define RAMCLOUD_SERVER_H

#include "AdminService.h"
#include "BackupService.h"
#include "CoordinatorClient.h"
#include "CoordinatorSession.h"
#include "DSSNService.h"
#include "FailureDetector.h"
#include "MasterService.h"
#include "ServerConfig.h"
#include "ServerId.h"
#include "ServerList.h"
#include "WorkerTimer.h"

namespace RAMCloud {

class BindTransport;

/**
 * Container for the various services and resources that make up a single
 * RAMCloud server (master and/or backup, but not coordinator).  Typically
 * there is a single Server created on main(), but this class can be used
 * to create many servers for testing (e.g., in MockCluster).
 *
 * Server is not (yet) capable of providing the CoordinatorService; it is
 * pieced together manually in CoordinatorMain.cc.
 */
class Server {
  PUBLIC:
    explicit Server(Context* context, const ServerConfig* config);
    ~Server();

    void startForTesting(BindTransport& bindTransport);
    void run();

  PRIVATE:
    ServerId createAndRegisterServices();
    void enlist(ServerId replacingId);

    /**
     * Shared RAMCloud information.
     */
    Context* context;

    /**
     * Configuration that controls which services are started as part of
     * this service and how each of those services is configured.  Many
     * of the contained parameters are set via option parsing from the
     * command line, but many are also internally set and used specifically
     * to create strange server configurations for testing.
     */
    ServerConfig config;

    /**
     * Read speed of the local backup's storage in MB/s.  Measured
     * startForTesting() used in run() during server enlistment unless
     * config.backup.mockSpeed is non-zero.
     */
    uint32_t backupReadSpeed;

    /**
     * The id of this server.  This id only becomes valid during run() or after
     * startForTesting() when the server enlists with the coordinator and
     * receives its cluster server id.
     */
    ServerId serverId;

    /**
     * The following variable is NULL if a serverList was already provided
     * in \c context. If not, we create a new one and store its pointer here
     * (so we can delete it in the destructor) as well as in context.
     * Normally, the server list should be accessed from \c context, not here.
     */
    ServerList* serverList;

    /// If enabled detects other Server in the cluster, else empty.
    Tub<FailureDetector> failureDetector;

    /**
     * The MasterService running on this Server, if requested, else empty.
     * See config.services.
     */
    Tub<MasterService> master;
#ifdef QDBTX
    /**
     * The DSSNService running on this Server, if requested, else empty.
     * See config.services.
     */
    Tub<QDB::DSSNService> dssnMaster;
#endif
    /**
     * The BackupService running on this Server, if requested, else empty.
     * See config.services.
     */
    Tub<BackupService> backup;

    /**
     * The AdminService running on this Server, if requested, else empty.
     * See config.services.
     */
    Tub<AdminService> adminService;

    // The class and variable below are used to run enlistment in a
    // worker thread, so that post-enlistment initialization doesn't
    // keep us from servicing RPCs.
    class EnlistTimer: public WorkerTimer {
      public:
        EnlistTimer(Server* server, ServerId formerServerId)
                : WorkerTimer(server->context->dispatch)
                , server(server)
                , formerServerId(formerServerId)
            {
                // Start enlistment immediately.
                start(0);
            }
        virtual void handleTimerEvent() {
            server->enlist(formerServerId);
        }
        Server* server;
        ServerId formerServerId;
        DISALLOW_COPY_AND_ASSIGN(EnlistTimer);
    };
    Tub<EnlistTimer> enlistTimer;

    DISALLOW_COPY_AND_ASSIGN(Server);
};

} // namespace RAMCloud

#endif
