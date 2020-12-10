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

#include <errno.h>
#include <fcntl.h>

#include "Common.h"
#include "AdminClient.h"
#include "CycleCounter.h"
#include "Cycles.h"
#include "Fence.h"
#include "CoordinatorClient.h"
#include "FailureDetector.h"
#include "IpAddress.h"
#include "MasterService.h"
#include "ShortMacros.h"
#include "WireFormat.h"
#include "ClusterTimeService.h"

namespace RAMCloud {

/**
 * Create a new FailureDetector object. Note that this class depends on the
 * AdminService running and keeping the global Context::serverList up
 * to date. Without it, we'd not know of new servers to ping.
 *
 * \param context
 *      Overall information about the RAMCloud server.
 * \param[in] ourServerId
 *      The ServerId of this server, as returned by enlistment with the
 *      coordinator. Used only to avoid pinging ourself.
 */
FailureDetector::FailureDetector(Context* context,
                                 const ServerId ourServerId)
    : context(context),
      ourServerId(ourServerId),
      serverTracker(context),
      probesWithoutResponse(0),
      thread(),
      threadShouldExit(false)
{
}

FailureDetector::~FailureDetector()
{
    halt();
}

/**
 * Start the failure detector thread.
 *
 * This method starts the failure detector thread and returns immediately. Use
 * the halt method to terminate the failure detector thread. A valid context
 * must be initialized before calling start.
 */
void
FailureDetector::start()
{
    thread.construct(detectorThreadEntry, this, context);
}

/**
 * Halt the failure detector thread.
 *
 * This method stops the failure detector thread. Once the function returns
 * the thread will have terminated and cleaned up after itself.
 */
void
FailureDetector::halt()
{
    if (thread) {
        threadShouldExit = true;
        Fence::sfence();
        thread->join();
        threadShouldExit = false;
        thread.destroy();
    }
}

/**
 * Main thread entry point for the failure detector. Spin forever, probing
 * hosts, checkings for responses, and alerting the coordinator of any timeouts.
 *
 * \param detector
 *          FailureDetector passed in from thread creation.
 * \param context
 *          Context object to use for all operations.
 */
void
FailureDetector::detectorThreadEntry(FailureDetector* detector,
                                     Context* context)
{
    LOG(NOTICE, "Failure detector thread started");

    // Wait a while before sending the first probe, so that the coordinator
    // has had a chance to let the other servers know of our existence.
    // Without this delay, we may end up probing a server that doesn't
    // know about us, resulting in extraneous warnings about verifying
    // cluster membership. The initial wait time used below is a fairly
    // random guess.
    struct timespec interval;
    interval.tv_sec = 1;
    interval.tv_nsec = 0;
    nanosleep(&interval, NULL);

    interval.tv_sec = 0;
    interval.tv_nsec = PROBE_INTERVAL_USECS*1000;

    while (1) {
        // Check if we have been requested to exit.
        Fence::lfence();
        if (detector->threadShouldExit)
            break;

        // Drain the list of changes to update our tracker.
        ServerDetails dummy1;
        ServerChangeEvent dummy2;
        while (detector->serverTracker.getChange(dummy1, dummy2)) {
        }

        // Ping a random server
        detector->pingRandomServer();

        // Sleep for the specified interval
        nanosleep(&interval, NULL);
    }
}

/**
 * Choose a random server from our list and ping it. Only one outstanding
 * ping is issued at a given time. If a timeout occurs we attempt to notify
 * the coordinator.
 */
void
FailureDetector::pingRandomServer()
{
    ServerId pingee = serverTracker.getRandomServerIdWithService(
        WireFormat::ADMIN_SERVICE);
    if (!pingee.isValid() || pingee == ourServerId) {
        // If there isn't anyone to talk to, or the host selected
        // is ourself, then just skip this round and try again
        // on the next ping interval.
        return;
    }

    probesWithoutResponse++;
    string locator;
    try {
        locator = context->serverList->getLocator(pingee);
        LOG(DEBUG, "Sending ping to server %s (%s)", pingee.toString().c_str(),
            locator.c_str());
        uint64_t start = Cycles::rdtsc();
        PingRpc rpc(context, pingee, ourServerId);
        if (rpc.wait(TIMEOUT_USECS *1000)) {
	    uint64_t currentTime = (context->ctsClock)->getLocalTime();
	    uint64_t rtdNs = Cycles::toNanoseconds(Cycles::rdtsc() - start);
	    uint64_t timeDiff = abs(int64_t(currentTime - rpc.remoteServerTime));
	    //uint64_t timeDiff = abs(int64_t(currentTime - rpc.remoteServerTime - ((rtdUs * 1000)>>1)));
            probesWithoutResponse = 0;
            LOG(DEBUG, "Ping succeeded to server %s (%s) in %lu ns",
                pingee.toString().c_str(), locator.c_str(),
                rtdNs);
	    //if (timeDiff > MAX_CLOCK_DRIFT_NS) {
	    if (timeDiff > rtdNs) {
	        LOG(ERROR, "The time difference %lu nsec exceeded the max clock drift %lu nsec allowed (local: %lu, remote: %lu, rtd: %lu)",
		    timeDiff, rtdNs, currentTime, rpc.remoteServerTime, rtdNs);
	    }
        } else {
            // Server appears to have crashed; notify the coordinator.
            LOG(WARNING, "Ping timeout to server id %s (locator \"%s\")",
                pingee.toString().c_str(), locator.c_str());
            CoordinatorClient::hintServerCrashed(context, pingee);
        }
    } catch (const ServerListException &sle) {
        // This isn't an error. It's just a race between this thread and
        // the membership service. It should be quite uncommon, so just
        // bail on this round and ping again next time.
        LOG(NOTICE, "Tried to ping locator \"%s\", but id %s was stale",
            locator.c_str(), pingee.toString().c_str());
    } catch (const CallerNotInClusterException &e) {
        // See "Zombies" in designNotes.
        MasterService::Disabler disabler(context->getMasterService());
        CoordinatorClient::verifyMembership(context, ourServerId);
        probesWithoutResponse = 0;
    }
    if (probesWithoutResponse >= MAX_FAILED_PROBES) {
        // See "Zombies" in designNotes.
        MasterService::Disabler disabler(context->getMasterService());
        CoordinatorClient::verifyMembership(context, ourServerId);
        probesWithoutResponse = 0;
    }
}
} // namespace
