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

/* Copyright (c) 2009-2016 Stanford University
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

/**
 * \file
 * This file provides the main program for RAMCloud storage servers.
 */

#include "Context.h"
#include "CoordinatorSession.h"
#if INFINIBAND
#include "InfRcTransport.h"
#endif
#include "MemoryMonitor.h"
#include "OptionParser.h"
#include "PortAlarm.h"
#include "Server.h"
#include "PerfStats.h"
#include "ShortMacros.h"
#include "TransportManager.h"
#include "WorkerTimer.h"

using namespace RAMCloud;

// The following class is used for performance debugging: it logs
// performance information at regular intervals.

class StatsLogger : WorkerTimer {
  public:
    /**
     * Constructor  for StatsLogger
     * \param dispatch
     *      Dispatcher that will be used to trigger logging.
     * \param intervalSecs
     *      Interval at which performance info is logged, in seconds.
     */
    StatsLogger(Dispatch* dispatch, double intervalSecs)
        : WorkerTimer(dispatch),
        intervalSecs(intervalSecs),
        intervalCycles(0),
        triggerTime(0),
        oldStats()
    {
        intervalCycles = static_cast<uint64_t>(
                Cycles::perSecond()*intervalSecs);
        triggerTime = Cycles::rdtsc() + intervalCycles;
        memset(&oldStats, 0, sizeof(oldStats));
        start(triggerTime);
    }

  PRIVATE:
    // See WorkerTimer::handleTimerEvent for documentation.
    virtual void handleTimerEvent()
    {
        PerfStats newStats;
        PerfStats::collectStats(&newStats);
        double rate = static_cast<double>((newStats.writeCount
                - oldStats.writeCount));
        rate /= Cycles::toSeconds(newStats.collectionTime
                - oldStats.collectionTime);
        RAMCLOUD_LOG(NOTICE,
                "Write rate for %.2f second interval: %6.2f kops/sec",
                intervalSecs, rate/1e03);
        oldStats = newStats;
        triggerTime += intervalCycles;
        start(triggerTime);
    }

    // Constructor argument.
    double intervalSecs;

    // The number of rdtsc cycles between firings of the timer.
    uint64_t intervalCycles;

    // The rdtsc time at which the timer is set to fire next.
    uint64_t triggerTime;

    // PerfStats from the previous time  the timer triggered.
    PerfStats oldStats;
};

int
main(int argc, char *argv[])
{
    signal(SIGTERM, Perf::terminationHandler);
    Logger::installCrashBacktraceHandlers();
    try {
        ServerConfig config = ServerConfig::forExecution();
        string masterTotalMemory, hashTableMemory;

        bool masterOnly;
        bool backupOnly;

        OptionsDescription serverOptions("Server");
        serverOptions.add_options()
            ("allowLocalBackup",
             ProgramOptions::bool_switch(&config.master.allowLocalBackup),
             "Allow replication to local backup")
            ("backupInMemory,m",
             ProgramOptions::bool_switch(&config.backup.inMemory),
             "Backup will store segment replicas in memory")
            ("backupOnly,B",
             ProgramOptions::bool_switch(&backupOnly),
             "The server should run the backup service only (no master)")
            ("backupStrategy",
             ProgramOptions::value<int>(&config.backup.strategy)->
               default_value(RANDOM_REFINE_AVG),
             "0 random refine min, 1 random refine avg, 2 even distribution, "
             "3 uniform random")
            ("backupWriteRateLimit",
             ProgramOptions::value<size_t>(
                &config.backup.writeRateLimit)->default_value(0),
             "If non-0, specifies the maximum number of megabytes per second "
             "of bandwidth this backup should use. Useful for artificially "
             "restricting bandwidth when measuring various parts of the "
             "system.")
            ("cleanerBalancer",
             ProgramOptions::value<string>(&config.master.cleanerBalancer)->
                default_value("tombstoneRatio:0.40"),
             "Which balancing algorithm to use to schedule cleaning on disk "
             "and in-memory compaction, as well as how to orchestrate multiple "
             "cleaner threads. You will almost certainly want to use the "
             "default value. Currently the only other option is \"fixed:X\", "
             "where 0 <= X <= 100 represents the percentage of CPU time the "
             "disk cleaner will be limited to (the rest is for compaction).")
            ("detectFailures",
             ProgramOptions::value<bool>(&config.detectFailures)->
                default_value(true),
             "Whether to use the randomized failure detector")
            ("disableLogCleaner,d",
             ProgramOptions::bool_switch(&config.master.disableLogCleaner),
             "Disable the log cleaner entirely. You will eventually run out "
             "of memory, but at least you can do so faster this way.")
            ("disableInMemoryCleaning,D",
             ProgramOptions::bool_switch(
                &config.master.disableInMemoryCleaning),
             "Disable the in-memory cleaning portion of the log cleaner. When "
             "turned off, the cleaner will always clean both in memory and on "
             "backup disks at the same time.")
            ("diskExpansionFactor,E",
             ProgramOptions::value<double>(&config.master.diskExpansionFactor)->
                default_value(2.0),
             "Factor (>= 1.0) controlling how much backup disk space this "
             "master will use beyond its in-memory log size. For example, if "
             "the master has memory for 100 full segments and the expansion "
             "factor is 2.0, it will place up to 200 segments (each replicated "
             "R times) on backups.")
            ("file,f",
             ProgramOptions::value<string>(&config.backup.file)->
                default_value("/var/tmp/backup.log"),
             "The file path to the backup storage.")
            ("hashTableMemory,h",
             ProgramOptions::value<string>(&hashTableMemory)->
                default_value("10%"),
             "Percentage or megabytes of master memory allocated to "
             "the hash table")
            ("logCleanerThreads",
             ProgramOptions::value<uint32_t>(
                &config.master.cleanerThreadCount)->default_value(1),
             "The number of cleaner threads controls the amount of parallelism "
             "in the cleaner. More threads will use more cores, but may be "
             "able to better keep up with high write rates.")
            ("masterOnly,M",
             ProgramOptions::bool_switch(&masterOnly),
             "The server should run the master service only (no backup)")
            ("maxCores",
             ProgramOptions::value<uint32_t>(
                &config.maxCores)->default_value(16),
             "Limit on number of cores to use for the dispatch and worker "
             "threads. This value should not exceed the number of cores "
             "available on the machine. RAMCloud will try to keep its usage "
             "under this limit, but may occasionally need to exceed it "
             "(e.g., to avoid distributed deadlocks). Th limit does not "
             "include cleaner threads and some other miscellaneous functions.")
            ("maxNonVolatileBuffers",
             ProgramOptions::value<uint32_t>(
               &config.backup.maxNonVolatileBuffers)->default_value(10),
             "Maximum number of segments the backup will buffer in memory. The "
             "value 0 is special: it tells the server to set the "
             "limit equal to the \"segmentFrames\" value, effectively making "
             "buffering unlimited.")
            ("maxRecoveryReplicas",
             ProgramOptions::value<uint32_t>(
               &config.backup.maxRecoveryReplicas)->default_value(20),
             "Maximum number of replicas any given master recovery will buffer "
             "in memory.")
            ("preferredIndex",
             ProgramOptions::value<uint32_t>(
                &config.preferredIndex)->default_value(0),
             "Use this value as the index number for this server's server id, "
             "if that number isn't already in use. Can be used to ensure "
             "a reproducible assignment of server ids.")
            ("replicas,r",
             ProgramOptions::value<uint32_t>(&config.master.numReplicas),
             "Number of backup copies to make for each segment")
            ("segmentFrames",
             ProgramOptions::value<uint32_t>(&config.backup.numSegmentFrames)->
                default_value(512),
             "The amount of space available to the backup service, specified "
             "in units of 8MB segments. If a server uses N bytes of DRAM and "
             "the replication factor is R, then this value should be at least "
             "2NR/8M (gives the backup 2NR bytes of space); any value lower "
             "than this may cause the cluster to eventually fail to service "
             "write requests.")
            ("sync",
             ProgramOptions::bool_switch(&config.backup.sync),
             "Make all updates completely synchronous all the way down to "
             "stable storage.")
            ("totalMasterMemory,t",

             // Note: we have tried changing the default value below to
             // something that would make sense in production (80%?), but
             // as of 6/2014 that causes too many problems in test and
             // development environments (e.g. hooks/pre-commit will take
             // forever, and if two people do this, rcmaster will run out
             // of memory).
             ProgramOptions::value<string>(&masterTotalMemory)->
                default_value("500"),
             "Percentage or megabytes of system memory for master log & "
             "hash table")
            ("hugepage",
             ProgramOptions::bool_switch(&config.master.useHugepages),
             "Whether to use hugepage memory to allocate LargeBlockOfMemory")
            ("useMinCopysets",
             ProgramOptions::value<bool>(&config.master.useMinCopysets)->
                default_value(false),
             "Whether to use MinCopysets or random replication")
            ("usePlusOneBackup",
             ProgramOptions::value<bool>(&config.master.usePlusOneBackup)->
                default_value(false),
             "Whether to use (masterServerId+1)modulo n or random "
             "replication for backupServerId")
            ("writeCostThreshold,w",
             ProgramOptions::value<uint32_t>(
                &config.master.cleanerWriteCostThreshold)->default_value(8),
             "If in-memory cleaning is enabled, do disk cleaning when the "
             "write cost of memory freed by the in-memory cleaner exceeds "
             "this value. Lower values cause the disk cleaner to run more "
             "frequently. Higher values do more in-memory cleaning and "
             "reduce the amount of backup disk bandwidth used during disk "
             "cleaning.")
	    ("metricScapePort",
             ProgramOptions::value<uint32_t>(&config.master.metricScrapePort)->default_value(-1),
             "The port at which the metric server will pull the metrics");

        OptionParser optionParser(serverOptions, argc, argv);

        // Log all the command-line arguments.
        string args;
        for (int i = 0; i < argc; i++) {
            if (i != 0)
                args.append(" ");
            args.append(argv[i]);
        }
        LOG(NOTICE, "Command line: %s", args.c_str());
        LOG(NOTICE, "Server process id: %u", getpid());

        Context context(true, &optionParser.options);

        if (masterOnly && backupOnly)
            DIE("Can't specify both -B and -M options");

        if (masterOnly) {
#if QDBTX
            config.services = {WireFormat::MASTER_SERVICE,
                               WireFormat::ADMIN_SERVICE,
			       WireFormat::DSSN_SERVICE};
#else
	    config.services = {WireFormat::MASTER_SERVICE,
                               WireFormat::ADMIN_SERVICE};
#endif
        } else if (backupOnly) {
            config.services = {WireFormat::BACKUP_SERVICE,
                               WireFormat::ADMIN_SERVICE};
        } else {
#if QDBTX
            config.services = {WireFormat::MASTER_SERVICE,
                               WireFormat::BACKUP_SERVICE,
                               WireFormat::ADMIN_SERVICE,
	                       WireFormat::DSSN_SERVICE};
#else
	    config.services = {WireFormat::MASTER_SERVICE,
                               WireFormat::BACKUP_SERVICE,
                               WireFormat::ADMIN_SERVICE};
#endif
        }

        const string localLocator = optionParser.options.getLocalLocator();

#if INFINIBAND
        InfRcTransport::setName(localLocator.c_str());
#endif
        context.transportManager->setSessionTimeout(
                optionParser.options.getSessionTimeout());
        context.transportManager->initialize(localLocator.c_str());
        // Transports may augment the local locator somewhat.
        // Make sure the server is aware of that augmented locator.
        config.localLocator =
            context.transportManager->getListeningLocatorsString();
        LOG(NOTICE, "%s: Listening on %s",
            config.services.toString().c_str(), config.localLocator.c_str());

        config.coordinatorLocator =
            optionParser.options.getExternalStorageLocator();
        if (config.coordinatorLocator.size() == 0) {
            config.coordinatorLocator =
                optionParser.options.getCoordinatorLocator();
        }
        config.clusterName = optionParser.options.getClusterName();
        context.coordinatorSession->setLocation(
                config.coordinatorLocator.c_str(),
                config.clusterName.c_str());

#if 0
        // This code has been removed because it introduces a
        // use-before-initialization bug.
        //
        // These RPC cause the Dispatch::poll to be invoked, which can cause
        // incoming RPC requests to be serviced, which causes
        // Context->workerManager to be used before it is initialized.

        // Get Default Backup Configuration from Coordinator.
        if (config.services.has(WireFormat::BACKUP_SERVICE)) {
            ProtoBuf::ServerConfig_Backup backupConfig;
            CoordinatorClient::getBackupConfig(&context, backupConfig);
            config.backup.deserialize(backupConfig);
        }

        // Get Default Master Configuration from Coordinator.
        if (config.services.has(WireFormat::MASTER_SERVICE)) {
            ProtoBuf::ServerConfig_Master masterConfig;
            CoordinatorClient::getMasterConfig(&context, masterConfig);
            config.master.deserialize(masterConfig);
        }
#endif

        // Re-parse the options to override coordinator provided defaults.
        OptionParser optionReparser(serverOptions, argc, argv);

        if (!backupOnly) {
            LOG(NOTICE, "Using %u backups", config.master.numReplicas);
            config.setLogAndHashTableSize(masterTotalMemory, hashTableMemory);
        }

        // Set PortTimeout and start portTimer
        LOG(NOTICE, "PortTimeOut=%d", optionParser.options.getPortTimeout());
        context.portAlarmTimer->setPortTimeout(
            optionParser.options.getPortTimeout());

        // Uncomment the following line to enable regular performance logging.
        // StatsLogger logger(context.dispatch, 1.0);
        MemoryMonitor monitor(context.dispatch, 1.0, 100);

#ifdef MONITOR
	// For the single node performance profiling, the system will use localhost
	// ip address and port 8080 for the metric scraping
	ServiceLocator sl(config.localLocator);
	uint32_t port = 8080;
	std::string hostName = "127.0.0.1";
	if (config.master.metricScrapePort != (uint32_t)-1) {
	    hostName = sl.getOption("host");
	    port = config.master.metricScrapePort;
	    //uint32_t port = std::stoi(sl.getOption("port")) + 1;
	}
	std::string address = hostName + string(":") + to_string(port);
	context.metricExposer = new prometheus::Exposer(address, "/metrics", 1);
	LOG(NOTICE, "start metric exporter on: %s", address.c_str());
#endif
        Server server(&context, &config);
        server.run(); // Never returns except for exceptions.

        return 0;
    } catch (const Exception& e) {
        LOG(ERROR, "Fatal error in server: %s", e.what());
        Logger::get().sync();
        return 1;
    } catch (const std::exception& e) {
        LOG(ERROR, "Fatal error in server: %s", e.what());
        Logger::get().sync();
        return 1;
    } catch (...) {
        LOG(ERROR, "Unknown fatal error in server");
        Logger::get().sync();
        return 1;
    }
}
