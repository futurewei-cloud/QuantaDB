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

#pragma once

#include <prometheus/counter.h>
#include <prometheus/histogram.h>
#include <prometheus/gauge.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

#include "OpTrace.h"

namespace QDB {
class DSSNService;
  
enum DSSNServiceOp {
    DSSNServiceCommit,
    DSSNServiceRead,
    DSSNServiceReadKV,
    DSSNServiceReadMulti,
    DSSNServiceSendTxReply,
    DSSNServiceSendDSSNInfo,
    DSSNServiceRequestDSSNInfo,
    DSSNServiceWrite,
    DSSNServiceWriteMulti,
    DSSNServiceOpsMax
};

static const char* DssnOpLabels[] = {
    "commit",
    "read",
    "read_kv",
    "read_multiops",
    "send_txreply",
    "send_dssninfo",
    "request_dssninfo",
    "write",
    "write_multiops",
    "invalid"
};


class DSSNServiceMonitor {
   public:
     DSSNServiceMonitor(DSSNService *service, prometheus::Exposer* exposer);
     Metric* getOpMetric(DSSNServiceOp op) {
         return &mOps[op];
     }
     void collectPfMetrics();
     void collectTcMetrics();
     void clearMetrics();
     bool isEnabled() { return mEnabled; }
    /**
     * Function for the sampling thread to execute.  Sample the DSSNService metrics periodically
     */
     static void sample(DSSNServiceMonitor* mon);
     DSSNService* getDSSNService() { return mService; }
   private:
    /**
     * Helper function to add a performane metric
     */
    void addPfMetric(DSSNServiceOp type) {
        mPPfCounterHandle[type] = &mPPfCounters->Add({{"label", DssnOpLabels[type]}});
    }
    /**
     * Helper function to add a tracing histogram
     */
    void addTcMetric(DSSNServiceOp type) {
        prometheus::Histogram::BucketBoundaries bucketsInMicroSec{0.1, 1, 2.5, 5, 10, 20, 40, 60, 80, 100,
	    130, 160, 200};
	mPTcCounterHandle[type] = &mPTcCounters->Add({{"label", DssnOpLabels[type]}}, bucketsInMicroSec);
    }
    /**
     * The Performance related counter
     */
     std::shared_ptr<prometheus::Registry> mPPfRegistry;
     prometheus::Family<prometheus::Gauge>* mPPfCounters = nullptr;
     prometheus::Gauge* mPPfCounterHandle[DSSNServiceOpsMax];
    /**
     * The Tracing related counter
     */
     std::shared_ptr<prometheus::Registry> mPTcRegistry;
     prometheus::Family<prometheus::Histogram>* mPTcCounters = nullptr;
     prometheus::Histogram* mPTcCounterHandle[DSSNServiceOpsMax];
     DSSNService* mService;
     Metric mOps[DSSNServiceOpsMax];
     std::thread* mSampler;
     bool mEnabled;
};

}
