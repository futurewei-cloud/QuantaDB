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

#ifndef WorkerManagerMetrics_H
#define WorkerManagerMetrics_H

#include <prometheus/counter.h>
#include <prometheus/histogram.h>
#include <prometheus/gauge.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <thread>

namespace RAMCloud {
/**
 * Performance related counters
 */
enum WmmPfMetric {
    WMM_PF_RPC_RATE,
    WMM_PF_MAX
};

static const char* WmmPfMetricLabels[] = {
    "RPC_request_rate",
    "Invalid"
};

/**
 * Diagnostic related counters
 */
enum WmmDxMetric {
    WMM_RPC_WAIT_COUNT,
    WMM_RPC_PROC_COUNT,
    WMM_BUSY_THREAD_COUNT,
    WMM_DX_MAX
};

static const char* WmmDxMetricLabels[] = {
    "RPC_waiting_count",
    "RPC_processing_count",
    "busy_worker_threads",
    "Invalid"
};

/**
 * Tracing related counters
 */
enum WmmTcMetric {
    WMM_TC_RPC_LATENCY,
    WMM_TC_INGRESS_LATENCY,
    WMM_TC_WORKER_HANDOFF_LATENCY,
    WMM_TC_RPC_PREPROC_LATENCY,
    WMM_TC_RPC_PROC_LATENCY,
    WMM_TC_RPC_PROC_QUEUE_LATENCY,
    WMM_TC_EGRESS_LATENCY,
    WMM_TC_POLL_LATENCY,
    WMM_TC_MAX
};

static const char* WmmTcMetricLabels[] = {
    "RPC_total_latency",
    "RPC_ingress_latency",
    "RPC_worker_handoff_latency",
    "RPC_preprocessing_latency",
    "RPC_processing_latency",
    "RPC_processing_queue_latency",
    "RPC_egress_latency",
    "worker_manager_poll_latency",
    "Invalid"
};

class WorkerManager;

class WorkerManagerMetrics {
 public:
    WorkerManagerMetrics() {};
    WorkerManagerMetrics(WorkerManager* manager, prometheus::Exposer* exposer);
    bool isEnabled() { return mEnabled; }
    /**
     * Function to insert a trace value into the histogram
     */
    void observeTcValue(WmmTcMetric type, double value) {
        if (mEnabled) {
	    mPTcCounterHandle[type]->Observe(value);
	}
    }
    /**
     * Function for the sampling thread to execute.  Sample the worker manager metrics periodically
     */
    static void sample(WorkerManagerMetrics* wmm);

 private:
    /**
     * Helper function to add a diagnostic metric
     */

    void addDxMetric(WmmDxMetric type) {
        mPDxCounterHandle[type] = &mPDxCounters->Add({{"label", WmmDxMetricLabels[type]}});
    }
    /**
     * Helper function to add a performane metric
     */
    void addPfMetric(WmmPfMetric type) {
        mPPfCounterHandle[type] = &mPPfCounters->Add({{"label", WmmPfMetricLabels[type]}});
    }
    /**
     * Helper function to add a tracing histogram
     */
    void addTcMetric(WmmTcMetric type) {
        prometheus::Histogram::BucketBoundaries bucketsInMicroSec{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 40, 60, 80, 100,
	    150, 200};
	mPTcCounterHandle[type] = &mPTcCounters->Add({{"label", WmmTcMetricLabels[type]}}, bucketsInMicroSec);
    }
    WorkerManager* mManager;
    /**
     * The Diagnostic related counter
     */
    std::shared_ptr<prometheus::Registry> mPDxRegistry;
    prometheus::Family<prometheus::Gauge>* mPDxCounters = nullptr;
    prometheus::Gauge* mPDxCounterHandle[WMM_DX_MAX];
    /**
     * The Performance related counter
     */
    std::shared_ptr<prometheus::Registry> mPPfRegistry;
    prometheus::Family<prometheus::Gauge>* mPPfCounters = nullptr;
    prometheus::Gauge* mPPfCounterHandle[WMM_PF_MAX];
    /**
     * The Tracing related counter
     */
    std::shared_ptr<prometheus::Registry> mPTcRegistry;
    prometheus::Family<prometheus::Histogram>* mPTcCounters = nullptr;
    prometheus::Histogram* mPTcCounterHandle[WMM_TC_MAX];

    std::thread* mSampler;
    bool mEnabled;
};

}
#endif /* WorkerManagerMetrics_H */
