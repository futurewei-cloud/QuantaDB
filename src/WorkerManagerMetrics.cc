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

#include "Common.h"
#include "WorkerManagerMetrics.h"
#include "WorkerManager.h"

namespace RAMCloud {
using namespace prometheus;
  
WorkerManagerMetrics::WorkerManagerMetrics(WorkerManager* manager, Exposer* exposer) {
    mManager = manager;
    mEnabled = false;

    if (!IS_WORKERMANAGER_MONITORED()) {
        return;
    }

#ifdef MONITOR
    bool startSampler = false;
    if (exposer) {
        if (IS_DIAG_MONITOR_ENABLED()) {
	      mPDxRegistry = std::make_shared<Registry>();
	      //Create the list of diagnostic Metrics
	      mPDxCounters = &BuildGauge()
		.Name("workermanager_diagnostic")
		.Help("Diagnostic metrics for the Worker Manager Module")
		.Register(*mPDxRegistry);
	      for (auto i = 0; i < WMM_DX_MAX; i++)
		addDxMetric((WmmDxMetric)i);
	      exposer->RegisterCollectable(mPDxRegistry);
	      startSampler = true;
	  }
	if (IS_PERF_MONITOR_ENABLED()) {
	    //Create the list of performance Metrics
	    mPPfRegistry = std::make_shared<Registry>();
	    mPPfCounters = &BuildGauge()
	      .Name("workermanager_performance")
	      .Help("Performance metrics for the Worker Manager Module")
	      .Register(*mPPfRegistry);
	    for (auto i = 0; i < WMM_PF_MAX; i++)
	        addPfMetric((WmmPfMetric)i);
	    exposer->RegisterCollectable(mPPfRegistry);
	    startSampler = true;
	}
	if (IS_TRACING_MONITOR_ENABLED()) {
	    //Create the list of tracing Metrics
	    mPTcRegistry = std::make_shared<Registry>();
	    mPTcCounters = &BuildHistogram()
	      .Name("workermanager_rpc_latency")
	      .Help("RPC latency histogram")
	      .Register(*mPTcRegistry);
	    for (auto i = 0; i < WMM_TC_MAX; i++)
	        addTcMetric((WmmTcMetric)i);
	    exposer->RegisterCollectable(mPTcRegistry);
	}
	if (startSampler) {
	    mSampler = new std::thread(sample, this);
	}
	mEnabled = true;
    }
#endif
}

void
WorkerManagerMetrics::sample(WorkerManagerMetrics* wmm) {
#ifdef MONITOR
    while(wmm->isEnabled()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(MONITOR_SAMPLING_INTERVAL_IN_MS));
        if (IS_DIAG_MONITOR_ENABLED()) {
	    wmm->mPDxCounterHandle[WMM_RPC_WAIT_COUNT]->Set((double)wmm->mManager->rpcsWaiting);
	    wmm->mPDxCounterHandle[WMM_RPC_PROC_COUNT]->Set((double)wmm->mManager->rpcInProcCount);
	    wmm->mPDxCounterHandle[WMM_BUSY_THREAD_COUNT]->Set((double)wmm->mManager->busyThreads.size());
	}
	if (IS_PERF_MONITOR_ENABLED()) {
	    uint64_t rpcRequestCounter = wmm->mManager->rpcRequestCount;
	    //we want to track the rpc request rate, reset the cumulated counter to zero
	    wmm->mManager->rpcRequestCount = 0;
	    wmm->mPPfCounterHandle[WMM_PF_RPC_RATE]->Set((double)(rpcRequestCounter*1000/MONITOR_SAMPLING_INTERVAL_IN_MS));
	}
	if (IS_TRACING_MONITOR_ENABLED()) {
	    wmm->observeTcValue(WMM_TC_POLL_LATENCY, Cycles::toPreciseMicroseconds(wmm->mManager->pollLatency));
	}
    }
#endif
}
}
