/* Copyright (c) 2020 Futurewei Technologies, Inc.
 *
 * All rights reserved.
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
    }
#endif
}
}
