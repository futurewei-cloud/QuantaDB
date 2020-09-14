/* Copyright (c) 2020 Futurewei Technologies, Inc.
 *
 * All rights reserved.
 */
#include "Common.h"
#include "DSSNService.h"
#include "DSSNServiceMonitor.h"

namespace DSSN {
using namespace prometheus;

DSSNServiceMonitor::DSSNServiceMonitor(DSSNService *service, Exposer* exposer) {
    mService = service;
    mEnabled = false;

#ifdef MONITOR
    bool startSampler = false;
    if (exposer) {
        if (IS_PERF_MONITOR_ENABLED()) {
	    //Create the list of performance Metrics
	    mPPfRegistry = std::make_shared<Registry>();
	    mPPfCounters = &BuildGauge()
	      .Name("DSSNService_performance")
	      .Help("Performance metrics for the DSSNService Module")
	      .Register(*mPPfRegistry);
	    for (uint32_t i = 0; i < DSSNServiceOpsMax; i++)
	        addPfMetric((DSSNServiceOp)i);
	    exposer->RegisterCollectable(mPPfRegistry);
	    startSampler = true;
	}
	if (IS_TRACING_MONITOR_ENABLED()) {
	    //Create the list of tracing Metrics
	    mPTcRegistry = std::make_shared<Registry>();
	    mPTcCounters = &BuildHistogram()
	      .Name("DSSNService_Tracing")
	      .Help("Operation histogram")
	      .Register(*mPTcRegistry);
	    for (uint32_t i = 0; i < DSSNServiceOpsMax; i++)
	        addTcMetric((DSSNServiceOp)i);
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
DSSNServiceMonitor::collectPfMetrics() {
  if (mEnabled) {
    /*
      for(uint32_t i; i < DSSNServiceOpsMax; i++) {
	  uint64_t count = mOps[i].count
	  mPTcCounterHandle[i]->Observe(mOps[i].latency);
      }
    */
  }

}

void
DSSNServiceMonitor::collectTcMetrics() {
#ifdef MONITOR
  if (mEnabled) {
      for(uint32_t i = 0; i < DSSNServiceOpsMax; i++) {
	if (mOps[i].isValid()) {
	    mPTcCounterHandle[i]->Observe(Cycles::toMicroseconds(mOps[i].latency));
	}
      }
  }
#endif
}

void
DSSNServiceMonitor::clearMetrics() {
  if (mEnabled) {
      for(uint32_t i = 0; i < DSSNServiceOpsMax; i++) {
	  mOps[i].clear();
      }
  }
}

void
DSSNServiceMonitor::sample(DSSNServiceMonitor* mon) {
#ifdef MONITOR
    while(mon->isEnabled()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(MONITOR_SAMPLING_INTERVAL_IN_MS));
        if (IS_DIAG_MONITOR_ENABLED()) {
	}
	if (IS_PERF_MONITOR_ENABLED()) {
	    mon->collectPfMetrics();
	}
	if (IS_TRACING_MONITOR_ENABLED()) {
	    mon->collectTcMetrics();
	}
	mon->clearMetrics();
    }
#endif
}

};
