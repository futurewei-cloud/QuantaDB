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

#include "Cycles.h"

namespace QDB {

struct Metric {
    Metric()
      : latency(0)
      , count(0)
      , sCount(0)
      , fCount(0)
  {}
    bool isValid() { return (count > 0); }
    void clear() {
        count = 0;
	sCount = 0;
	fCount = 0;
	latency = 0;
    }
    uint64_t latency;             //Last latency value in CPU cycles
    std::atomic<uint64_t> count;  //Cumulative count
    std::atomic<uint64_t> sCount; //Cumulative count on succeeded operations
    std::atomic<uint64_t> fCount; //Cumulative count on failed operations
};

/**
 *  This class track executive time of a function or block of codes in CPU
 *  cycles.
 *  The cycle time is written to the buffer provided by the caller
 */
class OpTrace {
 public:
    /**
     * Constructor of the OpTrace
     */
    OpTrace(Metric *metric) {
#if defined(MONITOR) || defined(TESTING)
        pMetric = metric;
	pResult = NULL;
        mStartTime = RAMCloud::Cycles::rdtsc();
#endif
 
    }

    OpTrace(Metric *metric, bool* result) {
#if defined(MONITOR) || defined(TESTING)
        pMetric = metric;
	pResult = result;
        mStartTime = RAMCloud::Cycles::rdtsc();
#endif
    }

    ~OpTrace() {
#if defined(MONITOR) || defined(TESTING)
        pMetric->latency = RAMCloud::Cycles::rdtsc() - mStartTime;
	pMetric->count++;

	if (pResult == NULL) return;

	if (*pResult) {
	    pMetric->sCount++;
	} else {
	    pMetric->fCount++;
	}
#endif
    }
   private:
      uint64_t mStartTime;
      Metric* pMetric;
      bool* pResult;
};

}
