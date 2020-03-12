/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "WaitQueue.h"

namespace DSSN {

void
WaitQueue::start() {
    // Henry: may need to use TBB to pin the threads to specific cores LATER
    std::thread( [=] { schedule(false); });
}

void
WaitQueue::schedule(bool isUnderTest) {
    TxEntry* txEntry;
	do {
		while (inQueue.pop(*&txEntry)) {
			while (!outQueue.push(txEntry));
		}
		if (recycleQueue.pop(*&txEntry))
			while (!outQueue.push(txEntry));
	} while (!isUnderTest);
};

} // end WaitQueue class

