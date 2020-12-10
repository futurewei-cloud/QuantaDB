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
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef RAMCLOUD_WORKERMANAGER_H
#define RAMCLOUD_WORKERMANAGER_H

#include <queue>

#include "Dispatch.h"
#include "Transport.h"
#include "WireFormat.h"
#include "ThreadId.h"
#include "TimeTrace.h"
#include "PerfCounter.h"
#include "PerfStats.h"
#include "ServerId.h"
#include "WorkerManagerMetrics.h"

namespace RAMCloud {

/**
 * This class manages a pool of worker threads that carry out RPCs for
 * RAMCloud services.  It also implements an asynchronous interface between
 * the dispatch thread (which manages all of the network connections for a
 * server and runs Transport code) and the worker threads.
 */
class Worker;
class RpcHandle;

class WorkerManager : Dispatch::Poller {
  public:
    explicit WorkerManager(Context* context, uint32_t maxCores = 3);
    ~WorkerManager();

    void exitWorker();
    void handleRpc(Transport::ServerRpc* rpc);
    /**
     * The transport layer notifies the WorkManager the RPC is delivered to the hardware.
     * The workermanager uses this callback to perform necessary bookkeeping
     */
    void handleRpcDone(Transport::ServerRpc* rpc);
    bool idle();
    static void init();
    int poll();
    void setServerId(ServerId serverId);
    Transport::ServerRpc* waitForRpc(double timeoutSeconds);
    void enqueueRpcReply(RpcHandle* rpc) {
        //TODO: replace this with lockless multi producers single consumer queue if necessary
        rpcReplyQueueMutex.lock();
        mRpcReply.push(rpc);
	rpcReplyQueueMutex.unlock();
    }

  PROTECTED:
  static inline void timeTrace(const char* format,
        uint32_t arg0 = 0, uint32_t arg1 = 0, uint32_t arg2 = 0,
        uint32_t arg3 = 0);

    /// How many microseconds worker threads should remain in their polling
    /// loop waiting for work. If no new arrives during this period the
    /// worker thread will put itself to sleep, which releases its core but
    /// will result in additional delay for the next RPC while it wakes up.
    /// The value of this variable is typically not modified except during
    /// testing.
    static int pollMicros;

    /// Shared RAMCloud information.
    Context* context;

    // This class (along with the levels variable) stores information
    // for each of the levels defined by RpcLevel; if we run low on threads
    // for servicing RPCs, we queue RPCs according to their level.
    class Level {
      public:
        int requestsRunning;           /// The number of RPCs at this level
                                       /// that are currently executing.
        std::queue<Transport::ServerRpc*> waitingRpcs;
                                       /// Requests that cannot execute until
                                       /// a thread becomes available.
        explicit Level()
            : requestsRunning(0)
            , waitingRpcs()
        {}
    };
    inline RpcHandle* getRpcHandle(Transport::ServerRpc* rpc);
    inline void freeRpcHandle(RpcHandle* handle);
    inline void recordPollLatency() {
        pollLatency = Cycles::rdtsc() - lastPollEnd;
    }
    inline void sendAsyncRpcReply();
    std::mutex rpcReplyQueueMutex;
    std::vector<Level> levels;

    // Worker threads that are currently executing RPCs (no particular order).
    std::vector<Worker*> busyThreads;

    // Worker threads that are available to execute incoming RPCs.  Threads
    // are push_back'ed and pop_back'ed (the thread with highest index was
    // the last one to go idle, so it's most likely to be POLLING and thus
    // offer a fast wakeup).
    std::vector<Worker*> idleThreads;

    // Once the number of worker threads reaches this value, new RPCs will
    // only start executing if that is needed to provide a distributed
    // deadlock; other RPCs will wait until some threads finish.
    uint32_t maxCores;

    // Total number of RPCs (across all Levels) in waitingRpcs queues.
    int rpcsWaiting;

    // Nonzero means save incoming RPCs rather than executing them.
    // Intended for use in unit tests only.
    int testingSaveRpcs;

    //Track number of rpc received.
    uint64_t rpcRequestCount;
    //Track number of RPC currently being processed
    uint64_t rpcInProcCount;
    //Track the timestamp at which the worker manager processing end
    uint64_t lastPollEnd;
    //Track the last poll latency
    uint64_t pollLatency;

    // Used for testing: if testingSaveRpcs is set, incoming RPCs are
    // queued here, not sent to workers.
    std::queue<Transport::ServerRpc*> testRpcs;
    std::vector<RpcHandle*> mFreeRpcHandlePool;
    std::mutex mFreeRpcHandlePoolLock;
    // Store the list of rpc to be sent out
    std::queue<RpcHandle*> mRpcReply;
    static void workerMain(Worker* worker);
    static Syscall *sys;
    WorkerManagerMetrics* mMetrics;
    friend class Worker;
    friend class WorkerManagerMetrics;
    DISALLOW_COPY_AND_ASSIGN(WorkerManager);
};

/**
 * An object of this class describes a single worker thread and is used
 * for communication between the thread and the WorkerManager poller
 * running in the dispatch thread.  This structure is read-only to the
 * worker except for the #state field.  In principle this class definition
 * should be nested inside WorkerManager; however, we need to make forward
 * references to it, and C++ doesn't seem to permit forward references to
 * nested classes.
 */
class Worker {
  typedef RAMCloud::Perf::ReadThreadingCost_MetricSet
      ReadThreadingCost_MetricSet;
  public:
    bool replySent();
    void sendReply();
    Transport::ServerRpc* getServerRpc();

  PRIVATE:
    Context* context;                  /// Shared RAMCloud information.
    Tub<std::thread> thread;           /// Thread that executes this worker.
  public:
    int threadId;                      /// Identifier for this thread, assigned
                                       /// by the ThreadId class; set when the
                                       /// worker starts execution, 0 before
                                       /// then.
    WireFormat::Opcode opcode;         /// Opcode value from most recent RPC.
    int level;                         /// RpcLevel of most recent RPC.
    RpcHandle* rpc;                    /// RPC being serviced by this worker.
                                       /// NULL means the last RPC given to
                                       /// the worker has been finished and a
                                       /// response sent (but the worker may
                                       /// still be in POSTPROCESSING state).
    uint64_t handoffTs;                /// Timestamp of the Dispatcher's request
                                       /// to worker
  PRIVATE:
    int busyIndex;                     /// Location of this worker in
                                       /// #busyThreads, or -1 if this worker
                                       /// is idle.
    Atomic<int> state;                 /// Shared variable used to pass RPCs
                                       /// between the dispatch thread and this
                                       /// worker.

    /// Values for #state:
    enum {
        /// Set by the worker thread to indicate that it has finished
        /// processing its current request and is in a polling loop waiting
        /// for more work to do.  From this state 2 things can happen:
        /// * dispatch thread can change state to WORKING
        /// * worker can change state to SLEEPING.
        /// Note: this is the only state where both threads may set a new
        /// state (it requires special care!).
        POLLING,

        /// Set by the dispatch thread to indicate that a new RPC is ready
        /// to be processed.  From this state the worker will change state
        /// to either POSTPROCESSING or POLLING.
        WORKING,

        /// Set by the worker thread if it invokes #sendReply on the RPC;
        /// means that the RPC response is ready to be returned to the client,
        /// but the worker is still busy so we can't give it anything else
        /// to do.  From the state the worker will eventually change the
        /// state to POLLING.
        POSTPROCESSING,

        /// Set by the worker thread to indicate that it has been waiting
        /// so long for new work that it put itself to sleep; the dispatch
        /// thread will need to wake it up the next time it has an RPC for the
        /// worker.  From the state the dispatch thread will eventually change
        /// state to WORKING.
        SLEEPING
    };
    bool exited;                       /// True means the worker is no longer
                                       /// running.

    explicit Worker(Context* context)
            : context(context)
            , thread()
            , threadId(0)
            , opcode(WireFormat::Opcode::ILLEGAL_RPC_TYPE)
            , level(0)
            , rpc(NULL)
            , busyIndex(-1)
            , state(POLLING)
            , exited(false),
            threadWork(&ReadThreadingCost_MetricSet::threadWork, false)
        {}
    void exit();
    void handoff(RpcHandle* rpc);

  public:
    ReadThreadingCost_MetricSet::Interval threadWork;

  private:
    friend class WorkerManager;
    DISALLOW_COPY_AND_ASSIGN(Worker);
};

class RpcHandle {
  public:
    RpcHandle(Transport::ServerRpc* rpc)
        : mServerRpc(rpc)
        , mWorkerManager(NULL)
        , isAsync(false)
        , isSendAsyncReplyCalled(false)
    {}
    RpcHandle(Transport::ServerRpc* rpc, WorkerManager* wm)
        : mServerRpc(rpc)
        , mWorkerManager(wm)
        , isAsync(false)
        , isSendAsyncReplyCalled(false)
    {
    }
    void init(Transport::ServerRpc * rpc, WorkerManager* wm) {
        //TODO: explore the possibility of using the allocator and construct mechanism
        mServerRpc = rpc;
	mWorkerManager = wm;
	isAsync = false;
	isSendAsyncReplyCalled = false;
    }

    inline void enableAsync() { isAsync = true; }
    inline bool isAsyncEnabled() { return isAsync; }
    void sendReplyAsync() {
        //Enqueue this RPC on WorkerManager's queue
        if (isSendAsyncReplyCalled) {
	    assert(!"validator called sendReplyAsync() multiple times");
	}
        if (mWorkerManager) {
	    isSendAsyncReplyCalled = true;
	    mServerRpc->endRpcProcessingTimer();
	    mWorkerManager->enqueueRpcReply(this);
	} else {
#if TESTING
	    //This only happens in the unit test mode
	    mServerRpc->sendReply();
#endif
	}
    }
    void clear() {
        mServerRpc = NULL;
	mWorkerManager = NULL;
	isAsync = false;
    }
    inline Transport::ServerRpc* getServerRpc() { return mServerRpc; }
    private:
      Transport::ServerRpc* mServerRpc;
      WorkerManager* mWorkerManager;
      /**
       * Flag to indicate if the RPC processing is async
       */
      bool isAsync;
      /**
       * Flag to indicate if the sendAsyncReply() has already been called.
       */
      bool isSendAsyncReplyCalled;
};
}  // namespace RAMCloud

#endif  // RAMCLOUD_WORKERMANAGER_H
