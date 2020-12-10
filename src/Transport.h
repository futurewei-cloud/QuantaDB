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

/* Copyright (c) 2010-2015 Stanford University
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

#ifndef RAMCLOUD_TRANSPORT_H
#define RAMCLOUD_TRANSPORT_H

#include <atomic>
#include <string>
#include <boost/intrusive_ptr.hpp>

#include "BoostIntrusive.h"
#include "Buffer.h"
#include "CodeLocation.h"
#include "Common.h"
#include "Cycles.h"
#include "Exception.h"

namespace RAMCloud {
class ServiceLocator;

/**
 * An exception that is thrown when the Transport class encounters a problem.
 */
struct TransportException : public Exception {
    explicit TransportException(const CodeLocation& where)
        : Exception(where) {}
    TransportException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
    TransportException(const CodeLocation& where, int errNo)
        : Exception(where, errNo) {}
    TransportException(const CodeLocation& where, string msg, int errNo)
        : Exception(where, msg, errNo) {}
};

/**
 * An interface for reliable communication across the network.
 *
 * Implementations all send and receive RPC messages reliably over the network.
 * These messages are variable-length and can be larger than a single network
 * frame in size.
 *
 * Implementations differ in the protocol stacks they use and their performance
 * characteristics.
 */
class Transport {
  public:
    class RpcNotifier;

      /// Maximum allowable size for an RPC request or response message: must
      /// be large enough to hold an 8MB segment plus header information.
#if HOMA_BENCHMARK
      static const uint32_t MAX_RPC_LEN = ((1 << 25) + 200);
#else
      static const uint32_t MAX_RPC_LEN = ((1 << 23) + 200);
#endif
    /**
     * An RPC request that has been received and is either being serviced or
     * waiting for service.
     */
    class ServerRpc {
      PROTECTED:
        /**
         * Constructor for ServerRpc.
         */
        ServerRpc()
            : requestPayload()
            , replyPayload()
            , epoch(0)
            , activities(~0)
            , outstandingRpcListHook()
	    , mIsRspReq(true)
        {}

      public:
        /**
         * Destructor for ServerRpc.
         */
        virtual ~ServerRpc() {}

        /**
         * Respond to the RPC with the contents of #replyPayload.
         *
         * You should discard all pointers to this #ServerRpc object after this
         * call.
         *
         * \throw TransportException
         *      If the client has crashed.
         */
        virtual void sendReply() = 0;

        /**
         * Return a ServiceLocator string describing the client that initiated
         * this RPC. Since it's a client this string can't be used to address
         * anything, but it can be useful for debugging purposes.
         *
         * Note that for performance reasons Transports may defer constructing
         * the string until this method is called (since it is expected to be
         * used only off of the fast path).
         */
        virtual string getClientServiceLocator() = 0;

        /**
         * Returns false if the epoch was not set, else true. Used to assert
         * that no RPCs are pushed through the WorkerManager without an epoch.
         */
        bool
        epochIsSet()
        {
            return epoch != 0;
        }

	/**
	 * Indicates if this RPC is being traced
	 */
	bool
	isTracing()
	{
	    return (mStartTime != 0);
	}
	/**
	 * Indicates if response is required for this RPC
	 */
	inline bool
	isRspReq()
	{
	    return mIsRspReq;
	}
	inline void
	setNoRsp()
	{
	    mIsRspReq = false;
	}
#define RPC_SAMPLING_FREQ (1000)

	/**
	 * Start the timer to track the lifecycle of this rpc processing
	 */
	void
	inline startTimer()
	{
	    static uint64_t mIndex = 0;
	    mIngressQueueingDelay = 0;
	    mRpcProcessingTime = 0;
	    mRpcPreProcessingTime = 0;
	    mRpcProcessingQueueTime = 0;
	    mEgressQueueingDelay = 0;
	    mStartTime = 0;
	    mIndex++;

	    if (!IS_TRACING_MONITOR_ENABLED()) return;
	    if ((mIndex % RPC_SAMPLING_FREQ) == 0) {
		mStartTime = Cycles::rdtsc();
	    }
	}

	/**
	 * End the ingress queueing timer and reset the timer
	 * for the next stage
	 */
	inline void endIngressQueueingTimer() {
	    if (!isTracing()) return;

	    uint64_t now = Cycles::rdtsc();
	    mIngressQueueingDelay = now - mStartTime;
	    mStartTime = now;
	}

	/**
	 * Return the amount of time this RPC waiting to be processed
	 */
	inline double getIngressQueueingDelay() {
	    if (!isTracing()) return 0;

	    return Cycles::toPreciseMicroseconds(mIngressQueueingDelay);
	}

	/**
	 * End the rpc preprocessing time and reset the timer
	 * for the next stage
	 */
	inline void endRpcPreProcessingTimer() {
	    if (!isTracing()) return;

	    uint64_t now = Cycles::rdtsc();
	    mRpcPreProcessingTime = now - mStartTime;
	    mStartTime = now;
	}

	/**
	 * Return the amount of time this RPC is being preprocessed
	 */
	inline double getRpcPreProcessingTime() {
	    if (!isTracing()) return 0;

	    return Cycles::toPreciseMicroseconds(mRpcPreProcessingTime);
	}
	/**
	 * End the rpc processing queue time and reset the timer
	 * for the next stage
	 */
	inline void endRpcProcessingQueueTimer() {
	    if (!isTracing()) return;

	    uint64_t now = Cycles::rdtsc();
	    mRpcProcessingQueueTime = now - mStartTime;
	    mStartTime = now;
	}
	/**
	 * Return the amount of time this RPC is being queued up in the validator
	 */
	inline double getRpcProcessingQueueTime() {
	    if (!isTracing()) return 0;

	    return Cycles::toPreciseMicroseconds(mRpcProcessingQueueTime);
	}
	/**
	 * End the rpc processing time and reset the timer
	 * for the next stage
	 */
	inline void endRpcProcessingTimer() {
	    if (!isTracing()) return;

	    uint64_t now = Cycles::rdtsc();
	    mRpcProcessingTime = now - mStartTime;
	    mStartTime = now;
	}
	/**
	 * Return the amount of time this RPC is being processed
	 */
	inline double getRpcProcessingTime() {
	    if (!isTracing()) return 0;

	    return Cycles::toPreciseMicroseconds(mRpcProcessingTime);
	}

	/**
	 * End the egress queueing timer
	 */
	inline void endEgressQueueingTimer() {
	    if (!isTracing()) return;

	    uint64_t now = Cycles::rdtsc();
	    mEgressQueueingDelay = now - mStartTime;
	    mStartTime = now;
	}
	/**
	 * Return the amount of time this RPC waiting to be sent out
	 */
	inline double getEgressQueueingDelay() {
	    if (!isTracing()) return 0;

	    return Cycles::toPreciseMicroseconds(mEgressQueueingDelay);
	}
	/**
	 * Start the worker handoff Timer for this RPC
	 */
	inline void startWorkerHandoffTimer() {
	    if (!isTracing()) return;
	    mWorkerHandoffTs = Cycles::rdtsc();
	}
	/**
	 * End the worker handoff Timer for this RPC
	 */
	inline void endWorkerHandoffTimer() {
	    if (!isTracing()) return;
	    mWorkerHandoffDelay = Cycles::rdtsc() - mWorkerHandoffTs;
	}
	/**
	 * Return the amount of time it takes for Dispatcher to handoff the
	 * RPC request to the Worker Thread
	 */
	inline double getWorkerHandoffDelay() {
	    if (!isTracing()) return 0;

	    return Cycles::toPreciseMicroseconds(mWorkerHandoffDelay);
	}
	/**
	 * Return the total time this RPC spent in the system
	 */
	inline double getTotalLatency() {
	    if (!isTracing()) return 0;

	    if (mEgressQueueingDelay) {
	        return Cycles::toPreciseMicroseconds(mIngressQueueingDelay +
						     mRpcPreProcessingTime +
						     mRpcProcessingQueueTime +
						     mRpcProcessingTime +
						     mEgressQueueingDelay);
	    }
	    return 0;
	}
        /**
         * The incoming RPC payload, which contains a request.
         */
        Buffer requestPayload;

        /**
         * The RPC payload to send as a response with #sendReply().
         */
        Buffer replyPayload;

        /**
         * The epoch of this RPC upon reception. ServerRpcPool will set this
         * value automatically, tagging the incoming RPC before it is passed to
         * the handling service's dispatch method. This number can be used later
         * to determine if any RPCs less than or equal to a certain epoch are
         * still outstanding in the system.
         */
        uint64_t epoch;

        /**
         * A bit mask indicating what sorts of actions are being performed
         * during this RPC (default: ~0, which means all activities).
         * Individual RPCs can replace the default with a more selective
         * value so that the RPCs will be ignored in some cases when
         * scanning epochs. 0 means the RPC isn't doing anything that
         * matters to anyone.
         */
        int activities;

        /**
         * Bit values for activities above.
         * READ_ACTIVITY:             RPC is reading log information
         * APPEND_ACTIVITY:           RPC may add new entries to the log
         */
        static const int READ_ACTIVITY = 1;
        static const int APPEND_ACTIVITY = 2;

        /**
         * Hook for the list of active server RPCs that the ServerRpcPool class
         * maintains. RPCs are added when ServerRpc-derived classes are
         * constructed by ServerRpcPool and removed when they're destroyed.
         */
        IntrusiveListHook outstandingRpcListHook;
	uint64_t mIngressQueueingDelay;
	uint64_t mWorkerHandoffTs;
	uint64_t mWorkerHandoffDelay;
	uint64_t mRpcPreProcessingTime;
	uint64_t mRpcProcessingQueueTime;
	uint64_t mRpcProcessingTime;
	uint64_t mEgressQueueingDelay;
      PRIVATE:
	/**
	 * The start time of the RPC operation
	 */
	std::atomic<uint64_t> mStartTime;
	bool mIsRspReq;
        DISALLOW_COPY_AND_ASSIGN(ServerRpc);
    };

    /**
     * A handle to send RPCs to a particular service.
     */
    class Session {
      public:
        explicit Session(const string& serviceLocator)
            : refCount(0)
            , serviceLocator(serviceLocator)
        {}

        virtual ~Session() {
            assert(refCount == 0);
        }

        /**
         * This method is invoked via the boost intrusive_ptr mechanism when
         * all copies of the SessionRef for this session have been deleted; it
         * should reclaim the storage for the session.  This method is invoked
         * (rather than just deleting the object) to enable transport-specific
         * memory allocation for sessions.  The default is to delete the
         * Session object.
         */
        virtual void release() {
            delete this;
        }

        /**
         * Initiate the transmission of an RPC request to the server.
         * \param request
         *      The RPC request payload to send. The caller must not modify or
         *      even access \a request until notifier's completed or failed
         *      method has been invoked.
         * \param[out] response
         *      A Buffer that will be filled in with the RPC response. The
         *      transport will clear any existing contents.  The caller must
         *      not access \a response until \a notifier's \c completed or
         *      \c failed method has been invoked.
         * \param notifier
         *      This object will be invoked when the RPC has completed or
         *      failed. It also serves as a unique identifier for the RPC,
         *      which can be passed to cancelRequest.
         */
        virtual void sendRequest(Buffer* request, Buffer* response,
                RpcNotifier* notifier) {}

        /**
         * Cancel an RPC request that was sent previously.
         * \param notifier
         *      Notifier object associated with this request (was passed
         *      to #sendRequest when the RPC was initiated).
         */
        virtual void cancelRequest(RpcNotifier* notifier) {}

        /**
         * Returns a human-readable string containing useful information
         * about any active RPC(s) on this session.
         */
        virtual string getRpcInfo() {
            return format("unknown RPC(s) on %s", serviceLocator.c_str());
        }

        /**
         * Shut down this session: abort any RPCs in progress and reject
         * any future calls to \c sendRequest. The caller is responsible
         * for logging the reason for the abort.
         */
        virtual void abort() {}

        friend void intrusive_ptr_add_ref(Session* session);

        friend void intrusive_ptr_release(Session* session);

      PROTECTED:
        std::atomic<int> refCount;      /// Count of SessionRefs that exist
                                        /// for this Session.

      public:
        /// The service locator this Session is connected to.
        const string serviceLocator;

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(Session);
    };

    /**
     * A reference to a #Session object on which to send client RPC requests.
     * Usage is automatically tracked by boost::intrusive_ptr, so this can
     * be copied freely.  When the last copy is deleted the transport is
     * invoked to reclaim the session storage.
     *
     * Note: the reference count management in this class is thread-safe
     * (but normal Transport::Session objects are not: they should be
     * manipulated only of the dispatch thread).
     */
    typedef boost::intrusive_ptr<Session> SessionRef;

    /**
     * An RpcNotifier is an object that transports use to notify higher-level
     * software when an RPC has completed.  The RpcNotifier also serves as a
     * unique identifier for the RPC, which can be used to cancel it.
     *
     * There are two RPC mode.
     * - Request/Response RPC
     * - Request only (Notification) RPC
     */
    class RpcNotifier {
      public:
        explicit RpcNotifier() {
	    isRspRequired = true;
	}
        explicit RpcNotifier(bool required_rsp)
	  : isRspRequired(required_rsp)
        {}
        virtual ~RpcNotifier() {}
        virtual void completed();
        virtual void failed();
	bool requiredRsp() { return isRspRequired; }
      PROTECTED:
	bool isRspRequired;
        DISALLOW_COPY_AND_ASSIGN(RpcNotifier);
    };

    /**
     * Constructor for Transport.
     */
    Transport() {}

    /**
     * Destructor for Transport.
     */
    virtual ~Transport() {}

    /**
     * Return a session that will communicate with the given service locator.
     * This function is normally only invoked by TransportManager; clients
     * call TransportManager::getSession.
     *
     * \param serviceLocator
     *      Identifies the server this session will communicate with.
     * \param timeoutMs
     *      If the server becomes nonresponsive for this many milliseconds
     *      the session will be aborted.  0 means the session will pick a
     *      suitable default value.
     * \throw NoSuchKeyException
     *      Service locator option missing.
     * \throw BadValueException
     *      Service locator option malformed.
     * \throw TransportException
     *      The transport can't open a session for \a serviceLocator.
     */
    virtual SessionRef getSession(const ServiceLocator* serviceLocator,
            uint32_t timeoutMs = 0) = 0;

    /**
     * Return the ServiceLocator for this Transport. If the transport
     * was not provided static parameters (e.g. fixed TCP or UDP port),
     * this function will return a SerivceLocator with those dynamically
     * allocated attributes.
     *
     * Enlisting the dynamic ServiceLocator with the Coordinator permits
     * other hosts to contact dynamically addressed services.
     */
    virtual string getServiceLocator() = 0;

    /**
     * Register a permanently mapped region of memory. This is a hint to
     * the transport that identifies regions of memory that can be used
     * as zero-copy source buffer for transmission.
     * The Dispatch lock must be held by the caller for the duration of this
     * function.
     * \param[in] base
     *      The base address of the region.
     * \param[in] bytes
     *      The length of the region in bytes.
     * \bug A real solution requires some means of locking the region (or
     *      a subset thereof) for updates so long as a Transport is using it.
     */
    virtual void registerMemory(void* base, size_t bytes) {};

    /// Dump out performance and debugging statistics.
    virtual void dumpStats() {}

    /// Default timeout for transports (individual transports can choose to
    /// use their own default instead of this).  This is the total time
    /// after which a session will be aborted if there has been no sign of
    /// life from the server. Transports may use shorter internal timeouts
    /// to trigger retransmissions.  The current value for this was chosen
    /// somewhat subjectively in 11/2011, based on the presence of time gaps
    /// in the poller of as much as 250ms.
    static const uint32_t DEFAULT_TIMEOUT_MS = 500;

    /**
     * One ServerPort instance is created for a listen port
     * of the transport on the server.
     * It keeps track the port liveness with watchdog timer,
     *
     * When associated client port is closed or the client crashes,
     * the liveness watchdog cleans up the associated server port.
     *
     * Each serverPort instance is freed with 'delete self' at
     * the watchdog timeout.
     * So, severPort has to be dynamically instanciated to avoid
     * 'double free' error.
     *
     **/
    class ServerPort {
      public:
        explicit ServerPort() {}
        virtual ~ServerPort() {}
        /**
         * Close this server port: shutdown and reject any future
         * any future request. The caller is responsible
         * for logging the reason for the close.
         */
        virtual void close() {}
        virtual const string getPortName() const
        {
            return "Error: No transport assigned to this port.";
        }
      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(ServerPort);
    };

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(Transport);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_TRANSPORT_H
