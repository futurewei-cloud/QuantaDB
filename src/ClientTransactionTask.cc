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

/* Copyright (c) 2015-2016 Stanford University
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

#include "ClientLeaseAgent.h"
#include "ClientTransactionManager.h"
#include "ClientTransactionTask.h"
#include "Context.h"
#include "ObjectFinder.h"
#include "RamCloud.h"
#include "RpcTracker.h"
#include "ShortMacros.h"
#include <bitset>
#include <unordered_map>

namespace RAMCloud {

/**
 * Constructor for a transaction task.
 *
 * \param ramcloud
 *      Overall information about the calling client.
 */
ClientTransactionTask::ClientTransactionTask(RamCloud* ramcloud)
    : ramcloud(ramcloud)
    , mMeta({QDB_MD_INITIAL,QDB_MD_INFINITY,QDB_MD_INITIAL})
    , readOnly(true)
    , participantCount(0)
    , participantList()
    , state(INIT)
    , decision(WireFormat::TxDecision::UNDECIDED)
    , lease()
    , txId(0)
    , prepareRpcs()
    , decisionRpcs()
    , commitCache()
    , nextCacheEntry()
    , startTime()
{
    RAMCLOUD_TEST_LOG("Constructor called.");
}

/**
 * Find and return the cache entry identified by the given key.
 *
 * \param key
 *      Key of the object contained in the cache entry that should be returned.
 * \return
 *      Returns a pointer to the cache entry if found.  Returns NULL otherwise.
 *      Pointer is invalid once the commitCache is modified.
 */
ClientTransactionTask::CacheEntry*
ClientTransactionTask::findCacheEntry(Key& key)
{
    CacheKey cacheKey = {key.getTableId(), key.getHash()};
    CommitCacheMap::iterator it = commitCache.lower_bound(cacheKey);
    CacheEntry* entry = NULL;

    while (it != commitCache.end()) {
        if (cacheKey < it->first) {
            break;
        }

        Key otherKey(it->first.tableId,
                     it->second.objectBuf.getKey(),
                     it->second.objectBuf.getKeyLength());
        if (key == otherKey) {
            entry = &it->second;
            break;
        }

        it++;
    }
    return entry;
}

/**
 * Inserts a new cache entry with the provided key and value.  Other members
 * of the cache entry are left to their default values.  This method must not
 * be called once the transaction has started committing.
 *
 * \param key
 *      Key of the object to inserted into the cache.
 * \param buf
 *      Address of the first byte of the new contents for the object;
 *      must contain at least length bytes.
 * \param length
 *      Size in bytes of the new contents for the object.
 * \return
 *      Returns a pointer to the inserted cache entry.  Pointer is invalid
 *      once the commitCache is modified.
 */
ClientTransactionTask::CacheEntry*
ClientTransactionTask::insertCacheEntry(Key& key, const void* buf,
        uint32_t length)
{
    CacheKey cacheKey = {key.getTableId(), key.getHash()};
    CommitCacheMap::iterator it = commitCache.emplace(std::piecewise_construct,
            std::forward_as_tuple(cacheKey),
            std::forward_as_tuple());
    Object::appendKeysAndValueToBuffer(
            key, buf, length, &it->second.objectBuf, true);
    return &it->second;
}

/**
 * Make incremental progress toward committing the transaction.  This method
 * is called during the poll loop when this task needs to make progress (i.e.
 * if the transaction is in the process of committing).
 */
void
ClientTransactionTask::performTask()
{
    try {
        if (state == INIT) {
            startTime = Cycles::rdtsc();

            // Build participant list
#ifdef QDBTX
	    initTaskQDB();
#else
            initTask();
#endif
            nextCacheEntry = commitCache.begin();
            state = PREPARE;
        }
        if (state == PREPARE) {
            sendPrepareRpc();
            processPrepareRpcResults();
            if (prepareRpcs.empty() && nextCacheEntry == commitCache.end()) {
                switch (decision) {
#ifdef QDBTX
		   case WireFormat::TxDecision::UNDECIDED:
		        //For DSSN, it doesn't support this mode
		        RAMCLOUD_LOG(ERROR, "Validator returns undecided decision");
			assert(1);
			break;
                    case WireFormat::TxDecision::ABORT:
                        // fall through to declare the
                        // transaction DONE.
                        FALLS_THROUGH_TO
#else
                    case WireFormat::TxDecision::UNDECIDED:
                        // Decide to commit.
                        decision = WireFormat::TxDecision::COMMIT;
                        TEST_LOG("Set decision to COMMIT.");
                        // NO break; fall through to continue with commit.
                        FALLS_THROUGH_TO
                    case WireFormat::TxDecision::ABORT:
                        // If not READ-ONLY, move to decision phase.
                        if (!readOnly) {
                            nextCacheEntry = commitCache.begin();
                            state = DECISION;
                            TEST_LOG("Move from PREPARE to DECISION phase.");
                            break;
                        }
                        // else NO break; fall through to declare the
                        // transaction DONE.
                        FALLS_THROUGH_TO
#endif
                    case WireFormat::TxDecision::COMMIT:
                        // Prepare must have returned COMMITTED or was READ-ONLY
                        // so the transaction is now done.
                        ramcloud->rpcTracker->rpcFinished(txId);
                        state = DONE;
                        TEST_LOG("Move from PREPARE to DONE phase; optimized.");
                        break;
                    default:
                        RAMCLOUD_LOG(ERROR, "Unexpected transaction decision "
                                "value in transaction %lu.%lu.",
                                     lease.leaseId, txId);
                        ClientException::throwException(HERE,
                                                        STATUS_INTERNAL_ERROR);
                }
            }
        }
        if (state == DECISION) {
            sendDecisionRpc();
            processDecisionRpcResults();
            if (decisionRpcs.empty() && nextCacheEntry == commitCache.end()) {
                ramcloud->rpcTracker->rpcFinished(txId);
                state = DONE;
            }
        }
    } catch (ClientException& e) {
        // If there are any unexpected problems with the commit protocol, STOP.
        // This shouldn't happen unless there is a bug.
        prepareRpcs.clear();
        decisionRpcs.clear();
        switch (state) {
            case INIT:
            case PREPARE:
                // If there is an error during the prepare, the "decision" that
                // is currently set may be in error.  Reset the decision to
                // UNDECIDED to signal the error.
                decision = WireFormat::TxDecision::UNDECIDED;
                RAMCLOUD_LOG(ERROR, "Unexpected exception '%s' while preparing "
                        "to commit transaction %lu.%lu; will result in "
                        "internal error.",
                        statusToString(e.status), lease.leaseId, txId);
                break;
            case DECISION:
                RAMCLOUD_LOG(WARNING, "Unexpected exception '%s' while issuing "
                        "decisions for transaction %lu.%lu; likely "
                        "recoverable.",
                        statusToString(e.status), lease.leaseId, txId);
                break;
            case DONE:
                RAMCLOUD_LOG(NOTICE, "Unexpected exception '%s' after "
                        "committing transaction %lu.%lu.",
                        statusToString(e.status), lease.leaseId, txId);
                break;
            default:
                // This case should be unreachable.
                RAMCLOUD_LOG(ERROR, "Unexpected exception '%s' while "
                        "transaction %lu.%lu was in an invalid state; this "
                        "case should not be reachable.",
                        statusToString(e.status), lease.leaseId, txId);
                break;
        }
        ramcloud->rpcTracker->rpcFinished(txId);
        state = DONE;
    }
}

/**
 * Initialize all necessary values of the commit task in preparation for the
 * commit protocol.  This includes building the send-ready buffer of
 * participants to be included in every prepare rpc and also the allocation of
 * rpcIds.  Used in the commit method.  Factored out mostly for ease of testing.
 */
void
ClientTransactionTask::initTask()
{
    lease = ramcloud->clientLeaseAgent->getLease();
    // First RPC id is used to identify the transaction.  One additional RPC
    // id is needed for each operation in the transation.
    txId = ramcloud->rpcTracker->newRpcIdBlock(this, commitCache.size() + 1);

    nextCacheEntry = commitCache.begin();
    uint64_t i = 0;
    while (nextCacheEntry != commitCache.end()) {
        const CacheKey* key = &nextCacheEntry->first;
        CacheEntry* entry = &nextCacheEntry->second;

        entry->rpcId = txId + (++i);
        participantList.emplaceAppend<WireFormat::TxParticipant>(
                key->tableId,
                static_cast<uint64_t>(key->keyHash),
                entry->rpcId);
        participantCount++;
        nextCacheEntry++;
    }
    assert(i == commitCache.size());
}

void
ClientTransactionTask::initTaskQDB()
{
    std::bitset<4096> participantSet;
    lease = ramcloud->clientLeaseAgent->getLease();
    // First RPC id is used to identify the transaction.  One additional RPC
    // id is needed for each operation in the transation.
    txId = ramcloud->rpcTracker->newRpcIdBlock(this, commitCache.size() + 1);

    nextCacheEntry = commitCache.begin();
    uint64_t i = 0;
    while (nextCacheEntry != commitCache.end()) {
        const CacheKey* key = &nextCacheEntry->first;
	CacheEntry* entry = &nextCacheEntry->second;
	entry->rpcId = txId + (++i);
	uint64_t serverId = ramcloud->clientContext->objectFinder->lookupTablet(key->tableId, key->keyHash)->tablet.serverId.getId();

	if (!participantSet[serverId]) {
	    participantList.emplaceAppend<WireFormat::TxParticipant>(
		  key->tableId,
		  static_cast<uint64_t>(key->keyHash),
		  serverId);
	    participantCount++;
	    participantSet[serverId] = true;
	}
        nextCacheEntry++;
    }
    assert(participantCount > 0);
    dssnCTS = ramcloud->getCTS();
}
/**
 * Process any decision rpcs that have completed.  Used in performTask.
 * Factored out mostly for clarity and ease of testing.
 */
void
ClientTransactionTask::processDecisionRpcResults()
{
    // Process outstanding RPCs.
    std::list<DecisionRpc>::iterator it = decisionRpcs.begin();
    for (; it != decisionRpcs.end(); it++) {
        DecisionRpc* rpc = &(*it);

        if (!rpc->isReady()) {
            continue;
        }

        try {
            rpc->wait();
            // At this point the decision must have been received successfully.
            // Nothing left to do.
            TEST_LOG("STATUS_OK");
        } catch (UnknownTabletException& e) {
            // Target server did not contain the requested tablet; the
            // operations should have been already marked for retry. Nothing
            // left to do.
            TEST_LOG("STATUS_UNKNOWN_TABLET");
        } catch (ServerNotUpException& e) {
            // If the target server is not up; the operations should have been
            // already marked for retry.  Nothing left to do.
            TEST_LOG("STATUS_SERVER_NOT_UP");
        }

        // Destroy object.
        it = decisionRpcs.erase(it);
    }
}

/**
 * Process any prepare rpcs that have completed.  Used in performTask.  Factored
 * out mostly for clarity and ease of testing.
 */
void
ClientTransactionTask::processPrepareRpcResults()
{
    // Process outstanding RPCs.
    std::list<PrepareRpc>::iterator it = prepareRpcs.begin();
    for (; it != prepareRpcs.end(); it++) {
        PrepareRpc* rpc = &(*it);

        if (!rpc->isReady()) {
            continue;
        }

        try {
            using WireFormat::TxPrepare;
            using WireFormat::TxDecision;

            TxPrepare::Vote newVote = rpc->wait();
            switch (newVote) {
                case TxPrepare::PREPARED:
                    // Wait for other prepare requests to complete;
                    // nothing to do for this rpc.
                    TEST_LOG("PREPARED");
                    break;
                case TxPrepare::COMMITTED:
                    // Note the transaction has COMMITTED (as long as the
                    // transaction did not previously decided to abort).
                    if (expect_true(decision != TxDecision::ABORT)) {
                        decision = TxDecision::COMMIT;
                    } else {
                        // Possible Byzantine failure detected; do not continue.
                        RAMCLOUD_LOG(ERROR, "Transaction %lu.%lu found "
                                "TxPrepare claiming to have COMMITTED after "
                                "ABORT already received.", lease.leaseId, txId);
                        ClientException::throwException(HERE,
                                                        STATUS_INTERNAL_ERROR);
                    }
                    break;
                case TxPrepare::ABORT_REQUESTED:
                    // Recovery was triggered before this commit process
                    // completed which means we should ABORT.  Split into its
                    // own case to detect ABORTs due to recovery timeouts.
                    if (decision != TxDecision::ABORT) {
                        double detectionTime =
                                Cycles::toSeconds(Cycles::rdtsc() - startTime);
                        RAMCLOUD_LOG(WARNING, "Transaction %lu.%lu consisting "
                                "of %u operation(s) aborted after %.1f us "
                                "because the commit took longer than expected.",
                                lease.leaseId, txId, participantCount,
                                detectionTime * 1e06);
                    }
                    // NO break; fall through to perform actual ABORT work.
                    FALLS_THROUGH_TO
                case TxPrepare::ABORT:
                    // Decide the transaction should ABORT (as long as the
                    // transaction has not already committed).
                    if (expect_true(decision != TxDecision::COMMIT)) {
                        decision = TxDecision::ABORT;
                    } else {
                        // Possible Byzantine failure detected; do not continue.
                        RAMCLOUD_LOG(ERROR,
                                "Transaction %lu.%lu detected TxPrepare trying "
                                "to ABORT after COMMITTED.",
                                lease.leaseId, txId);
                        ClientException::throwException(HERE,
                                                        STATUS_INTERNAL_ERROR);
                    }
                    break;
                default:
                    // Possible Byzantine failure detected; do not continue.
                    RAMCLOUD_LOG(ERROR, "Unexpected result from TxPrepare in "
                            "transaction %lu.%lu.",
                            lease.leaseId, txId);
                    ClientException::throwException(HERE,
                                                    STATUS_INTERNAL_ERROR);
            }
        } catch (UnknownTabletException& e) {
            // Target server did not contain the requested tablet; the
            // operations should have been already marked for retry. Nothing
            // left to do.
            TEST_LOG("STATUS_UNKNOWN_TABLET");
        } catch (ServerNotUpException& e) {
            // If the target server is not up; the operations should have been
            // already marked for retry.  Nothing left to do.
            TEST_LOG("STATUS_SERVER_NOT_UP");
        }

        // Destroy object.
        it = prepareRpcs.erase(it);
    }
#if QDBTX
    if (decision != WireFormat::TxDecision::UNDECIDED) {
        /*
	 * One of the participants of the transaction has already voted.  For DSSN,
	 * the client can now conclude the transaction without getting reply from all.
	 */
        it = prepareRpcs.begin();
        for (; it != prepareRpcs.end(); it++) {
	    prepareRpcs.erase(it);
	}
	//RAMCLOUD_LOG(NOTICE, "Decision has been made, Skip response from other server");
    }
#endif
}

/**
 * Send out a batch of un-sent decision notifications as a single DecisionRpc
 * if not all masters have been notified.  Used in performTask.  Factored out
 * mostly for clarity and ease of testing.
 */
void
ClientTransactionTask::sendDecisionRpc()
{
    DecisionRpc* nextRpc = NULL;
    Transport::SessionRef rpcSession;
    for (; nextCacheEntry != commitCache.end(); nextCacheEntry++) {
        const CacheKey* key = &nextCacheEntry->first;
        CacheEntry* entry = &nextCacheEntry->second;

        // Skip the entry if the decision was already sent.  This might happen
        // when an RPC receives STATUS_RETRY and we need to look through all
        // the entries again looking for entries that have been marked PENDING
        // indicating the decisions need to be resent; entries not marked don't
        // need to be resent.
        if (entry->state == CacheEntry::DECIDE) {
            continue;
        }

        // Batch is done naively assuming that tables are partitioned across
        // servers into contiguous key-hash ranges (tablets).  The commit cache
        // is iterated in key-hash order batching together decisions
        // notifications that share a destination server.
        //
        // This naive approach behaves poorly if the table is highly sharded
        // resulting in poor batching.
        if (nextRpc == NULL) {
            rpcSession =
                    ramcloud->clientContext->objectFinder->lookup(key->tableId,
                                                                  key->keyHash);
            decisionRpcs.emplace_back(ramcloud, rpcSession, this);
            nextRpc = &decisionRpcs.back();
        }

        Transport::SessionRef session =
                ramcloud->clientContext->objectFinder->lookup(key->tableId,
                                                              key->keyHash);
        if (session->serviceLocator != rpcSession->serviceLocator
                || !nextRpc->appendOp(nextCacheEntry)) {
            break;
        }
    }
    if (nextRpc) {
        nextRpc->send();
    }
}

/**
 * Send out a batch of un-sent prepare requests in a single PrepareRpc if there
 * are remaining un-prepared transaction ops.  Used in performTask.  Factored
 * out mostly for clarity and ease of testing.
 */
void
ClientTransactionTask::sendPrepareRpc()
{
    PrepareRpc* rpc = NULL;
    Transport::SessionRef rpcSession;
    std::unordered_map<std::string, PrepareRpc*> rpcTable;
    std::unordered_map<std::string, PrepareRpc*>::const_iterator result;

    for (; nextCacheEntry != commitCache.end(); nextCacheEntry++) {
        const CacheKey* key = &nextCacheEntry->first;
        CacheEntry* entry = &nextCacheEntry->second;

        // Skip the entry if the prepare was already sent.  This might happen
        // when an RPC receives STATUS_RETRY and we need to look through all
        // the entries again looking for entries that have been marked PENDING
        // indicating the prepares need to be resent; entries not marked don't
        // need to be resent.
        if (entry->state == CacheEntry::PREPARE) {
            continue;
        }
	rpcSession =
	  ramcloud->clientContext->objectFinder->lookup(key->tableId,
							key->keyHash);
	result = rpcTable.find(rpcSession->serviceLocator);

	if (result != rpcTable.end()) {
	    rpc = result->second;
	    if (!rpc->appendOp(nextCacheEntry)) {
#if QDBTX
	        //Exit, DSSN Validator expect 1 commit request per cts per server
	        RAMCLOUD_LOG(ERROR, "Exceeded the payload of the RPC");
		exit(1);
#endif
	        rpc->send();
		//Allocate a new one
		prepareRpcs.emplace_back(ramcloud, rpcSession, this);
		rpc = &prepareRpcs.back();
		rpcTable[rpcSession->serviceLocator] = rpc;
		rpc->appendOp(nextCacheEntry);
	    }
	} else {
	    prepareRpcs.emplace_back(ramcloud, rpcSession, this);
            rpc = &prepareRpcs.back();
	    rpcTable[rpcSession->serviceLocator] = rpc;
	    rpc->appendOp(nextCacheEntry);
	}
    }
    // Finish iterating through the list of KV set, now we can send all the RPCs out
    for (auto iter = rpcTable.begin(); iter != rpcTable.end(); iter++) {
        rpc = iter->second;
	rpc->send();
    }

}

// See RpcTracker::TrackedRpc for documentation.
void ClientTransactionTask::tryFinish()
{
    // Making forward progress requires the follow:
    //  (1) Calling performTask (by calling poll on the manager)
    //  (2) Allowing the transport to run by calling poll
    ramcloud->transactionManager->poll();
    ramcloud->poll();
}

/**
 * Constructor for ClientTransactionRpcWrapper.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param session
 *      Session on which this RPC will eventually be sent.
 * \param task
 *      Pointer to the transaction task that issued this request.
 * \param responseHeaderLength
 *      The size of header expected in the response for this RPC;
 *      incoming responses will be checked by this class to ensure that
 *      they contain at least this much data, wrapper subclasses can
 *      use the getResponseHeader method to access the response header
 *      once isReady has returned true.
 */
ClientTransactionTask::ClientTransactionRpcWrapper::ClientTransactionRpcWrapper(
        RamCloud* ramcloud,
        Transport::SessionRef session,
        ClientTransactionTask* task,
        uint32_t responseHeaderLength)
    : RpcWrapper(responseHeaderLength)
    , ramcloud(ramcloud)
    , task(task)
    , ops()
{
    this->session = session;
}

// See RpcWrapper for documentation.
bool
ClientTransactionTask::ClientTransactionRpcWrapper::checkStatus()
{
    if (responseHeader->status == STATUS_UNKNOWN_TABLET) {
        markOpsForRetry();
    }
    return true;
}

// See RpcWrapper for documentation.
bool
ClientTransactionTask::ClientTransactionRpcWrapper::handleTransportError()
{
    // There was a transport-level failure. Flush cached state related
    // to this session, and related to the object mappings.  The objects
    // will all be retried when \c finish is called.
    if (session.get() != NULL) {
        ramcloud->clientContext->transportManager->flushSession(
                session->serviceLocator);
        session = NULL;
    }
    markOpsForRetry();
    return true;
}

// See RpcWrapper for documentation.
void
ClientTransactionTask::ClientTransactionRpcWrapper::send()
{
    state = IN_PROGRESS;
    session->sendRequest(&request, response, this);
}

/**
 * Constructor for a DecisionRpc.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param session
 *      Session on which this RPC will eventually be sent.
 * \param task
 *      Pointer to the transaction task that issued this request.
 */
ClientTransactionTask::DecisionRpc::DecisionRpc(RamCloud* ramcloud,
        Transport::SessionRef session,
        ClientTransactionTask* task)
    : ClientTransactionRpcWrapper(ramcloud,
                                  session,
                                  task,
                                  sizeof(WireFormat::TxDecision::Response))
#ifdef QDBTX
    , reqHdr(allocHeader<WireFormat::TxDecisionDSSN>())
#else
    , reqHdr(allocHeader<WireFormat::TxDecision>())
#endif
{
    reqHdr->decision = task->decision;
    reqHdr->leaseId = task->lease.leaseId;
    reqHdr->transactionId = task->txId;
    reqHdr->recovered = false;
    reqHdr->participantCount = 0;
}

/**
 * Append an operation to the end of this decision rpc.
 *
 * \param opEntry
 *      Handle to information about the operation to be appended.
 * \return
 *      True if the op was successfully appended; false otherwise.
 */
bool
ClientTransactionTask::DecisionRpc::appendOp(CommitCacheMap::iterator opEntry)
{
    if (reqHdr->participantCount >= DecisionRpc::MAX_OBJECTS_PER_RPC) {
        return false;
    }

    const CacheKey* key = &opEntry->first;
    CacheEntry* entry = &opEntry->second;

    request.emplaceAppend<WireFormat::TxParticipant>(
            key->tableId,
            static_cast<uint64_t>(key->keyHash),
            entry->rpcId);

    entry->state = CacheEntry::DECIDE;
    ops[reqHdr->participantCount] = opEntry;
    reqHdr->participantCount++;
    return true;
}

/**
 * Wait for the Decision RPC to be acknowledged.
 *
 * \throw ServerNotUpException
 *      The intended server for this RPC is not part of the cluster; if it ever
 *      existed, it has since crashed.  Operations have been marked for retry;
 *      caller can and should discard this RPC.
 * \throw UnknownTabletException
 *      The target server is not the owner of one or more of the included
 *      operations.  This could have occurred due to an out of date tablet map.
 *      Operations have been marked for retry; caller can and should discard
 *      this RPC.
 */
void
ClientTransactionTask::DecisionRpc::wait()
{
    waitInternal(ramcloud->clientContext->dispatch);

    if (getState() == FAILED) {
        // Target server was not reachable. Retry has already been arranged.
        throw ServerNotUpException(HERE);
    } else if (responseHeader->status != STATUS_OK) {
        ClientException::throwException(HERE, responseHeader->status);
    }
}

/**
 * This method is invoked when a decision RPC couldn't complete successfully. It
 * arranges for prepares to be tried again for all of the participant objects in
 * that request.
 */
void
ClientTransactionTask::DecisionRpc::markOpsForRetry()
{
    for (uint32_t i = 0; i < reqHdr->participantCount; i++) {
        const CacheKey* key = &ops[i]->first;
        CacheEntry* entry = &ops[i]->second;
        ramcloud->clientContext->objectFinder->flush(key->tableId);
        entry->state = CacheEntry::PENDING;
    }
    task->nextCacheEntry = task->commitCache.begin();
}

/**
 * Constructor for PrepareRpc.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param session
 *      Session on which this RPC will eventually be sent.
 * \param task
 *      Pointer to the transaction task that issued this request.
 */
ClientTransactionTask::PrepareRpc::PrepareRpc(RamCloud* ramcloud,
					      Transport::SessionRef session,
					      ClientTransactionTask* task)
    : ClientTransactionRpcWrapper(ramcloud,
                                  session,
                                  task,
                                  sizeof(WireFormat::TxDecision::Response))
#ifdef QDBTX
    , reqHdr(allocHeader<WireFormat::TxCommitDSSN>())
#else
    , reqHdr(allocHeader<WireFormat::TxPrepare>())
#endif
{
    reqHdr->clientTxId = task->txId;
    reqHdr->ackId = ramcloud->rpcTracker->ackId();
    reqHdr->participantCount = task->participantCount;
    reqHdr->opCount = 0;
    reqHdr->readOpCount = 0;
    request.appendExternal(&task->participantList);
#ifdef QDBTX
    reqHdr->meta.pstamp = task->mMeta.pstamp;
    reqHdr->meta.sstamp = task->mMeta.sstamp;
    reqHdr->meta.cts = task->dssnCTS;
#else
    reqHdr->lease = task->lease;
#endif
}

/**
 * Append an operation to the end of this prepare rpc.
 *
 * \param opEntry
 *      Handle to information about the operation to be appended.
 * \return
 *      True if the op was successfully appended; false otherwise.
 */
bool
ClientTransactionTask::PrepareRpc::appendOp(CommitCacheMap::iterator opEntry)
{
    if (reqHdr->opCount >= PrepareRpc::MAX_OBJECTS_PER_RPC) {
        return false;
    }

    const CacheKey* key = &opEntry->first;
    CacheEntry* entry = &opEntry->second;

    switch (entry->type) {
        case CacheEntry::READ:
            request.emplaceAppend<WireFormat::TxPrepare::Request::ReadOp>(
                    key->tableId, entry->rpcId,
                    entry->objectBuf.getKeyLength(), entry->rejectRules,
                    task->readOnly);
            request.appendExternal(entry->objectBuf.getKey(),
                    entry->objectBuf.getKeyLength());
	    reqHdr->readOpCount++;
            break;
        case CacheEntry::REMOVE:
            request.emplaceAppend<WireFormat::TxPrepare::Request::RemoveOp>(
                    key->tableId, entry->rpcId,
                    entry->objectBuf.getKeyLength(), entry->rejectRules);
            request.appendExternal(entry->objectBuf.getKey(),
                    entry->objectBuf.getKeyLength());
            break;
        case CacheEntry::WRITE:
            request.emplaceAppend<WireFormat::TxPrepare::Request::WriteOp>(
                    key->tableId, entry->rpcId,
                    entry->objectBuf.size(), entry->rejectRules);
            request.appendExternal(&entry->objectBuf);
            break;
        case CacheEntry::READ_MODIFY_WRITE:
            request.emplaceAppend<WireFormat::TxPrepare::Request::WriteOp>(
                    key->tableId, entry->rpcId,
                    entry->objectBuf.size(), entry->rejectRules, true);
            request.appendExternal(&entry->objectBuf);
            break;
        default:
            RAMCLOUD_LOG(ERROR, "Unknown transaction op type found for "
                    "CacheEntry (%lu : %lu) while attempting to prepare "
                    "transaction %lu.%lu.",
                    key->tableId, key->keyHash,
                    task->lease.leaseId, task->txId);
            return false;
    }

    entry->state = CacheEntry::PREPARE;
    ops[reqHdr->opCount] = opEntry;
    reqHdr->opCount++;
    return true;
}

/**
 * Wait for the Prepare request to complete, and return participant servers
 * vote to either proceed or abort.
 *
 * \return
 *      The participant server's response to the request to prepare the included
 *      transaction operations for commit.  See WireFormat::TxPrepare::Vote for
 *      documentation of possible responses.
 * \throw ServerNotUpException
 *      The intended server for this RPC is not part of the cluster; if it ever
 *      existed, it has since crashed.  Operations have been marked for retry;
 *      caller can and should discard this RPC.
 * \throw UnknownTabletException
 *      The target server is not the owner of one or more of the included
 *      operations.  This could have occurred due to an out of date tablet map.
 *      Operations have been marked for retry; caller can and should discard
 *      this RPC.
 */
WireFormat::TxPrepare::Vote
ClientTransactionTask::PrepareRpc::wait()
{
    waitInternal(ramcloud->clientContext->dispatch);

    if (getState() == FAILED) {
        // Target server was not reachable. Retry has already been arranged.
        throw ServerNotUpException(HERE);
    } else if (responseHeader->status != STATUS_OK) {
        ClientException::throwException(HERE, responseHeader->status);
    }

    WireFormat::TxPrepare::Response* respHdr =
            response->getStart<WireFormat::TxPrepare::Response>();
    return respHdr->vote;
}

/**
 * This method is invoked when a prepare RPC couldn't complete successfully. It
 * arranges for prepares to be tried again for all of the participant objects in
 * that request.
 */
void
ClientTransactionTask::PrepareRpc::markOpsForRetry()
{
    for (uint32_t i = 0; i < reqHdr->opCount; i++) {
        const CacheKey* key = &ops[i]->first;
        CacheEntry* entry = &ops[i]->second;
        ramcloud->clientContext->objectFinder->flush(key->tableId);
        entry->state = CacheEntry::PENDING;
    }
    task->nextCacheEntry = task->commitCache.begin();
}

bool
ClientTransactionTask::isTxValid()
{
    bool result = true;

#ifdef QDBTX
    if (mMeta.sstamp > mMeta.pstamp) {
        return result;
    }
    //Invalid transaction
    state = DONE;
    decision = WireFormat::TxDecision::ABORT;
    result = false;
#endif

    return result;
}

void
ClientTransactionTask::updateSSNReadMeta(WireFormat::QDBXmitMeta& v)
{
    if (v.cstamp > mMeta.pstamp) {
        mMeta.pstamp= v.cstamp;
    }
}

} // namespace RAMCloud
