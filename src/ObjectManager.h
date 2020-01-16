/* Copyright (c) 2014-2016 Stanford University
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

#ifndef RAMCLOUD_OBJECTMANAGER_H
#define RAMCLOUD_OBJECTMANAGER_H

#include "Common.h"
#include "Log.h"
#include "SideLog.h"
#include "LogEntryHandlers.h"
#include "HashTable.h"
#include "IndexKey.h"
#include "Object.h"
#include "ParticipantList.h"
#include "PreparedOp.h"
#include "SegmentManager.h"
#include "SegmentIterator.h"
#include "ReplicaManager.h"
#include "RpcResult.h"
#include "ServerConfig.h"
#include "SpinLock.h"
#include "TabletManager.h"
#include "TransactionManager.h"
#include "TxDecisionRecord.h"
#include "TxRecoveryManager.h"
#include "MasterTableMetadata.h"
#include "UnackedRpcResults.h"
#include "LockTable.h"

namespace RAMCloud {

/**
 * The ObjectManager class is responsible for storing objects in a master
 * server. It is essentially the union of the Log, HashTable, TabletMap,
 * and ReplicaManager classes.
 *
 * Each MasterService instance has a single ObjectManager that encapsulates
 * the details of object storage and consistency. MasterService knows that
 * tablets and objects exist, but is unaware of the details of their storage -
 * it merely translates RPCs to ObjectManager method calls.
 *
 * ObjectManager is thread-safe. Multiple worker threads in MasterService may
 * call into it simultaneously.
 */
class ObjectManager : public LogEntryHandlers,
                      public AbstractLog::ReferenceFreer {
  public:
    ObjectManager(Context* context, ServerId* serverId,
		  const ServerConfig* config,
		  TabletManager* tabletManager,
		  MasterTableMetadata* masterTableMetadata,
		  UnackedRpcResults* unackedRpcResults,
		  TransactionManager* transactionManager,
		  TxRecoveryManager* txRecoveryManager)
    : context(context)
    , config(config)
    , tabletManager(tabletManager)
    , masterTableMetadata(masterTableMetadata)
    , unackedRpcResults(unackedRpcResults)
    , transactionManager(transactionManager)
    , txRecoveryManager(txRecoveryManager)
    , allocator(config)
    , replicaManager(context, serverId,
                     config->master.numReplicas,
                     config->master.useMinCopysets,
                     config->master.usePlusOneBackup,
                     config->master.allowLocalBackup)
    , segmentManager(context, config, serverId,
                     allocator, replicaManager, masterTableMetadata)
    , mutex("ObjectManager::mutex")
    , tombstoneProtectorCount(0) { }

    virtual void freeLogEntry(Log::Reference ref) = 0;
    virtual void initOnceEnlisted() = 0;

    virtual void readHashes(const uint64_t tableId, uint32_t reqNumHashes,
                Buffer* pKHashes, uint32_t initialPKHashesOffset,
                uint32_t maxLength, Buffer* response, uint32_t* respNumHashes,
                uint32_t* numObjects) = 0;
    virtual Status readObject(Key& key, Buffer* outBuffer,
                RejectRules* rejectRules, uint64_t* outVersion,
                bool valueOnly = false) = 0;
    virtual Status removeObject(Key& key, RejectRules* rejectRules,
                uint64_t* outVersion, Buffer* removedObjBuffer = NULL,
                RpcResult* rpcResult = NULL, uint64_t* rpcResultPtr = NULL) = 0;
    virtual void removeOrphanedObjects() = 0;
    virtual void replaySegment(SideLog* sideLog, SegmentIterator& it,
                std::unordered_map<uint64_t, uint64_t>* nextNodeIdMap) = 0;
    virtual void replaySegment(SideLog* sideLog, SegmentIterator& it) = 0;
    virtual void syncChanges() = 0;
    virtual Status writeObject(Object& newObject, RejectRules* rejectRules,
                uint64_t* outVersion, Buffer* removedObjBuffer = NULL,
                RpcResult* rpcResult = NULL, uint64_t* rpcResultPtr = NULL) = 0;
    virtual bool keyPointsAtReference(Key& k, AbstractLog::Reference oldReference) = 0;
    virtual void writePrepareFail(RpcResult* rpcResult, uint64_t* rpcResultPtr) = 0;
    virtual void writeRpcResultOnly(RpcResult* rpcResult, uint64_t* rpcResultPtr) = 0;
    virtual Status prepareOp(PreparedOp& newOp, RejectRules* rejectRules,
                uint64_t* newOpPtr, bool* isCommitVote,
                RpcResult* rpcResult, uint64_t* rpcResultPtr) = 0;
    virtual Status prepareReadOnly(PreparedOp& newOp, RejectRules* rejectRules,
                bool* isCommitVote) = 0;
    virtual Status tryGrabTxLock(Object& objToLock, Log::Reference& ref) = 0;
    virtual Status writeTxDecisionRecord(TxDecisionRecord& record) = 0;
    virtual Status commitRead(PreparedOp& op, Log::Reference& refToPreparedOp) = 0;
    virtual Status commitRemove(PreparedOp& op, Log::Reference& refToPreparedOp,
                        Buffer* removedObjBuffer = NULL) = 0;
    virtual Status commitWrite(PreparedOp& op, Log::Reference& refToPreparedOp,
                        Buffer* removedObjBuffer = NULL) = 0;

    /**
     * The following three methods are used when multiple log entries
     * need to be committed to the log atomically.
     */

    virtual bool flushEntriesToLog(Buffer *logBuffer, uint32_t& numEntries) = 0;
    virtual Status prepareForLog(Object& newObject, Buffer *logBuffer,
                uint32_t* offset, bool *tombstoneAdded) = 0;
    virtual Status writeTombstone(Key& key, Buffer *logBuffer) = 0;

    /**
     * The following two methods are used by the log cleaner. They aren't
     * intended to be called from any other modules.
     */
    virtual uint32_t getTimestamp(LogEntryType type, Buffer& buffer) = 0;
    virtual void relocate(LogEntryType type, Buffer& oldBuffer,
                Log::Reference oldReference, LogEntryRelocator& relocator) = 0;

    /**
     * The following methods exist because our current abstraction doesn't quite
     * cut it in terms of hiding object storage information from MasterService.
     * Sometimes MasterService needs to poke at the log, the replica manager, or
     * the object map.
     *
     * If you're considering using these methods, please think twice.
     */
    virtual Log* getLog() = 0;
    virtual ReplicaManager* getReplicaManager() = 0;
    virtual HashTable* getObjectMap() = 0;
    virtual void stopTombstoneRemover() = 0;
    virtual void startTombstoneRemover() = 0;

    /**
     * An object of this class must be held by any activity that places
     * tombstones in the hash table temporarily (e.g., anyone who calls
     * replaySegment). While there exist any of these objects, tombstones
     * will not be removed from the hash table; however, once there are no
     * more objects of this class, a background activity will be initiated
     * to remove the tombstones.
     */
    class TombstoneProtector {
      public:
        TombstoneProtector(ObjectManager* objectManager)
	  : objectManager(objectManager)
        {
	    SpinLock::Guard guard(objectManager->mutex);
	    ++objectManager->tombstoneProtectorCount;
	    //objectManager->tombstoneRemover.stop();
	    objectManager->stopTombstoneRemover();
	}
        ~TombstoneProtector()
	{
	    SpinLock::Guard guard(objectManager->mutex);
	    --objectManager->tombstoneProtectorCount;
	    if (objectManager->tombstoneProtectorCount == 0) {
	      //objectManager->tombstoneRemover.currentBucket = 0;
	      //objectManager->tombstoneRemover.start(0);
	        objectManager->startTombstoneRemover();
	    }
	}


      PRIVATE:
        // Saved copy of the constructor argument.
        ObjectManager* objectManager;

        DISALLOW_COPY_AND_ASSIGN(TombstoneProtector);
    };

  PROTECTED:

    /**
     * Produce a human-readable description of the contents of a segment.
     * Intended primarily for use in unit tests.
     *
     * \param segment
     *       Segment whose contents should be dumped.
     *
     * \result
     *       A string describing the contents of the segment
     */
    static string dumpSegment(Segment* segment)
    {
        const char* separator = "";
	string result;
	SegmentIterator it(*segment);
	while (!it.isDone()) {
	    LogEntryType type = it.getType();
	    if (type == LOG_ENTRY_TYPE_OBJ) {
	        Buffer buffer;
		it.appendToBuffer(buffer);
		Object object(buffer);
		result += format("%sobject at offset %u, length %u with tableId "
				 "%lu, key '%.*s'",
				 separator, it.getOffset(), it.getLength(),
				 object.getTableId(), object.getKeyLength(),
				 static_cast<const char*>(object.getKey()));
	    } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {
	        Buffer buffer;
		it.appendToBuffer(buffer);
		ObjectTombstone tombstone(buffer);
		result += format("%stombstone at offset %u, length %u with tableId "
				 "%lu, key '%.*s'",
				 separator, it.getOffset(), it.getLength(),
				 tombstone.getTableId(),
				 tombstone.getKeyLength(),
				 static_cast<const char*>(tombstone.getKey()));
	    } else if (type == LOG_ENTRY_TYPE_SAFEVERSION) {
	        Buffer buffer;
		it.appendToBuffer(buffer);
		ObjectSafeVersion safeVersion(buffer);
		result += format("%ssafeVersion at offset %u, length %u with "
				 "version %lu",
				 separator, it.getOffset(), it.getLength(),
				 safeVersion.getSafeVersion());
	    } else if (type == LOG_ENTRY_TYPE_RPCRESULT) {
	        Buffer buffer;
		it.appendToBuffer(buffer);
		RpcResult rpcResult(buffer);
		result += format("%srpcResult at offset %u, length %u with tableId "
				 "%lu, keyHash 0x%016" PRIX64 ", leaseId %lu, rpcId %lu",
				 separator, it.getOffset(), it.getLength(),
				 rpcResult.getTableId(), rpcResult.getKeyHash(),
				 rpcResult.getLeaseId(), rpcResult.getRpcId());
	    } else if (type == LOG_ENTRY_TYPE_PREP) {
	        Buffer buffer;
		it.appendToBuffer(buffer);
		PreparedOp op(buffer, 0, buffer.size());
		result += format("%spreparedOp at offset %u, length %u with "
				 "tableId %lu, key '%.*s', leaseId %lu, rpcId %lu",
				 separator, it.getOffset(), it.getLength(),
				 op.object.getTableId(), op.object.getKeyLength(),
				 static_cast<const char*>(op.object.getKey()),
				 op.header.clientId, op.header.rpcId);
	    } else if (type == LOG_ENTRY_TYPE_PREPTOMB) {
	        Buffer buffer;
		it.appendToBuffer(buffer);
		PreparedOpTombstone opTomb(buffer, 0);
		result += format("%spreparedOpTombstone at offset %u, length %u "
				 "with tableId %lu, keyHash 0x%016" PRIX64 ", leaseId %lu, "
				 "rpcId %lu",
				 separator, it.getOffset(), it.getLength(),
				 opTomb.header.tableId, opTomb.header.keyHash,
				 opTomb.header.clientLeaseId, opTomb.header.rpcId);
	    } else if (type == LOG_ENTRY_TYPE_TXDECISION) {
	        Buffer buffer;
		it.appendToBuffer(buffer);
		TxDecisionRecord decisionRecord(buffer);
		result += format("%stxDecision at offset %u, length %u with tableId"
				 " %lu, keyHash 0x%016" PRIX64 ", leaseId %lu",
				 separator, it.getOffset(), it.getLength(),
				 decisionRecord.getTableId(), decisionRecord.getKeyHash(),
				 decisionRecord.getLeaseId());

	    } else if (type == LOG_ENTRY_TYPE_TXPLIST) {
	        Buffer buffer;
		it.appendToBuffer(buffer);
		ParticipantList participantList(buffer);
		result += format("%sparticipantList at offset %u, length %u with "
				 "TxId: (leaseId %lu, rpcId %lu) containing %u entries",
				 separator, it.getOffset(), it.getLength(),
				 participantList.getTransactionId().clientLeaseId,
				 participantList.getTransactionId().clientTransactionId,
				 participantList.getParticipantCount());
	    }

	    it.next();
	    separator = " | ";
	}
	return result;
    }

    /**
     * Shared RAMCloud information.
     */
    Context* context;

    /**
     * The runtime configuration for the server this ObjectManager is in. Used
     * to pass parameters to various subsystems such as the log, hash table,
     * cleaner, and so on.
     */
    const ServerConfig* config;

    /**
     * The TabletManager keeps track of table hash ranges that belong to this
     * server. ObjectManager uses this information to avoid returning objects
     * that are still in the hash table, but whose tablets are not assigned to
     * the server. This occurs, for instance, during recovery before a tablet's
     * ownership is taken and after a tablet is dropped.
     */
    TabletManager* tabletManager;

    /**
     * Used to update table statistics.
     */
    MasterTableMetadata* masterTableMetadata;

    /**
     * Used to managed cleaning and recovery of RpcResult objects.
     */
    UnackedRpcResults* unackedRpcResults;

    /**
     * Used to manage cleaning and recovery of PreparedOp objects.
     */
    TransactionManager* transactionManager;

    /**
     * Used to managed cleaning and recovery of RpcResult objects.
     */
    TxRecoveryManager* txRecoveryManager;

    /**
     * Allocator used by the SegmentManager to obtain main memory for log
     * segments.
     */
    SegletAllocator allocator;

    /**
     * Creates and tracks replicas of in-memory log segments on remote backups.
     * Its BackupFailureMonitor must be started after the log is created
     * and halted before the log is destroyed.
     */
    ReplicaManager replicaManager;

    /**
     * The SegmentManager manages all segments in the log and interfaces
     * between the log and the cleaner modules.
     */
    SegmentManager segmentManager;

    /**
     * Protects access to tombstoneRemover and tombstoneProtectorCount.
     */
    SpinLock mutex;

    /**
     * Number of TombstoneProtector objects that currently exist for this
     * ObjectsManager.
     */
    int tombstoneProtectorCount;

    friend class CleanerCompactionBenchmark;
    friend class ObjectManagerBenchmark;

    DISALLOW_COPY_AND_ASSIGN(ObjectManager);
};

} // namespace RAMCloud

#endif // RAMCLOUD_OBJECTMANAGER_H
