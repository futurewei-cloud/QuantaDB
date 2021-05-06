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
#ifndef PEER_INFO_H
#define PEER_INFO_H

#include "Common.h"
#include "TxEntry.h"
#include "Validator.h"
#include "tbb/concurrent_unordered_map.h"
#include <boost/lockfree/queue.hpp>

namespace QDB {

/**
 * The class implements a search-able table of tx entries undergoing
 * SSN message exchanges with the tx peers.
 *
 * It is expected that serialize() adds a new entry.
 *
 * It is expected that RPC handlers, in response to receiving SSN info from
 * transaction peers, use this class to look up their
 * tx entries and update their DSSN meta data.
 *
 * It is expected that a thread would scan the tx entries
 * for fear that SSN messages might be lost and some exchanges would not complete
 * without further triggering message exchange.
 *
 * Considering all those concurrency concerns, we would use TBB concurrent_unordered_map
 * which supports concurrent insert, find, and iteration. However, a concurrent erase would disrupt
 * the iteration, so we would have to use that thread to erase finished entries while scanning.
 *
 * Compared to TBB concurrent_hash_map, our choice would perform better as the frequent
 * inserts and finds are lock-free while using finer per TxEntry locking to protect concurrent value
 * modification.
 *
 */

#define TBLSZ 8192

typedef __uint128_t CTS;
typedef tbb::concurrent_unordered_map<CTS, uint32_t>::iterator PeerInfoIterator;

struct PeerEntry {
    CTS cts = 0;
    bool isConcluded = true;
    uint64_t peerSeenSet; //peers seen so far
    uint64_t peerAlertSet; //peers seen currently in alert state
    uint32_t peerTxState = TxEntry::TX_PENDING; //summary state of all seen peers
    DSSNMeta meta; //summary of pstamp and sstamp of all seen peers
    TxEntry *txEntry = NULL; //reference to the associated commit intent
};

class  Validator;

struct PeerEvent {
    CTS cts;
    uint64_t peerId = 0;
    uint32_t eventType = 0;
    uint32_t peerTxState = 0;
    uint64_t peerPStamp = 0;
    uint64_t peerSStamp = 0;
    TxEntry* txEntry = NULL;
    PeerEntry *peerEntry = NULL;
    uint8_t peerPosition = 0;
};

class PeerInfo {
    PROTECTED:
    tbb::concurrent_unordered_map<CTS, uint32_t> peerInfo;
    PeerEntry peerEntryTable[TBLSZ];
    std::queue<uint32_t> recycleQueue;
    uint64_t lastTick = 0;
    uint64_t tickUnit = 10000000; //10ms per tick
    uint64_t alertThreshold = 10 * tickUnit;
    boost::lockfree::queue<PeerEvent *> eventQueue{1000};
    uint32_t tid;

    //evaluate the new states of the commit intent; caller is supposed to hold the mutex
    inline bool evaluate(PeerEntry *peerEntry, TxEntry *txEntry, Validator *validator);

    //log and send; caller is supposed to hold the mutex
    inline bool logAndSend(TxEntry *txEntry, Validator *validator);

    PUBLIC:
    PeerInfo(uint32_t tid);
    ~PeerInfo();

	//add a tx for tracking peer info
    ///txEntry may or may not be NULL
    bool add(CTS cts, TxEntry* txEntry, Validator* validator);

    //send tx SSN info to peers
    bool send(PeerEntry *peerEntry, Validator *validator);

    //monitor SSN peer status
    bool monitor(Validator *validator);

    //update peer info of a tx identified by cts
    bool update(CTS cts, uint64_t peerId, uint32_t peerTxState, uint64_t eta, uint64_t pi, uint8_t peerPosition,
            uint32_t &myTxState, uint64_t &myEta, uint64_t &myPi, uint8_t &myPeerPosition, Validator *validator);

    bool poseEvent(uint32_t eventType, CTS cts, uint64_t peerId, uint8_t peerPosition, uint32_t peerTxState, uint64_t eta, uint64_t pi, TxEntry *txEntry, PeerEntry *peerEntry);

    bool processEvent(Validator *validator);

    //current capacity
    uint32_t size();
}; // end PeerInfo class

} // end namespace QDB

#endif  /* PEER_INFO_H */

