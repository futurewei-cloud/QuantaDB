/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef PEER_INFO_H
#define PEER_INFO_H

#include "Common.h"
#include "TxEntry.h"
#include "Validator.h"
#include "tbb/concurrent_unordered_map.h"

namespace DSSN {

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

struct PeerEntry {
    bool isValid = true;
    std::mutex mutexForPeerUpdate; //mutex for this peer entry
    std::set<uint64_t> peerSeenSet; //peers seen so far
    std::set<uint64_t> peerAlertSet; //peers seen currently in alert state
    uint32_t peerTxState = TxEntry::TX_PENDING; //summary state of all seen peers
    DSSNMeta meta; //summary of pstamp and sstamp of all seen peers
    TxEntry *txEntry = NULL; //reference to the associated commit intent
};

class  Validator;

typedef __uint128_t CTS;
typedef tbb::concurrent_unordered_map<CTS, PeerEntry *>::iterator PeerInfoIterator;

class PeerInfo {
    PROTECTED:
    tbb::concurrent_unordered_map<CTS, PeerEntry *> peerInfo;
    std::mutex mutexForPeerAdd;
    uint64_t lastTick = 0;
    uint64_t tickUnit = 1000000; //1ms per tick
    uint64_t alertThreshold = 1000 * tickUnit;

    //evaluate the new states of the commit intent; caller is supposed to hold the mutex
    inline bool evaluate(PeerEntry *peerEntry, TxEntry *txEntry, Validator *validator);

    PUBLIC:
    ~PeerInfo();

	//add a tx for tracking peer info
    ///txEntry may or may not be NULL
    bool add(CTS cts, TxEntry* txEntry, Validator* validator);

    //for iteration
    TxEntry* getFirst(PeerInfoIterator &it);
    TxEntry* getNext(PeerInfoIterator &it);

    //free txs which have been enqueued into conclusion queue
    bool sweep(Validator *validator);

    //send tx SSN info to peers
    bool send(Validator *validator);

    //update peer info of a tx identified by cts and return true if peer entry is found
    bool update(CTS cts, uint64_t peerId, uint8_t peerTxState, uint64_t eta, uint64_t pi,
            TxEntry *&txEntry, Validator *validator);

    //current capacity
    uint32_t size();

}; // end PeerInfo class

} // end namespace DSSN

#endif  /* PEER_INFO_H */

