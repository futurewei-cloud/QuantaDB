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
    std::mutex mutexForPeerUpdate;
    std::set<uint64_t> peerSeenSet;
    std::set<uint64_t> peerAlertSet;
    DSSNMeta meta;
    TxEntry *txEntry = NULL;
};

class  Validator;

typedef uint64_t CTS;
typedef tbb::concurrent_unordered_map<CTS, PeerEntry *>::iterator PeerInfoIterator;

class PeerInfo {
    PROTECTED:
    tbb::concurrent_unordered_map<CTS, PeerEntry *> peerInfo;
    std::mutex mutexForPeerAdd;
    uint64_t lastTick = 0;
    uint64_t tickUnit = 10000000; //10ms per tick
    uint64_t alertThreshold = tickUnit; //arbitrary

    //evaluate the new states of the commit intent; caller is supposed to hold the mutex
    bool evaluate(PeerEntry *peerEntry, uint8_t peerTxState, TxEntry *txEntry, Validator *validator);

    PUBLIC:
	//add a tx for tracking peer info
    ///txEntry may or may not be NULL
    bool add(CTS cts, TxEntry* txEntry, Validator* validator);

    //for iteration
    TxEntry* getFirst(PeerInfoIterator &it);
    TxEntry* getNext(PeerInfoIterator &it);

    //free txs which have been enqueued into conclusion queue
    bool sweep();

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

