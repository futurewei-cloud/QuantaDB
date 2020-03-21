/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef PEER_INFO_H
#define PEER_INFO_H

#include "Common.h"
#include "TxEntry.h"
#include "tbb/concurrent_unordered_map.h"

namespace DSSN {

typedef uint64_t CTS;

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
 * which supports concurrent insert and iteration. However, a concurrent erase would disrupt
 * the iteration, so we would have to use that thread to erase completed entries while scanning.
 *
 */
class PeerInfo {
    PROTECTED:
    tbb::concurrent_unordered_map<CTS, TxEntry *> peerInfo;

    PUBLIC:
    bool add(TxEntry* txEntry);

    bool sweep();

    bool update(CTS cts, uint64_t peerId, uint64_t eta, uint64_t pi, TxEntry *&txEntry);

    uint32_t size();

}; // end PeerInfo class

} // end namespace DSSN

#endif  /* PEER_INFO_H */

