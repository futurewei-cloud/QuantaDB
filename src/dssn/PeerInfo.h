/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef PEER_INFO_H
#define PEER_INFO_H

#include "Common.h"
#include "TxEntry.h"
#include "tbb/concurrent_hash_map.h"

namespace DSSN {

typedef uint64_t CTS;

/**
 * The class implements a search-able table of tx entries undergoing
 * SSN message exchanges with the tx peers.
 *
 * It is expected that RPC handlers, in response to receiving SSN info from
 * transaction peers, use this class to look up their
 * tx entries and update their DSSN meta data.
 *
 * The validator inserts a new entry
 * and removes the entry which has received from all relevant peers.
 */
class PeerInfo {
    PROTECTED:
    tbb::concurrent_hash_map<CTS, TxEntry *, tbb::tbb_hash_compare<CTS>> peerInfo;

    PUBLIC:
    bool add(TxEntry* txEntry);

    bool remove(TxEntry* txEntry);

    bool update(CTS cts, uint64_t peerId, uint64_t eta, uint64_t pi);

}; // end PeerInfo class

} // end namespace DSSN

#endif  /* PEER_INFO_H */

