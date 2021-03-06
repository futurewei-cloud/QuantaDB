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

#include <iostream>
#include "TxEntry.h"
#include "ConcurrentBitmap.h"

namespace QDB {

TxEntry::TxEntry(uint32_t _readSetSize, uint32_t _writeSetSize) {
    if (_readSetSize > TUPLE_ENTRY_MAX || _writeSetSize > TUPLE_ENTRY_MAX) {
        printf("read or write set of the transaction exceeded the maximum supported size: %d", TUPLE_ENTRY_MAX);
        exit(1);
    }
    sstamp = std::numeric_limits<uint64_t>::max();
    pstamp = 0;
    txState = TX_PENDING;
    commitIntentState = TX_CI_UNQUEUED;
    cts = 0;
    writeSetSize = _writeSetSize;
    readSetSize = _readSetSize;
    readSetIndex = writeSetIndex = 0;
    if (writeSetSize > 0) {
        writeSet.reset(new KVLayout *[writeSetSize]);
        writeSetHash.reset(new uint64_t[writeSetSize]);
        writeSetInStore.reset(new KVLayout *[writeSetSize]);
        writeSetKVSPtr.reset(new void *[writeSetSize]);
        for (uint32_t i = 0; i < writeSetSize; i++) {
            writeSet[i] = NULL;
            writeSetInStore[i] = NULL;
            writeSetKVSPtr[i] = NULL;
            writeTupleSkipLock[i] = false;
        }
    }
    if (readSetSize > 0) {
        readSet.reset(new KVLayout *[readSetSize]);
        readSetHash.reset(new uint64_t[readSetSize]);
        readSetInStore.reset(new KVLayout *[readSetSize]);
        readSetKVSPtr.reset(new void *[readSetSize]);
        for (uint32_t i = 0; i < readSetSize; i++) {
            readSet[i] = NULL;
            readSetInStore[i] = NULL;
            readSetKVSPtr[i] = NULL;
            readTupleSkipLock[i] = false;
        }
    }

    rpcHandle = NULL;
}


TxEntry::~TxEntry() {
    //Free KVLayout allocated in txCommit RPC handler
    for (uint32_t i = 0; i < writeSetSize; i++) {
        if (writeSet[i]) {
            if (writeSet[i]->meta().cStamp > 0)
                continue; //a RMW - let it be free from read set
            delete writeSet[i];
            writeSet[i] = NULL;
        }
    }
    for (uint32_t i = 0; i < readSetSize; i++) {
        if (readSet[i]) {
            delete readSet[i];
            readSet[i] = NULL;
        }
    }
}

bool
TxEntry::insertReadSet(KVLayout* kv, uint32_t i) {
    assert(i < readSetSize);
    readSet[i] = kv;
    uint64_t hash = kv->k.getKeyHash();
    readSetHash[i] = hash;

    //Apply local lock filter
    uint64_t loc = ConcurrentBitmap::getBitLocation(hash);
    auto lookup = lockTableFilter.find(loc);
    if (lookup != lockTableFilter.end()) {
        readTupleSkipLock[i] = true;
    } else {
        lockTableFilter.insert(loc);
    }
    return true;
}

bool
TxEntry::insertWriteSet(KVLayout* kv, uint32_t i) {
    assert(i < writeSetSize);
    writeSet[i] = kv;
    uint64_t hash = kv->k.getKeyHash();
    writeSetHash[i] = hash;

    //Apply local lock filter
    uint64_t loc = ConcurrentBitmap::getBitLocation(hash);
    auto lookup = lockTableFilter.find(loc);
    if (lookup != lockTableFilter.end()) {
      writeTupleSkipLock[i] = true;
    } else {
        lockTableFilter.insert(loc);
    }

    return true;
}

bool
TxEntry::correctReadSet(uint32_t size) {
    //This is a workaround function to correct the size of a possibly over-provisioned readSet.
    //Because scoped_array vars are used, we do not need to worry about memory leak.
    //If there is over-provisioning, the null elements will be at the tail of the arrays.
    //By changing the readSetSize, the handling of the readSet will be the same as if there
    //were no over-provisioning at all.
    //The proper solution is to have the commit intent pass the correct readSet size
    //so that txEntry will have the correct readSet size at instantiation.
    readSetSize = size;
    return true;
}

uint32_t 
TxEntry::serializeSize(uint32_t *wsSzRet, uint32_t *rsSzRet, uint32_t *psSzRet)
{
    uint32_t sz = sizeof(cts) + sizeof(txState) + sizeof(pstamp) + sizeof(sstamp);
    uint32_t wsSz = 0, rsSz = 0, psSz = 0;

    if (txState == TX_PENDING || txState == TX_FABRICATED) {
        sz += sizeof(commitIntentState);

        // writeSet
        wsSz += sizeof(uint32_t);
        for (uint32_t i = 0; i < getWriteSetSize(); i++) {
            if (writeSet[i])
                wsSz += writeSet[i]->serializeSize();
        }
        sz += wsSz;

        // readSet
        rsSz += sizeof(uint32_t);
        for (uint32_t i = 0; i < getReadSetSize(); i++) {
            if (readSet[i])
                rsSz += readSet[i]->serializeSize();
        }
        sz += rsSz;

        // peerSet
        psSz += sizeof(uint32_t);
        psSz += peerSet.size() * sizeof(uint64_t);
        psSz += sizeof(uint8_t);
        sz += psSz;
    }
    if (wsSzRet) *wsSzRet = wsSz;
    if (psSzRet) *psSzRet = psSz;
    if (rsSzRet) *rsSzRet = rsSz;
    return sz;
}

void 
TxEntry::serialize( outMemStream& out )
{
    out.write(&cts, sizeof(cts));
    uint32_t tmp = txState;
    out.write(&tmp, sizeof(txState));
    out.write(&pstamp,  sizeof(pstamp));
    out.write(&sstamp,  sizeof(sstamp));
    if (txState == TX_PENDING || txState == TX_FABRICATED) {
        tmp = commitIntentState;
        out.write(&tmp, sizeof((uint32_t)commitIntentState));

        // count writeSet #entry
        uint32_t nWriteSet = 0;
        for (uint32_t i = 0; i < getWriteSetSize(); i++) {
            if (writeSet[i])
                nWriteSet++;
        }

        // assert(nWriteSet == getWriteSetSize()); // According to Henry, the writeSet[] should be full of valid entries

        // writeSet
        out.write(&nWriteSet, sizeof(nWriteSet));
        for (uint32_t i = 0; i < nWriteSet; i++) {
            assert (writeSet[i]);
            writeSet[i]->serialize(out);
        }

        // count readSet #entry
        uint32_t nReadSet = 0;
        for (uint32_t i = 0; i < getReadSetSize(); i++) {
            if (readSet[i])
                nReadSet++;
        }

        // readSet
        out.write(&nReadSet, sizeof(nReadSet));
        for (uint32_t i = 0; i < nReadSet; i++) {
            assert (readSet[i]);
            readSet[i]->serialize(out);
        }

        // peerSet
        uint32_t peerSetSize = peerSet.size();
        out.write(&peerSetSize, sizeof(peerSetSize));
        for(std::set<uint64_t>::iterator it = peerSet.begin(); it != peerSet.end(); it++) {
            uint64_t peer = *it;
            out.write(&peer, sizeof(peer));
        }
        out.write(&myPeerPosition, sizeof(myPeerPosition));
    }
}

void
TxEntry::deSerialize_common( inMemStream& in )
{
    in.read(&cts, sizeof(cts));
    uint32_t tmp;
    in.read(&tmp, sizeof(txState));
    txState = tmp;
    in.read(&pstamp,  sizeof(pstamp));
    in.read(&sstamp,  sizeof(sstamp));
}

void
TxEntry::deSerialize_additional( inMemStream& in )
{
    uint32_t nWriteSet, nReadSet;

    uint32_t tmp;
    in.read(&tmp, sizeof(commitIntentState));
    commitIntentState = tmp;

    // writeSet
    in.read(&nWriteSet, sizeof(nWriteSet));
    writeSetSize = nWriteSet;
    writeSet.reset(new KVLayout *[nWriteSet]);
    for (uint32_t i = 0; i < nWriteSet; i++) {
        KVLayout* kv = new KVLayout(0);
        kv->deSerialize(in);
        writeSet[i] = kv;
    }

    // readSet
    in.read(&nReadSet, sizeof(nReadSet));
    readSetSize = nReadSet;
    readSet.reset(new KVLayout *[nReadSet]);
    for (uint32_t i = 0; i < nReadSet; i++) {
        KVLayout* kv = new KVLayout(0);
        kv->deSerialize(in);
        readSet[i] = kv;
    }

    // peerSet
    uint32_t peerSetSize;
    in.read(&peerSetSize, sizeof(peerSetSize));
    peerSet.clear();
    for(uint32_t idx = 0; idx < peerSetSize; idx++) {
        uint64_t peer;
        in.read(&peer, sizeof(peer));
        insertPeerSet(peer);
    }
    in.read(&myPeerPosition, sizeof(myPeerPosition));
}

void
TxEntry::deSerialize( inMemStream& in )
{
    deSerialize_common( in );
    if (txState == TX_PENDING || txState == TX_FABRICATED) {
        deSerialize_additional( in );
    }
}

} // end TxEntry class
