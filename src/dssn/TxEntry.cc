/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */
#include <iostream>
#include "TxEntry.h"
#include "MurmurHash3.h"

namespace DSSN {

TxEntry::TxEntry(uint32_t _readSetSize, uint32_t _writeSetSize) {
	sstamp = std::numeric_limits<uint64_t>::max();
	pstamp = 0;
	txState = TX_PENDING;
	commitIntentState = TX_CI_UNQUEUED;
	cts = 0;
	writeSetSize = _writeSetSize;
	readSetSize = _readSetSize;
	readSetIndex = this->writeSetIndex = 0;
	writeSet.reset(new KVLayout *[writeSetSize]);
	writeSetHash.reset(new uint64_t[writeSetSize]);
	writeSetInStore.reset(new KVLayout *[writeSetSize]);
	readSet.reset(new KVLayout *[readSetSize]);
	readSetHash.reset(new uint64_t[readSetSize]);
	readSetInStore.reset(new KVLayout *[readSetSize]);
	for (uint32_t i = 0; i < writeSetSize; i++)
		writeSet[i] = writeSetInStore[i] = NULL;
	for (uint32_t i = 0; i < readSetSize; i++)
		readSet[i] = readSetInStore[i] = NULL;
	rpcHandle = NULL;
}


TxEntry::~TxEntry() {
    //Free KVLayout allocated in txCommit RPC handler
    for (uint32_t i = 0; i < writeSetSize; i++) {
        if (writeSet[i]) {
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
	readSet[i] = kv;
	uint64_t indexes[2];
	RAMCloud::MurmurHash3_x64_128(kv->k.key.get(), kv->k.keyLength, 0, indexes);
	readSetHash[i] = ((indexes[0] << 32) | (indexes[1] & 0xffffffff));
	return true;
}

bool
TxEntry::insertWriteSet(KVLayout* kv, uint32_t i) {
	writeSet[i] = kv;
	uint64_t indexes[2];
	RAMCloud::MurmurHash3_x64_128(kv->k.key.get(), kv->k.keyLength, 0, indexes);
	writeSetHash[i] = ((indexes[0] << 32) | (indexes[1] & 0xffffffff));
	return true;
}

uint32_t 
TxEntry::serializeSize()
{
    uint32_t sz = sizeof(cts) + sizeof(txState) + sizeof(pstamp) + sizeof(sstamp);
    if (txState == TX_PENDING) {
        sz += sizeof(commitIntentState);

        // writeSet
        sz += sizeof(uint32_t);
        for (uint32_t i = 0; i < getWriteSetSize(); i++) {
            if (writeSet[i])
                sz += writeSet[i]->serializeSize();
        }

        // peerSet
        sz += sizeof(uint32_t);
        sz += peerSet.size() * sizeof(uint64_t);
    }
    return sz;
}

void 
TxEntry::serialize( outMemStream& out )
{
    out.write(&cts, sizeof(cts));
    out.write(&txState, sizeof(txState));
    out.write(&pstamp,  sizeof(pstamp));
    out.write(&sstamp,  sizeof(sstamp));
    if(txState == TX_PENDING) {
        out.write(&commitIntentState, sizeof(commitIntentState));

        // count writeSet #entry
        uint32_t nWriteSet = 0;
        for (uint32_t i = 0; i < getWriteSetSize(); i++) {
            if (writeSet[i])
                nWriteSet++;
        }

        // writeSet
        out.write(&nWriteSet, sizeof(nWriteSet));
        for (uint32_t i = 0; i < nWriteSet; i++) {
            assert (writeSet[i]);
            writeSet[i]->serialize(out);
        }

        // peerSet
        uint32_t peerSetSize = peerSet.size();
        out.write(&peerSetSize, sizeof(peerSetSize));
        for(std::set<uint64_t>::iterator it = peerSet.begin(); it != peerSet.end(); it++) {
            uint64_t peer = *it;
            out.write(&peer, sizeof(peer));
        }
    }
}

void
TxEntry::deSerialize_common( inMemStream& in )
{
    in.read(&cts, sizeof(cts));
    in.read(&txState, sizeof(txState));
    in.read(&pstamp,  sizeof(pstamp));
    in.read(&sstamp,  sizeof(sstamp));
}

void
TxEntry::deSerialize_additional( inMemStream& in )
{
    uint32_t nWriteSet;

    in.read(&commitIntentState, sizeof(commitIntentState));

    // writeSet
    in.read(&nWriteSet, sizeof(nWriteSet));
    writeSetSize = nWriteSet;
    writeSetIndex = nWriteSet;
	writeSet.reset(new KVLayout *[nWriteSet]);
    for (uint32_t i = 0; i < nWriteSet; i++) {
    	KVLayout* kv = new KVLayout(0);
        kv->deSerialize(in);
        writeSet[i] = kv;
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
}

void
TxEntry::deSerialize( inMemStream& in )
{
    deSerialize_common( in );
    if (txState == TX_PENDING) {
        deSerialize_additional( in );
    }
}

} // end TxEntry class
