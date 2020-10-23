/* Copyright (c) 2020  Futurewei Technologies, Inc.
 * All rights are reserved.
 */
#include "TxLog.h"

namespace DSSN {

bool
TxLog::add(TxEntry *txEntry)
{
    logMutex.lock();
    uint32_t logsize = txEntry->serializeSize();
    uint32_t totalsz = logsize + sizeof(TxLogHeader_t) + sizeof(TxLogTailer_t);
    TxLogHeader_t hdr = {TX_LOG_HEAD_SIG, totalsz};
    TxLogTailer_t tal = {TX_LOG_TAIL_SIG, totalsz};

    void *dst = log->reserve(totalsz);
    outMemStream out((uint8_t*)dst, totalsz);
    out.write(&hdr, sizeof(hdr));
    txEntry->serialize( out );
    out.write(&tal, sizeof(tal));
    logMutex.unlock();
    return true;
}

bool
TxLog::getFirstPendingTx(uint64_t &idOut, DSSNMeta &meta, std::set<uint64_t> &peerSet,
                        boost::scoped_array<KVLayout*> &writeSet)
{
    return getNextPendingTx(0, idOut, meta, peerSet, writeSet);
}

bool
TxLog::getNextPendingTx(uint64_t idIn, uint64_t &idOut, DSSNMeta &meta, std::set<uint64_t> &peerSet,
                        boost::scoped_array<KVLayout*> &writeSet)
{
    TxEntry tx(1,1);
    if (getNextPendingTx(idIn, idOut, &tx)) {
        meta.pStamp = tx.getPStamp();
        meta.sStamp = tx.getSStamp();
        meta.cStamp = tx.getCTS();
        peerSet =   tx.getPeerSet();
        writeSet.reset(new KVLayout*[tx.getWriteSetIndex()]);
        memcpy(writeSet.get(), tx.getWriteSet().get(), sizeof(KVLayout*) * tx.getWriteSetIndex()); 
        return true;
    }
    return false;
}

bool
TxLog::getNextPendingTx(uint64_t idIn, uint64_t &idOut, TxEntry *txOut)
{
    uint32_t dlen;
    uint64_t off = idIn;
    TxLogHeader_t * hdr;

    while ((hdr = (TxLogHeader_t*)log->getaddr (off, &dlen))) {
        assert(hdr->sig == TX_LOG_HEAD_SIG);
        off += hdr->length;

        inMemStream in((uint8_t*)&hdr[1], dlen - sizeof(hdr));
        txOut->deSerialize( in );
        if (txOut->getTxState() == TxEntry::TX_PENDING) {
            idOut = off;
            return true;
        }
    }
    return false;
}

uint32_t
TxLog::getTxState(__uint128_t cts)
{
    uint32_t dlen;
    TxLogTailer_t * tal;
    size_t hdrsz = sizeof(TxLogTailer_t) + sizeof(TxLogHeader_t);

    // Search backward to find the latest matching Tx
    size_t tail_off = size() - sizeof(TxLogTailer_t);;
    while ((tail_off > 0) && (tal = (TxLogTailer_t*)log->getaddr (tail_off, &dlen))) {
        tail_off -= tal->length; // next tail
        assert(tal->sig == TX_LOG_TAIL_SIG);

        inMemStream in((uint8_t*)tal - tal->length + hdrsz, dlen + tal->length - hdrsz);
        TxEntry tx(1,1);
        tx.deSerialize_common( in );
        if (tx.getCTS() == cts) { 
            return tx.getTxState(); 
        }
    }
    return TxEntry::TX_ALERT; // indicating not found here
}

bool
TxLog::getTxInfo(__uint128_t cts, uint32_t &txState, uint64_t &pStamp, uint64_t &sStamp)
{
    uint32_t dlen;
    TxLogTailer_t * tal;
    size_t hdrsz = sizeof(TxLogTailer_t) + sizeof(TxLogHeader_t);

    logMutex.lock();
    // Search backward to find the latest matching Tx
    size_t tail_off = size() - sizeof(TxLogTailer_t);;
    while ((tail_off > 0) && (tal = (TxLogTailer_t*)log->getaddr (tail_off, &dlen))) {
        tail_off -= tal->length; // next tail
        if (tal->sig != TX_LOG_TAIL_SIG) abort();

        inMemStream in((uint8_t*)tal - tal->length + hdrsz, dlen + tal->length - hdrsz);
        TxEntry tx(1,1);
        tx.deSerialize_common( in );
        if (tx.getCTS() == cts) {
            txState = tx.getTxState();
            pStamp = tx.getPStamp();
            sStamp = tx.getSStamp();
            logMutex.unlock();
            return true;
        }
    }
    logMutex.unlock();
    return false; // indicating not found here
}

bool
TxLog::fabricate(__uint128_t cts, uint8_t *key, uint32_t keyLength, uint8_t *value, uint32_t valueLength)
{
    TxEntry *txEntry = new TxEntry(0,1);
    txEntry->setCTS(cts);
    txEntry->setTxState(TxEntry::TX_FABRICATED);
    KVLayout *kvLayout = new KVLayout(keyLength);
    std::memcpy(kvLayout->getKey().key.get(), key, keyLength);
    kvLayout->v.valuePtr = value;
    kvLayout->v.valueLength = valueLength;
    txEntry->insertWriteSet(kvLayout, 0);
    add(txEntry);
    delete txEntry;
    return true;
}

static const char *txStateToStr(uint32_t txState)
{
    switch(txState) {
    case TxEntry::TX_ALERT: return (const char*)"TX_ALERT";
    case TxEntry::TX_PENDING: return (const char*)"TX_PENDING";
    case TxEntry::TX_COMMIT: return (char const *)"TX_COMMIT";
    case TxEntry::TX_CONFLICT: return (const char*)"TX_CONFLICT";
    case TxEntry::TX_ABORT: return (const char*)"TX_ABORT";
    case TxEntry::TX_FABRICATED: return (const char*)"TX_FABRICATED";
    case TxEntry::TX_OUTOFORDER: return (const char*)"TX_OUTOFORDER";
    }
    assert(0);
    return (const char*)"TX_UNKNOWN";
}

void
TxLog::dump(int fd)
{
    uint32_t dlen;
    TxLogTailer_t * tal;
    size_t hdrsz = sizeof(TxLogTailer_t) + sizeof(TxLogHeader_t);
    std::set<uint64_t> peerSet;

    dprintf(fd, "Dumping TxLog backward\n\n");

    // Search backward to find the latest matching Tx
    int64_t tail_off = size() - sizeof(TxLogTailer_t);;
    while ((tail_off > 0) && (tal = (TxLogTailer_t*)log->getaddr (tail_off, &dlen))) {
        tail_off -= tal->length; // next tail
        assert(tal->sig == TX_LOG_TAIL_SIG);

        inMemStream in((uint8_t*)tal - tal->length + hdrsz, dlen + tal->length - hdrsz);
        TxEntry tx(0,0);
        tx.deSerialize( in );

        dprintf(fd, "CTS: %lu:%lu, TxState: %s, pStamp: %lu, sStamp: %lu\n",
            (uint64_t)(tx.getCTS()>>64), (uint64_t)tx.getCTS(), txStateToStr(tx.getTxState()), tx.getPStamp(), tx.getSStamp());

        dprintf(fd, "\tpeerSet: ");
        peerSet =   tx.getPeerSet();
        for(std::set<uint64_t>::iterator it = peerSet.begin(); it != peerSet.end(); it++) {
            uint64_t peer = *it;
            dprintf(fd, "%lu, ", peer);
        }
        dprintf(fd, "\n");

        KVLayout **writeSet = tx.getWriteSet().get();
        dprintf(fd, "\twriteSet:\n");
        for (uint32_t widx = 0; widx < tx.getWriteSetSize(); widx++) {
            KVLayout *kv = writeSet[widx];
            assert(kv);
            dprintf(fd, "\t  key%02d: ", widx+1);
            for (uint32_t kidx = 0; kidx < kv->k.keyLength; kidx++) { 
                dprintf(fd, "%02X ", kv->k.key.get()[kidx]);
            }
            dprintf(fd, "\n");
            if (tx.getTxState() == TxEntry::TX_FABRICATED) {
                dprintf(fd, "%s\n", kv->v.valuePtr);
            }
        }

        dprintf(fd, "\n");
    }
}

};
