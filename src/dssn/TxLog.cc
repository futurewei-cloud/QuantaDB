/* Copyright (c) 2020  Futurewei Technologies, Inc.
 * All rights are reserved.
 */
#include "TxLog.h"

namespace DSSN {

TxLog::TxLog()
{
    log = new DLog<TXLOG_CHUNK_SIZE>(TXLOG_DIR, false);
}

TxLog::TxLog(bool recovery_mode = false)
{
    log = new DLog<TXLOG_CHUNK_SIZE>(TXLOG_DIR, recovery_mode);
}

bool
TxLog::add(TxEntry *txEntry)
{
    uint32_t logsize = txEntry->serializeSize();
    uint32_t totalsz = logsize + sizeof(TxLogHeader_t) + sizeof(TxLogTailer_t);
    TxLogHeader_t hdr = {TX_LOG_HEAD_SIG, totalsz};
    TxLogTailer_t tal = {TX_LOG_TAIL_SIG, totalsz};

    void *dst = log->reserve(totalsz, NULL);
    outMemStream out((uint8_t*)dst, totalsz);
    out.write(&hdr, sizeof(hdr));
    txEntry->serialize( out );
    out.write(&tal, sizeof(tal));
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
    uint32_t dlen;
    uint64_t off = idIn;
    TxLogHeader_t * hdr;

    while ((hdr = (TxLogHeader_t*)log->getaddr (off, &dlen))) {
        assert(hdr->sig == TX_LOG_HEAD_SIG);
        off += hdr->length;

        inMemStream in((uint8_t*)&hdr[1], dlen - sizeof(hdr));
        TxEntry tx(1,1);
        tx.deSerialize( in );
        if (tx.getTxState() == TxEntry::TX_PENDING) {
            idOut = off; 
            meta.pStamp = tx.getPStamp();
            meta.sStamp = tx.getSStamp();
            peerSet =   tx.getPeerSet();
            writeSet.reset(new KVLayout*[tx.getWriteSetIndex()]);
            memcpy(writeSet.get(), tx.getWriteSet().get(), sizeof(KVLayout*) * tx.getWriteSetIndex()); 
            return true;
        }
    }
    return false;
}

uint32_t
TxLog::getTxState(uint64_t cts)
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

};