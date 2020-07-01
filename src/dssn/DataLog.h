/* Copyright (c) 2020  Futurewei Technologies, Inc.
 * All rights are reserved.
 */
#pragma once

#include "Common.h"
#include "DLog.h"

namespace DSSN {
/**
 * This class provides data logging service.
 * Logged data can later be referenced by its <logid, offset>.
 */
class DataLog {
    private:
    // private struct
    typedef struct DataLogMarker {
        #define LOG_HEAD_SIG 0xA5A5F0F0
        #define LOG_TAIL_SIG 0xF0F0A5A5
        uint32_t sig;   // signature
        uint32_t length;// Tx log record size, include header and tailer
    } TxLogHeader_t, TxLogTailer_t;

    // Defines 
    #define DATALOG_DIR   "/dev/shm/datalog-%03d"
    #define DATALOG_CHUNK_SIZE (1024*1024*1024)

    // private variables
    DLog<DATALOG_CHUNK_SIZE> *log;
    uint32_t datalog_id;

    public:
    DataLog(uint32_t logid = 0) : datalog_id(logid)
    {
        char logdir[strlen(DATALOG_DIR)];
        assert(logid < 1000);
        sprintf(logdir, DATALOG_DIR, logid);
        std::string s(logdir);
        log = new DLog<DATALOG_CHUNK_SIZE>(s, true);
    }

    // Add data blob to the log.
    // // Return log offset
    uint64_t add(const void* dblob, size_t dlen)
    {
        uint64_t logoff;
        uint32_t totalsz = dlen + sizeof(TxLogHeader_t) + sizeof(TxLogTailer_t);
        TxLogHeader_t hdr = {LOG_HEAD_SIG, totalsz};
        TxLogTailer_t tal = {LOG_TAIL_SIG, totalsz};
        
        void *dst = log->reserve(totalsz, &logoff);
        outMemStream out((uint8_t*)dst, totalsz);
        out.write(&hdr, sizeof(hdr));
        out.write(dblob, dlen);
        out.write(&tal, sizeof(tal));
        return logoff + sizeof(TxLogHeader_t);
    }

    void* getdata(uint64_t offset, uint32_t *len /*Out*/)
    {
        TxLogHeader_t *hdr = (TxLogHeader_t*)log->getaddr(offset - sizeof(TxLogHeader_t), len);
        if (!hdr) {
            *len = 0;
            return NULL;
        }
        // assert(hdr->sig == LOG_HEAD_SIG);
        if (hdr->sig != LOG_HEAD_SIG)
            *(int*)0 = 0;

        assert(hdr->length <= *len);
        *len = hdr->length - sizeof(TxLogHeader_t) - sizeof(TxLogTailer_t);
        return &hdr[1];
    }

    // Return logid
    uint32_t logid() { return datalog_id; }

    // Return data size of DataLog
    size_t size() { return log->size(); }

    // Clear TxLog
    void clear() { log->cleanup(); }

    // Trim
    void trim(size_t off) { log->trim(off); }

}; // DataLog

} // end namespace DSSN
