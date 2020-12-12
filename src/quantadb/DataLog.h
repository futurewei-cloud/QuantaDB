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

#pragma once

#include <iostream>
#include "MemStreamIo.h"
#include "Common.h"
#include "DLog.h"

namespace QDB {
/**
 * This class provides data logging services.
 * Logged data can later be referenced by its address, i.e., <logid, offset>.
 * Multiple instances of DataLog supported. Each instance is identified by a unique logid.
 *
 * Class constructor function takes an optional 'logid' argument. By default, logid is 1.
 * Note that, logid 0 is reserved for unit test only.
 *
 * API Description
 *
 * uint64_t add (const void *blob, size_t len)
 *      Add data 'blob' of size 'len' to the log.
 *      Return offset to the log device where the data begins.
 *
 * uint64_t add (std::string& str)
 *      String variation of the blob add.
 *
 * void * getdata(uint64_t offset, uint32_t *len = NULL)
 *      Return the memory address of logged data. The 'offset' is what was returned by the add()
 *      when data was logged via the add() API.
 *      The 'len' argument, if not NULL, is where data length is returned.
 *
 * void clear()
 *      Cleanup (remove) all data log files.
 *
 * void trim(uint64_t off)
 *      Trim the log from the begining to 'off'.
 *
 * void dump(int fd)
 *      Debugging dump to file descriptor 'fd'
 */
class DataLog {
  private:
    // Defines 
    #define DATALOG_DIR   "/dev/shm/datalog-%03d"
    #define DATALOG_CHUNK_SIZE (1024*1024*1024)

    #define LOG_HEAD_SIG 0xA5A5F0F0
    #define LOG_TAIL_SIG 0xF0F0A5A5

  public:
    DataLog(uint32_t logid = 1) : datalog_id(logid)
    {
        char logdir[strlen(DATALOG_DIR)];
        #if (TESTING == false)
        assert(logid != 0); // logid 0 reserved for testing only
        #endif
        sprintf(logdir, DATALOG_DIR, logid);
        std::string s(logdir);
        log = new DLog<DATALOG_CHUNK_SIZE>(s, true);

        bgn_off = (size() > 0)?  ((LogHeader_t*)log->getaddr(0))->doff - sizeof(LogHeader_t) : 0;
    }

    // Add data blob to the log.
    // Return log offset of the data
    uint64_t add(const void* dblob, size_t dlen)
    {
        uint32_t totalsz = dlen + sizeof(LogHeader_t) + sizeof(LogTailer_t);
        LogHeader_t hdr = {LOG_HEAD_SIG, 0, totalsz};
        LogTailer_t tal = {LOG_TAIL_SIG, totalsz};
        
        void *dst = log->reserve(totalsz, &hdr.doff);
        hdr.doff += bgn_off + sizeof(LogHeader_t);

        outMemStream out((uint8_t*)dst, totalsz);
        out.write(&hdr, sizeof(hdr));
        out.write(dblob, dlen);
        out.write(&tal, sizeof(tal));
        return hdr.doff;
    }

    uint64_t add(std::string& str)
    {
        return add(str.data(), str.length());
    }

    // Given an offset (which was return by add()), return the memory address of the data.
    void* getdata(uint64_t offset, uint32_t *len /*Out*/)
    {
        LogHeader_t *hdr;
        if (!valid_offset(offset, &hdr, len)) {
            if (len) *len = 0;
            return NULL;
        }
        assert(hdr->length <= *len);
        *len = hdr->length - sizeof(LogHeader_t) - sizeof(LogTailer_t);
        return &hdr[1];
    }

    // Return logid
    inline uint32_t logid() { return datalog_id; }

    // Return data size of DataLog
    inline size_t size() { return log->size(); }

    // Cleanup DataLog. Remove all chunk files.
    inline void clear() { log->cleanup(); }

    // Trim data up until, but exclude, the data associated to doff. 
    // off' must be a valid data offset (i.e., an offset returned by add())
    inline bool trim(size_t doff)
    {
        if (!valid_offset(doff)) {
            return false;
        }
        uint64_t trim_off = doff - bgn_off - sizeof(LogHeader_t);
        if (trim_off > 0)
            log->trim(trim_off);
        bgn_off = doff - sizeof(LogHeader_t);
        return true;
    }

    // For debugging. Dump log content to file descriptor 'fd'
    void dump(int fd)
    {
        uint32_t dlen, ditem = 0;
        LogTailer_t * tal;
        size_t hdrsz = sizeof(LogTailer_t) + sizeof(LogHeader_t);

        dprintf(fd, "Dumping DataLog backward. Log id: %d\n\n", datalog_id);

        // Search backward to find the latest matching 
        int64_t tail_off = size() - sizeof(LogTailer_t);;
        while ((tail_off > 0) && (tal = (LogTailer_t*)log->getaddr ((uint64_t)tail_off))) {
            assert(tal->sig == LOG_TAIL_SIG);

            uint32_t data_len = tal->length - hdrsz;
            uint64_t data_off = tail_off - data_len;
            tail_off -= tal->length; // next tail

            char *data = (char *)getdata(data_off, &dlen);
            assert(dlen == data_len);

            dprintf(fd, "%d. length: %d\n", ditem++, data_len);
            dprintf(fd, "\t  Hex: ");
            for (uint32_t ii = 0; ii < data_len && ii < 24; ii++) { dprintf(fd, "%02X ", data[ii]); }
            dprintf(fd, "\n");

            dprintf(fd, "\tAscii: ");
            for (uint32_t ii = 0; ii < data_len && ii < 24; ii++) { dprintf(fd, "%c  ", data[ii]); }
            dprintf(fd, "\n\n");
        }
    }

    inline uint64_t bgn_offset() { return bgn_off; }

  private:
    // private struct
    typedef struct {
        uint32_t sig;   // signature
        uint64_t doff;  // data id (offset before trim)
        uint32_t length;// log record size, include header and tailer
    } LogHeader_t;

    typedef struct {
        uint32_t sig;   // signature
        uint32_t length;// log record size, include header and tailer
    } LogTailer_t;

    // private variables
    DLog<DATALOG_CHUNK_SIZE> *log;
    uint32_t datalog_id;
    uint64_t bgn_off;

    // Returns NULL if offset is invalid.
    inline bool valid_offset(uint64_t offset, LogHeader_t **hdrpp = NULL/*out*/, uint32_t *len = NULL/*out*/)
    {
        if (offset < bgn_off)
            return NULL;

        uint64_t phyoff = offset - bgn_off;
        LogHeader_t *hdr = (LogHeader_t*)log->getaddr(phyoff - sizeof(LogHeader_t), len);

        if (hdr && (hdr->sig == LOG_HEAD_SIG)) {
            assert (hdr->doff == offset);
            if (hdrpp) *hdrpp = hdr;
            return true;
        }
        return false;
    }

}; // DataLog

} // end namespace QDB
