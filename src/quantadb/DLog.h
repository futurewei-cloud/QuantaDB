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

#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <string.h>
#include <libgen.h>
#include <mutex>
#include <iostream>

namespace QDB {
/*
 * DLog (DSSN Log) Design Highlights:
 * - DLog is a local log device. No remote access.
 * - Logicalaly, DLog is a log device with append write and ramdom read.
 * - Physically, DLog is consisted of a set of log files (aka chunk file).
 * - Log files are memory mapped for high speed access.
 * - Supports concurrent append operation.
 * - Log append space can be reserved. Class object serialzation output can
 *   go directly into log buffer to save an intermediate stage.
 * Non-goals
 * - DLog is not a Plog simulator.
 * API
 * - uint64_t append (void *data, uint32_t len) 
 *
 *   Append data of size 'len' to log. Return the beginning offset (to the log)
 *   of the appended data. The offset is good until the next trim operation.
 *   Multi-thread concurrent append() is supported.
 *
 * - void * reserve (uint32_t len)
 *
 *   Reserve log append space of 'len' size. Return the memory address of reserved space.   
 *   Multi-thread concurrent reserve() is supported.
 *
 * - uint64_t trim (uint64_t len)
 *
 *   Trim the log from the beginning for 'len' bytes.
 *   Trim will also delete the associated backing store files.
 *   Return the size that was actually trimmed.
 *   Note that trim() will not trim active (i.e., unsealed) chunk files.
 *
 * - uint32_t read (uint64_t off, void *obuf, uint32_t len)
 *
 *   Read 'len' bytes from 'off' offset into 'obuf'. Return bytes read.
 *
 * - void * getaddr(uint64_t off, uint32_t *len)
 *
 *   Return log buffer address at offset 'off'. The output argument 'len' stores buffer length
 */
template <uint64_t CHUNK_SIZE = (16*1024*1024) >
class DLog {
  private:
    /*
     * DLog Internal data structure
     */
    // ondisk chunk header
    typedef struct ondisk_chunk_header {
        #define DLOG_SIGNATURE    0xF0F05A5A
        uint32_t    Sig;
        uint8_t     version;// header version
        uint8_t     sealed; // bool
        uint32_t    fsize;  // chunk file size
        uint32_t    bgn_off;// beginning offset of data
        uint32_t    dsize;  // data size
        uint32_t    seqno;
    } chunk_hdr_t;

    // incore chunk
    typedef struct chunk {
        struct chunk *  next;
        std::string path;
        void *          maddr;
        chunk_hdr_t *   hdr;
        ~chunk() {
            munmap(maddr, hdr->fsize);
        }
        void remove() {
            int ret = unlink(path.c_str());
            assert(ret == 0);
            delete this; 
        }
    } chunk_t;

  public:
    DLog(std::string logdir = "/tmp", bool recovery_mode = false) : topdir(logdir)
    {
        next_seqno = 0;
        chunk_head = chunk_tail = NULL;
        chunk_size = CHUNK_SIZE;

        struct stat st;
        if (stat(topdir.c_str(), &st) != 0) {
            int ret = mkpath(topdir.c_str(), 0777);
            assert(ret == 0);
        }

        // Load existing logs
        if (recovery_mode)
            load_chunks(topdir.c_str());
        else
            clean_chunks(topdir.c_str());

        // If no log yet, create one.
        if (chunk_head == NULL) {
            if (! add_new_chunk_file()) {
                std::cout << "Failed to create log file" << std::endl;
                exit (1);
            }
        }
        assert(chunk_head);
    }

    ~DLog()
    {
        cleanup();
    }

    // Return log data size
    inline uint64_t size(void)
    {
        uint64_t lsize = 0;
        for (chunk_t *tmp = chunk_head; tmp; tmp = tmp->next) {
            lsize += tmp->hdr->dsize;
        }
        return lsize;
    }

    // Return free space
    inline uint64_t free_space(void)
    {
        uint64_t free_size = 0;
        for (chunk_t *tmp = chunk_tail; tmp; tmp = tmp->next) {
            free_size += tmp->hdr->fsize - tmp->hdr->dsize - tmp->hdr->bgn_off;
        }
        return free_size;
    }

    // Reserve 'len' bytes append space in log.
    // Return starting address of the reserved (continuous) space.
    void * reserve(uint32_t len, /* log offset out */ uint64_t * offset = NULL )
    {
        uint32_t oldsize;
        chunk_t * chunk;
        do {
            chunk = chunk_tail;
            if (chunk->hdr->sealed) {
                if (chunk->next) { // move chunk_tail to the next
                    __sync_bool_compare_and_swap(&chunk_tail, chunk, chunk->next);
                } else {
                    add_new_chunk_file();
                }
                continue;
            }

            // If chunk free space too small, seal the chunk
            uint64_t free_space = chunk->hdr->fsize - chunk->hdr->bgn_off - chunk->hdr->dsize;
            if (free_space < len) {
                chunk->hdr->sealed = true; // insufficient space, sealed it.
                continue;
            }

            oldsize = chunk->hdr->dsize;;
            if (__sync_bool_compare_and_swap(&chunk->hdr->dsize, oldsize, oldsize+len)) {
                    break; // done
            }
        } while (true);

        if (offset)
            *offset = chunk_offset(chunk) + oldsize;

        return (char *)chunk->maddr + chunk->hdr->bgn_off + oldsize; 
    }

    // Append to log. Return log offset of the appended data.
    // Note: append() must be thread safe
    uint64_t append(const void *data, uint32_t len)
    {
        uint64_t off;
        void *dst = reserve(len, &off);
        memcpy(dst, data, len);
        return off;
    }

    // Trim Log content from the beginning for length bytes. 
    // If length == 0, trim all.
    // Return trim'ed size.
    uint64_t trim (uint64_t length = 0)
    {
        mtx.lock();
        if (length == 0)
            length = size();
        uint64_t remain = length;
        chunk_t * tmp, *old_tmp, * old_head;
        tmp = old_head = chunk_head;
        while (tmp) {
            if (tmp->hdr->dsize >= remain) {
                tmp->hdr->dsize -= remain;
                tmp->hdr->bgn_off = (tmp->hdr->dsize == 0)? sizeof(chunk_hdr_t) : tmp->hdr->bgn_off + remain;
                remain = 0;
                break;
            }
            assert(tmp->hdr->sealed);
            remain -= tmp->hdr->dsize;
            tmp = tmp->next;
        }
        chunk_head = tmp;

        // Remove trim'ed chunks
        tmp = old_head;
        while (tmp && (tmp != chunk_head)) {
            old_tmp = tmp;
            tmp = tmp->next;
            old_tmp->remove();
        }
        mtx.unlock();
        return length - remain;
    }

    // Delete all chunks
    void inline cleanup(void)
    {
        chunk_t * tmp;
        while ((tmp = chunk_head) != NULL) {
            chunk_head = chunk_head->next;
            tmp->remove();
        }
        chunk_tail = NULL;
    }

    // Return log buffer address at offset 'off'.
    // The output argument 'len' stores continuous buffer length
    void * getaddr (uint64_t off, uint32_t *len = NULL)
    {
        chunk_t * tmp = chunk_head;
        uint64_t remain = off;
        while (tmp && (remain > 0)) {
            if ((tmp->hdr->dsize) > remain) {
                break;
            }
            remain -= tmp->hdr->dsize;
            tmp = tmp->next;
        }

        if (!tmp || (size() == 0)) {
            if (len)
                *len = 0;
            return NULL; // log is empty or off is beyond the log size
        }

        if (len)
            *len = tmp->hdr->dsize - remain;
        return (char *)tmp->maddr + tmp->hdr->bgn_off + remain;
    }

    uint32_t read (uint64_t off, void *obuf, uint32_t len)
    {
        uint32_t todo, remain;
        uint32_t logsize = size();
        todo = remain = (logsize > len)? len : logsize;
        while (remain) {
            uint32_t dlen;
            void * dptr = getaddr(off, &dlen);
            uint32_t ncopy = (remain > dlen)? dlen : remain;
            memcpy(obuf, dptr, ncopy);
            remain -= ncopy;
            off += ncopy;
        }
        return todo;
    }

    void set_chunk_size(uint64_t size)
    {
        chunk_size = size;
    }

    bool add_new_chunk_file()
    {
        char chunk_name[128];
        uint32_t seqno = next_seqno++;
        sprintf(chunk_name, "%s/DLog-%06d", topdir.c_str(), seqno);

        int fd = open(chunk_name, O_RDWR|O_CREAT, 0666);

        if (fd < 0) {
            std::cout << "Failed to open Log file: " << chunk_name << std::endl;
            return false;
        }

        uint64_t current_chunk_size = chunk_size;
        if (ftruncate(fd, current_chunk_size) != 0) {
            std::cout << "Failed to set Log file size: " << chunk_name << std::endl;
            close(fd);
            return false;
        }

        void * maddr = mmap(NULL, current_chunk_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);  

        close(fd);

        chunk_hdr_t * hdr = (chunk_hdr_t *)maddr;
        hdr->Sig =      DLOG_SIGNATURE;
        hdr->version =  0;
        hdr->seqno =    seqno;
        hdr->sealed =   false;
        hdr->dsize =    0;
        hdr->bgn_off =  sizeof(chunk_hdr_t);
        hdr->fsize =    current_chunk_size;

        // Setup chunk_t
        chunk_t * chunkp = new chunk_t;
        chunkp->path =  std::string(chunk_name);
        chunkp->maddr = maddr;
        chunkp->hdr =   hdr;
        chunkp->next =  NULL;

        insert_chunk(chunkp);

        return true;
    }

  private:
    // Load log file, return chunk_t *. Or if failed, NULL.
    chunk_t * load_one_chunk (const char *dir, const char *logname)
    {
        uint32_t logno;
        char logpath[256];

        if (sscanf(logname, "DLog-%d", &logno) != 1) {
            return NULL;
        }

        sprintf(logpath, "%s/%s", dir, logname);

        int fd = open(logpath, O_RDWR);

        if (fd < 0) {
            std::cout << "Error: can not open log file: " << logpath << std::endl;
            close(fd);
            return NULL;
        }

        struct stat st;
        int ret = fstat(fd, &st);

        if (ret < 0) {
            std::cout << "Error: can not stat log file: " << logpath << std::endl;
            close(fd);
            return NULL;
        }

        void * maddr = mmap(NULL, st.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);  

        close(fd);

        if (!maddr) {
            std::cout << "Mmap failed: " << logpath << std::endl;
            return NULL;
        }

        chunk_hdr_t *hdr = (chunk_hdr_t *)maddr;

        if (hdr->Sig != DLOG_SIGNATURE) {
            std::cout << "Error: log file: " << logpath << " : bad magic " << std::endl;
            return NULL;
        }
        assert(hdr->seqno == logno);
        assert(hdr->fsize == st.st_size);

        // Setup chunk_t
        chunk_t * chunkp = new chunk_t;
        chunkp->path =  std::string(logpath);
        chunkp->maddr = maddr;
        chunkp->hdr =   (chunk_hdr_t *)maddr;
        chunkp->next =  NULL;

        return chunkp;
    }

    // Insert a chunk to chunk_head list
    void insert_chunk(chunk_t *chunkp)
    {
        mtx.lock();
        // Insert chunk to chunk list
        chunk_t **cur = &chunk_head;
        while( *cur ) {
            assert ((*cur)->hdr->seqno != chunkp->hdr->seqno);

            if ((*cur)->hdr->seqno > chunkp->hdr->seqno) {  
                break;
            }
            cur = &(*cur)->next;
        }

        chunkp->next = (*cur);
        *cur = chunkp;

        if (!chunk_tail || (chunkp->hdr->sealed && (chunk_tail->hdr->seqno < chunkp->hdr->seqno))) {
            chunk_tail = chunkp;
        }

        if (next_seqno <= chunkp->hdr->seqno) {
            next_seqno = chunkp->hdr->seqno + 1;
        }

        mtx.unlock();
    }

    // Return starting log offset of the chunk
    inline uint64_t chunk_offset(chunk_t *c)
    {
        chunk_t * tmp = chunk_head;
        uint64_t off = 0;
        while (tmp && tmp != c) {
            off += tmp->hdr->dsize;
            tmp = tmp->next;
        }
        return off;
    }

    int mkpath(const char *dir, mode_t mode)
    {
        struct stat st;
        if (!dir)
            return 0;

        if (!stat(dir, &st))
            return 0;

        mkpath(dirname(strdupa(dir)), mode);

        return mkdir(dir, mode);
    }

    void load_chunks (const char *logdir)
    {
        DIR * dir;
        
        if((dir = opendir(logdir)) == NULL) {
            if (mkpath(logdir, 0777) != 0) {
                std::cout << "Failed to create Log dir: " << logdir << std::endl;
                exit (1);
            }
        }

        // Scan chunk files
        struct dirent *dent;
        while ((dent = readdir(dir)) != NULL) {
            uint32_t logno;
            if (sscanf(dent->d_name, "DLog-%d", &logno) == 1) {
                chunk_t * chunkp = load_one_chunk(logdir, dent->d_name);
                if (chunkp)
                    insert_chunk(chunkp);
           }
        }

        closedir(dir);
    }

    void clean_chunks (const char *logdir)
    {
        DIR * dir;
        
        if((dir = opendir(logdir)) == NULL) {
            return;
        }

        // Scan chunk files
        struct dirent *dent;
        while ((dent = readdir(dir)) != NULL) {
            uint32_t logno;
            if (sscanf(dent->d_name, "DLog-%d", &logno) == 1) {
                std::string path = std::string(logdir) + "/" + std::string(dent->d_name);
                int ret = unlink(path.c_str());
                assert(ret == 0);
           }
        }

        closedir(dir);
    }

    // private variables
    std::mutex mtx;
    std::string topdir;
    std::atomic<uint32_t> next_seqno;
    uint64_t chunk_size;
    chunk_t * chunk_head, * chunk_tail;
};

} // End QDB namespace

