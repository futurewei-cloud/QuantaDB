/*
 * Copyright (c) 2020 Futurewei Technologies Inc
 */
#include <sys/mman.h>
#include <dirent.h>
#include <string.h>
#include <mutex>

namespace DSSN {
/*
 * DLog (DSSN Log) Design Highlights:
 * - DLog is a local log device. No remote access.
 * - Logicalaly, DLog is a log device with append write and ramdom read.
 * - Physically, DLog is consisted of a set of log files (aka chunk file).
 * - DLog log files are memory mapped for high speed access.
 * - DLog supports concurrent append operation (using fetch_and_add pointer)
 * Non-goals
 * - DLog is not a Plog simulator.
 */
template <uint32_t CHUNK_SIZE = (16*1024*1024) >
class DLog {
  private:
    #define DLOG_SIGNATURE    0xF0F05A5A
    // ondisk chunk header
    typedef struct ondisk_chunk_header {
        uint32_t    Sig;
        uint8_t     version;
        uint8_t     sealed; // bool
        uint32_t    fsize;  // chunk file size
        uint32_t    dsize;  // data size
        uint32_t    seqno;
    } chunk_hdr_t;

    // incore chunk
    typedef struct chunk {
        struct chunk *  next;
        std::string path;
        void *          maddr;
        chunk_hdr_t *   hdr;
        ~chunk()
        {
            munmap(maddr, hdr->fsize);
        }
        void remove() 
        {
            int ret = unlink(path.c_str());
            assert(ret == 0);
            delete this; 
        }
    } chunk_t;

  public:
    DLog(std::string logdir = "/tmp") : topdir(logdir)
    {
        next_seqno = 0;
        chunk_head = chunk_tail = NULL;
        chunk_size = CHUNK_SIZE;

        // Load existing logs
        load_chunks(topdir.c_str());

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
        chunk_t * tmp;
        while ((tmp = chunk_head) != NULL) {
            chunk_head = chunk_head->next;
            delete tmp;
        }
    }

    // Return log data size
    inline uint32_t size(void)
    {
        uint32_t lsize = 0;
        for (chunk_t *tmp = chunk_head; tmp; tmp = tmp->next) {
            lsize += tmp->hdr->dsize;
        }
        return lsize;
    }

    // Return free space
    inline uint32_t free_space(void)
    {
        uint32_t free_size = 0;
        for (chunk_t *tmp = chunk_tail; tmp; tmp = tmp->next) {
            free_size += tmp->hdr->fsize - tmp->hdr->dsize - sizeof(chunk_hdr_t);
        }
        return free_size;
    }

    // Reserve 'len' bytes append space in log.
    // Return starting address of the reserved space.  
    void * reserve(uint32_t len, uint64_t * offset = NULL)
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
            } else {
                uint32_t max_oldsize = chunk->hdr->fsize - (len + sizeof(chunk_hdr_t));
                if ((oldsize = chunk->hdr->dsize) > max_oldsize) {
                    chunk->hdr->sealed = true; // insufficient space, sealed it.
                } else if (__sync_bool_compare_and_swap(&chunk->hdr->dsize, oldsize, oldsize+len)) {
                    break; // done
                }
            }
        } while (true);

        if (offset)
            *offset = chunk_offset(chunk) + oldsize;

        return (char *)chunk->maddr + sizeof(chunk_hdr_t) + oldsize; 
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

    // Trim Log content starting from offset for length bytes. 
    // If length == 0, trim all.
    void trim (uint32_t length)
    {
        uint32_t remain = length;
        chunk_t * tmp, * old_head;
        tmp = old_head = chunk_head;
        while (tmp) {
            if (tmp->hdr->dsize > remain) {
                break;
            }
            remain -= tmp->hdr->dsize;
            tmp = tmp->next;
        }
        chunk_head = tmp;
        tmp = old_head;
        while (tmp != chunk_head) {
            tmp->remove();
            tmp = tmp->next;
        }
    }

    void set_chunk_size(uint32_t size)
    {
        chunk_size = size;
    }

  private:
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

        uint32_t current_chunk_size = chunk_size;
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

    // return starting offset of the chunk
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

    void load_chunks (const char *logdir)
    {
        DIR * dir;
        
        if((dir = opendir(logdir)) == NULL) {
            if (mkdir(logdir, 0777) == -1) {
                std::cout << "Failed to create Log dir: " << dir << std::endl;
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
    }

    // private variables
    std::mutex mtx;
    std::string topdir;
    std::atomic<uint32_t> next_seqno;
    uint32_t chunk_size;
    chunk_t * chunk_head, * chunk_tail;
};

} // End DSSN namespace

