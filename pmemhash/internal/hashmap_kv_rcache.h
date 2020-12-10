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

/*
 * Hashmap KV store.
 * This Hashmap KV store implements a read cache and uses [er thread circular buffer as its cache storage.
 * API:
 * int Get (plogid_t plogid, uint32_t offset, uint32_t len, callback_t cbfunc)
 * int Prefetch (plogid_t plogid, uint32_t offset, uint32_t len)
 */
#pragma once
#include <atomic>
#include <queue>
#include <errno.h>
#include <time.h>
#include "hash_map.h"
#include "cbuf_per_thread.h"
#include "hashmap_kv_porting.h"
#include "c_str_util_classes.h"

using namespace std;

#define	PREFETCH_MAX_THREADS	10
#define	PREFETCH_Q_SIZE			512
#define	HASH_TABLE_TEMPLATE		cbuf_desc_t, char *, uint64_t, hash_c_str
#define	ROUND_DOWN(n,p)			(n & ~(p-1))
#define	ROUND_UP(n,p)			((n + p - 1) & ~(p - 1))

#define	plog_id(desc)			desc->cbuf_custom_field_1	// Use the custom field to store plog id
#define	plog_offset(desc)		desc->cbuf_custom_field_2	// Use the custom field to store plog io offset

typedef void (*callback_t)(cbuf_desc_t *desc); // return a pined cbuf_desc, caller use cbuf_read(desc,...) to get data

typedef struct prefetch_q_entry {
	plogid_t	plog;
	uint32_t 	offset;
	uint32_t 	length;
} prefetch_q_entry_t;

typedef struct prefetch_queue {
	pthread_spinlock_t spinlock;
	queue<prefetch_q_entry_t> prefetch_q;
} prefetch_queue_t;

class HashmapKV
{
public:
	HashmapKV(uint32_t nthread = 3, uint32_t nbucket = 1024) : stat_cache_miss(0), stat_cache_hit(0), stat_prefetch_q_full(0)
	{
		prefetch_thread_count = (nthread <= PREFETCH_MAX_THREADS)? nthread : PREFETCH_MAX_THREADS;
		bucket_count = nbucket;
		my_hashtable = new hash_table<HASH_TABLE_TEMPLATE>(bucket_count);

		// Init prefetch_queue_t
		prefetch_thr_idx = 0;
		prefetch_q_rotor = 0;
		for(uint32_t idx = 0; idx < prefetch_thread_count; idx++) {
			pthread_spin_init(&pfq[idx].spinlock, PTHREAD_PROCESS_SHARED);
		}

		thread_run_run = true;
		for(uint32_t idx = 0; idx < prefetch_thread_count; idx++) {
			pthread_create(&prefetch_thread_id[idx], NULL, prefetch_thread, (void *)(uint64_t)this);
		}
	}

	~HashmapKV()
	{
		thread_run_run = false;
		for(uint32_t idx = 0; idx < prefetch_thread_count; idx++) {
			pthread_join(prefetch_thread_id[idx], NULL);
		}
	}

    // Find <plogid, off, len> from hashmap. If not there, load it.
    // When ready, call cbfunc(buf) with @buf points to the data.
    template <bool WAIT_READ = true>
	int Get(plogid_t plogid, uint32_t offset, uint32_t len, callback_t cbfunc)
	{
		cbuf_desc_t *desc;
		char key[CBUF_MAX_KEY_LEN];

		uint32_t io_offset = ROUND_DOWN(offset, 8192);
		uint32_t io_length = ROUND_UP(len, 8192);

		assert((io_offset + io_length) < 256*1024*1024); // assert plog size limit

		sprintf(key, "%012d-%09d-%09d", plogid, io_offset, io_length); assert(strlen(key) < CBUF_MAX_KEY_LEN); 

        elem_pointer<cbuf_desc_t> elem_ret = my_hashtable->get(key);

        if ((desc = (cbuf_desc_t *)elem_ret.ptr_) != NULL) {
			cbuf_desc_rd_lock(desc);
			if (cbuf_desc_validate(desc) && (strcmp(desc->key, key) == 0) && (desc->len >= io_length)) {
				if (cbfunc) {
					cbfunc(desc);
				}
				cbuf_desc_rd_unlock(desc);
				stat_cache_hit++;
            	return 0;
			}
			cbuf_desc_rd_unlock(desc);
			// Fall thru to cache miss processing
		}

        // if not asked to wait, we can simply return now
        if (!WAIT_READ)
			return 1;

		stat_cache_miss++;	// stat

		// Read data off plog
		char data[io_length];
		uint32_t ret = read_from_plog(plogid, io_offset, io_length, data);
		if(ret != 0) {
			printf("FatalError: plog_read failed, ret=%d\n", ret);
			return ret;
		}

		// Write data to cbuf
		desc = cbuf_put(key, data, io_length);
		assert(desc);
		plog_id(desc)		= plogid;
		plog_offset(desc)	= io_offset; 

		// Insert to hash_table
        my_hashtable->put(key, desc);
        // elem_pointer<cbuf_desc_t> elem_ptr = my_hashtable->put(key, desc);
		// printf("HashmapKV: insert key(%s) to slot=%d bucket=%d\n", key, elem_ptr.slot_, elem_ptr.bucket_);

		// Call cbfunc
		cbuf_desc_rd_lock(desc);
		if (cbfunc)
			cbfunc(desc);
		cbuf_desc_rd_unlock(desc);
        return 0;
	}

	void Prefetch (plogid_t plogid, uint32_t offset, uint32_t len)
	{
		prefetch_q_entry_t entry = {plogid, offset, len};
		uint32_t dest_pfq_idx = prefetch_q_rotor.fetch_add(1, std::memory_order_relaxed) % prefetch_thread_count;
		prefetch_queue_t *myPfq = &pfq[dest_pfq_idx];

		pthread_spin_lock(&myPfq->spinlock);
		if (myPfq->prefetch_q.size() < PREFETCH_Q_SIZE) {
			myPfq->prefetch_q.push(entry);
		} else {
			stat_prefetch_q_full++;
		}
		pthread_spin_unlock(&myPfq->spinlock);
	}

    int Prefetch (plogid_t plogid, uint32_t offset, uint32_t len, callback_t cbfunc)
    {
		int ret = Get<false>(plogid, offset, len, cbfunc);
		if (ret == 0){
			// good, cbfunc is served
			return 0;
		} else if (ret == 1) {
			// target not in pool, we can issue a prefetch
			Prefetch (plogid, offset, len);
			return 1;
		}

		// other error conditions, let caller handle it
		assert (ret != 0 && ret != 1);
		return ret;
    }

	void stat_reset()
	{
		stat_cache_miss = stat_cache_hit = stat_prefetch_q_full = 0;
	}

	static void * prefetch_thread(void *arg)
	{
		HashmapKV *myKV = (HashmapKV *)arg;
		uint32_t pidx = myKV->prefetch_thr_idx.fetch_add(1, std::memory_order_relaxed);

		prefetch_queue_t *myPfq = &myKV->pfq[pidx];

		while (myKV->thread_run_run)
		{
			pthread_spin_lock(&myPfq->spinlock);
			while(!myPfq->prefetch_q.empty() && myKV->thread_run_run)
			{
				prefetch_q_entry_t entry = myPfq->prefetch_q.front();
				myPfq->prefetch_q.pop();
				pthread_spin_unlock(&myPfq->spinlock);
				myKV->Get(entry.plog, entry.offset, entry.length, NULL);
				// printf("prefetch %d %d %d\n", entry.plog, entry.offset, entry.length);
				pthread_spin_lock(&myPfq->spinlock);
			}
			pthread_spin_unlock(&myPfq->spinlock);
			struct timespec ts = {0, 2000};
			nanosleep(&ts, NULL);
		}
		return NULL;
	}

	atomic<uint32_t>	stat_cache_miss,
						stat_cache_hit,
						stat_prefetch_q_full;
	uint32_t			prefetch_thread_count,
						bucket_count;

private:
	hash_table<HASH_TABLE_TEMPLATE> * my_hashtable;
	bool				thread_run_run;
	pthread_t			prefetch_thread_id[PREFETCH_MAX_THREADS];
	prefetch_queue_t	pfq[PREFETCH_MAX_THREADS];
	atomic<uint32_t>	prefetch_thr_idx, prefetch_q_rotor;
};
