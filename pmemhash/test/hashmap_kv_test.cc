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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include "hashmap_kv_rcache.h"

HashmapKV myKV(10, 2048); // 10 prefetch threads, hash table with 2048 bucket count

void cbfunc(cbuf_desc_t *desc)
{
	char buf[desc->len];
	uint32_t nrd = cbuf_read(desc, buf, desc->len);
	assert(nrd == desc->len || nrd == 0);
	if (nrd == desc->len) {
		uint32_t plogid;
		int n = sscanf(buf, "plogid=%d ", &plogid);
		assert(n == 1);
		assert (plogid == plog_id(desc));
		// printf("cbfunc offset=%ld len=%d data=%.14s\n", plog_offset(desc), desc->len, buf);
	} else {
		printf("cbuf_read ret 0\n");
		assert(0);
	}
}

int thread_run_run = 0;			// global switch

typedef void *(*thread_func_t)(void *arg);

void * per_thread_test_function(void *arg)
{
	printf("starting thread id %ld ...\n", (uint64_t)arg);
	cbuf_resize(1024*64);
    while(thread_run_run) {
		for(auto plogid = 0; plogid < 10000; plogid++) {
			//myKV.Prefetch(plogid, plogid, 1024);
			myKV.Get(plogid, plogid, 1024, cbfunc);	// this should bring data into cache
			myKV.Get(plogid, plogid, 1024, cbfunc);	// this should hit data in cache
		}
    }
    return NULL;
}

void run_parallel(int nthreads, int run_time/* #sec */, thread_func_t func)
{
	pthread_t tid[nthreads];
	// 
	thread_run_run = 1;
	for (auto idx = 0; idx < nthreads; idx++) {
	    pthread_create(&tid[idx], NULL, func, (void *)(uint64_t)idx);
	}

	sleep(run_time);
	thread_run_run = 0;

	for (auto idx = 0; idx < nthreads; idx++) {
	    pthread_join(tid[idx], NULL);
	}
}

int main(void)
{
	run_parallel(10, 3, per_thread_test_function);

	printf("HashmapKV cache miss = %u cache hit = %u \n", myKV.stat_cache_miss.load(), myKV.stat_cache_hit.load());
	printf("\t prefetch queue full count = %d \n", myKV.stat_prefetch_q_full.load());
	printf("\t bucket count = %d \n", myKV.bucket_count);
}
