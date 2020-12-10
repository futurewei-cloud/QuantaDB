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
#include "hash_map.h"

/*
 * A simple multi-thread test for hash_map thread safety.
 */

class Element
{
public:
    uint64_t key;
    uint64_t value;

    Element(uint64_t k = 0, uint64_t v = 0) { key=k; value=v; }
    inline uint64_t getKey() { return key; }
};

hash_table<Element, uint64_t, uint64_t, std::hash<uint64_t>> my_hashtable;

// -- MT test globals --- //
#define	N_THREAD 10
#define MT_INSERT_KEY 1357
int thread_run_run = 0;			// global switch
int thread_run_time = 3;		// sec
Element mt_insert_elem[N_THREAD];

void * mt_insert_test(void *arg)
{
    int tid = (int)*(uint64_t*)arg;
    elem_pointer<Element> elem_ret;
    uint64_t ctr = 0;

    printf("mt_insert tid=%d\n", tid);

    while (thread_run_run) 
    {
    	elem_ret = my_hashtable.put(mt_insert_elem[tid].key, &mt_insert_elem[tid]);
        ctr++;
    }

    *(uint64_t*)arg = ctr;

    return NULL;
}

int main(void)
{
    elem_pointer<Element> elem_ret;
    printf("========== Hash Map MT Test ==\n");
    uint64_t id_ctr[N_THREAD];
	pthread_t tid[N_THREAD];

	// Init mt_insert_elem[MT_INSERT_KEY];
	for(auto idx = 0; idx < N_THREAD; idx++) {
	    mt_insert_elem[idx].key = MT_INSERT_KEY;
	    mt_insert_elem[idx].value = idx;
	}

	thread_run_run = 1;
	// 
	for (auto idx = 0; idx < N_THREAD; idx++) {
        id_ctr[idx] = idx;
	    pthread_create(&tid[idx], NULL, mt_insert_test, (void *)&id_ctr[idx]);
	}

	sleep(thread_run_time);
	thread_run_run = 0;

	for (auto idx = 0; idx < N_THREAD; idx++) {
	    pthread_join(tid[idx], NULL);
	}
	
    uint64_t total_puts = 0;
	for (auto idx = 0; idx < N_THREAD; idx++) {
	    total_puts += id_ctr[idx];
	}

    assert(total_puts == (my_hashtable.get_insert_count() + my_hashtable.get_update_count()));

    elem_ret = my_hashtable.get(MT_INSERT_KEY);
    assert (elem_ret.ptr_!=NULL);
    printf("MT INSERT Result: bucket:%i slot:%i key:%lu, value:%lu\n", elem_ret.bucket_, elem_ret.slot_,
                elem_ret.ptr_->key, elem_ret.ptr_->value);
    printf("Evict count = %d\n", my_hashtable.get_evict_count());
    printf("Insert count = %d\n", my_hashtable.get_insert_count());
    printf("Update count = %d\n", my_hashtable.get_update_count());
    printf("Lookup count = %lu\n", my_hashtable.get_lookup_count());
    printf("Average bucket element search length = %u\n", my_hashtable.get_avg_elem_iter_len());
}
