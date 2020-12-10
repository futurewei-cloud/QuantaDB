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
#include <sys/time.h>
#include <pthread.h>
#include "hash_map.h"

class Element
{
public:
    uint64_t key;
    uint64_t value;

    Element(uint64_t k = 0, uint64_t v = 0) { key=k; value=v; }
    inline uint64_t getKey() { return key; }
};

#define MAX_THREADS 32
#define ELEM_BOUND 65536
Element elem[ELEM_BOUND * MAX_THREADS];

hash_table<Element, uint64_t, uint64_t, std::hash<uint64_t>> my_hashtable;
volatile int thread_run_run = 0;			// global switch

inline uint64_t getusec()
{
	struct timeval tv;
	gettimeofday(&tv,NULL);
	return (uint64_t)(1000000*tv.tv_sec) + tv.tv_usec;
}

typedef void *(*thread_func_t)(void *arg);

uint64_t mt_lookup_test_s(uint64_t index)
{
	elem_pointer<Element> elem_ret;
	uint64_t ctr = 0, idx;

	while (thread_run_run) 
	{
		idx = (ctr & (ELEM_BOUND -1)) + index;
		elem_ret = my_hashtable.get(elem[idx].key);
        assert(elem_ret.ptr_->value == (elem[idx].key << 2));
		ctr++;
	}

	return 0;
}
uint64_t mt_lookup_after_insert_test_s(uint64_t index)
{
	elem_pointer<Element> elem_ret;
	uint64_t ctr = 0, idx;

	while (thread_run_run) 
	{
		idx = (ctr & (ELEM_BOUND -1)) + index;
		elem_ret = my_hashtable.put(elem[idx].key, &elem[idx]);
        assert(elem_ret.ptr_ == &elem[idx]);
		elem_ret = my_hashtable.get(elem[idx].key);
        assert(elem_ret.ptr_->value == (elem[idx].key << 2));

        // modify value
		elem[idx].value = ctr;
		elem_ret = my_hashtable.put(elem[idx].key, &elem[idx]);
		elem_ret = my_hashtable.get(elem[idx].key);
        assert(elem_ret.ptr_->value == ctr);

        // restore value (so that next round will not break)
		elem[idx].value = elem[idx].key << 2;

		ctr++;
	}
	return 0;
}

void * mt_lookup_test(void *arg)
{
	uint32_t tid = (uint32_t)(uint64_t)arg;
	return (void*)mt_lookup_test_s(tid*ELEM_BOUND);
}

void * mt_lookup_after_insert_test(void *arg)
{
	uint32_t tid = (uint32_t)(uint64_t)arg;
	return (void*)mt_lookup_after_insert_test_s(tid*ELEM_BOUND);
}

uint64_t run_parallel(int nthreads, int run_time/* #sec */, thread_func_t func)
{
	pthread_t tid[nthreads];
	// 
	thread_run_run = 1;
	for (auto idx = 0; idx < nthreads; idx++) {
	    pthread_create(&tid[idx], NULL, func, (void *)(uint64_t)idx);
	}

	sleep(run_time);
	thread_run_run = 0;

	uint64_t total = 0;

	for (auto idx = 0; idx < nthreads; idx++) {
		void * ret;
	    pthread_join(tid[idx], &ret);
		total += (uint64_t)ret;
	}

	return total;
}

void init_elem(bool randomkey)
{
	for (uint32_t i = 0; i < ELEM_BOUND * MAX_THREADS; i++) {
		elem[i].key = (randomkey)? rand() : i;
		elem[i].value = elem[i].key << 2;
	}
}

int main(void)
{
    int nsecond = 3;

	setlocale(LC_NUMERIC, "");

	init_elem(false);
	printf("Sequential key test\n");
    for (int nthread = 1; nthread < MAX_THREADS; nthread <<= 1) {
		printf("========== %d thread(s) Hash Map MT read-after-write test\n", nthread);
		run_parallel(nthread, nsecond, mt_lookup_after_insert_test);
        // printf("Evict=%d Insert=%d\n", my_hashtable.get_evict_count(), my_hashtable.get_insert_count());

		printf("========== %d thread(s) Hash Map MT Lookup Test \n", nthread);
		run_parallel(nthread, nsecond, mt_lookup_test);
    }

}
