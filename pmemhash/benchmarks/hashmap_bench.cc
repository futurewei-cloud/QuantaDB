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

uint64_t TOTAL_ELEM_SIZE;
uint32_t ELEM_BOUND;
Element *elem;

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
	elem_pointer<Element> elem_ret[10];
	uint64_t ctr = 0, idx, bgn_time, end_time;

	bgn_time = getusec();
	while (thread_run_run) 
	{
		idx = (ctr & (ELEM_BOUND -1)) + index;
		elem_ret[0] = my_hashtable.get(elem[idx+0].key);
		elem_ret[1] = my_hashtable.get(elem[idx+1].key);
		elem_ret[2] = my_hashtable.get(elem[idx+2].key);
		elem_ret[3] = my_hashtable.get(elem[idx+3].key);
		elem_ret[4] = my_hashtable.get(elem[idx+4].key);

		elem_ret[5] = my_hashtable.get(elem[idx+5].key);
		elem_ret[6] = my_hashtable.get(elem[idx+6].key);
		elem_ret[7] = my_hashtable.get(elem[idx+7].key);
		elem_ret[8] = my_hashtable.get(elem[idx+8].key);
		elem_ret[9] = my_hashtable.get(elem[idx+9].key);

		ctr += 10;
	}
	end_time = getusec();

	uint64_t thruput = ctr*1000000/(end_time - bgn_time);

	return thruput;
}
uint64_t mt_insert_test_s(uint64_t index)
{
	elem_pointer<Element> elem_ret;
	uint64_t ctr = 0, idx, bgn_time, end_time;

	bgn_time = getusec();
	while (thread_run_run) 
	{
		idx = (ctr & (ELEM_BOUND -1)) + index;
		elem_ret = my_hashtable.put(elem[idx+0].key, &elem[idx+0]);
		elem_ret = my_hashtable.put(elem[idx+1].key, &elem[idx+1]);
		elem_ret = my_hashtable.put(elem[idx+2].key, &elem[idx+2]);
		elem_ret = my_hashtable.put(elem[idx+3].key, &elem[idx+3]);
		elem_ret = my_hashtable.put(elem[idx+4].key, &elem[idx+4]);

		elem_ret = my_hashtable.put(elem[idx+5].key, &elem[idx+5]);
		elem_ret = my_hashtable.put(elem[idx+6].key, &elem[idx+6]);
		elem_ret = my_hashtable.put(elem[idx+7].key, &elem[idx+7]);
		elem_ret = my_hashtable.put(elem[idx+8].key, &elem[idx+8]);
		elem_ret = my_hashtable.put(elem[idx+9].key, &elem[idx+9]);

		ctr += 10;
	}
	end_time = getusec();

	uint64_t thruput = ctr*1000000/(end_time - bgn_time);

	// printf("hashmap insert key=%lu, thruput = %lu/sec\n", key, thruput);
	return thruput;
}

void * mt_lookup_test(void *arg)
{
	uint32_t tid = (uint32_t)(uint64_t)arg;
	return (void*)mt_lookup_test_s(tid*ELEM_BOUND);
}

void * mt_insert_test(void *arg)
{
	uint32_t tid = (uint32_t)(uint64_t)arg;
	return (void*)mt_insert_test_s(tid*ELEM_BOUND);
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

void init_elem(bool contention)
{
	for (uint32_t i = 0; i < TOTAL_ELEM_SIZE; i++) {
		if (contention) {
			elem[i].key = rand();
			elem[i].value = elem[i].key << 2;
		} else {
			elem[i].key = i;
			elem[i].value = i << 2;
		}
	}
}

int main(int ac, char *av[])
{
    ELEM_BOUND = (ac == 1)? 65536 : atoi(av[1]);
    TOTAL_ELEM_SIZE  = ELEM_BOUND * 32;

    if ((elem = new Element[TOTAL_ELEM_SIZE]) == NULL) {
        printf("Mem alloc failed\n");
        exit(1);
    }

    printf("Pmemhash benchmark. Elem bound = %d\n", ELEM_BOUND);

	uint64_t total;

	setlocale(LC_NUMERIC, "");

	for (uint32_t i = 0; i < 2 ; i++) {
		init_elem(i);
		printf("========== Hash Map MT Insert Benchmark - contention:%d ==\n", i);

		total = run_parallel(1, 10 /* #sec */, mt_insert_test);
		printf("1      thread  total (insert/sec) = %'lu\n", total); fflush(stdout);

        // printf("Evict=%d Insert=%d\n", my_hashtable.get_evict_count(), my_hashtable.get_insert_count());

		total = run_parallel(2, 10 /* #sec */, mt_insert_test);
		printf("2      thread  total (insert/sec) = %'lu\n", total); fflush(stdout);

		total = run_parallel(4, 10 /* #sec */, mt_insert_test);
		printf("4      threads total (insert/sec) = %'lu\n", total); fflush(stdout);

		total = run_parallel(8, 10 /* #sec */, mt_insert_test);
		printf("8      threads total (insert/sec) = %'lu\n", total); fflush(stdout);

		total = run_parallel(16, 10 /* #sec */, mt_insert_test);
		printf("16     threads total (insert/sec) = %'lu\n", total); fflush(stdout);

		total = run_parallel(32, 10 /* #sec */, mt_insert_test);
		printf("32     threads total (insert/sec) = %'lu\n", total); fflush(stdout);

		//total = run_parallel(64, 10 /* #sec */, mt_insert_test);
		//printf("64     threads total (insert/sec) = %'lu\n", total); fflush(stdout);

		printf("========== Hash Map MT Lookup Benchmark - contention:%d ==\n", i);

		total = run_parallel(1, 10 /* #sec */, mt_lookup_test);
		printf("1      thread  total (lookup/sec) = %'lu\n", total); fflush(stdout);

		total = run_parallel(2, 10 /* #sec */, mt_lookup_test);
		printf("2      thread  total (lookup/sec) = %'lu\n", total); fflush(stdout);

		total = run_parallel(4, 10 /* #sec */, mt_lookup_test);
		printf("4      threads total (lookup/sec) = %'lu\n", total); fflush(stdout);

		total = run_parallel(8, 10 /* #sec */, mt_lookup_test);
		printf("8      threads total (lookup/sec) = %'lu\n", total); fflush(stdout);

		total = run_parallel(16, 10 /* #sec */, mt_lookup_test);
		printf("16     threads total (lookup/sec) = %'lu\n", total); fflush(stdout);

		total = run_parallel(32, 10 /* #sec */, mt_lookup_test);
		printf("32     threads total (lookup/sec) = %'lu\n", total); fflush(stdout);

		//total = run_parallel(64, 10 /* #sec */, mt_lookup_test);
		//printf("64     threads total (insert/sec) = %'lu\n", total); fflush(stdout);
	}

}
