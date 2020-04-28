#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
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

hash_table<Element, uint64_t, uint64_t, std::hash<uint64_t>> my_hashtable;

// -- MT test globals --- //
#define	N_THREAD 10
#define MT_INSERT_KEY 1357
int thread_run_run = 0;			// global switch
int thread_run_time = 3;		// sec
Element mt_insert_elem[N_THREAD];

void * mt_insert_test(void *arg)
{
    int tid = (int)(uint64_t)arg;
    elem_pointer<Element> elem_ret;

    printf("mt_insert tid=%d\n", tid);

    while (thread_run_run) 
    {
    	elem_ret = my_hashtable.put(mt_insert_elem[tid].key, &mt_insert_elem[tid]);
    }

    return NULL;
}

int main(void)
{
    elem_pointer<Element> elem_ret;
    printf("========== Hash Map MT Test ==\n");
	pthread_t tid[N_THREAD];

	// Init mt_insert_elem[MT_INSERT_KEY];
	for(auto idx = 0; idx < N_THREAD; idx++) {
	    mt_insert_elem[idx].key = MT_INSERT_KEY;
	    mt_insert_elem[idx].value = idx;
	}

	thread_run_run = 1;
	// 
	for (auto idx = 0; idx < N_THREAD; idx++) {
	    pthread_create(&tid[idx], NULL, mt_insert_test, (void *)(uint64_t)idx);
	}

	sleep(thread_run_time);
	thread_run_run = 0;

	for (auto idx = 0; idx < N_THREAD; idx++) {
	    pthread_join(tid[idx], NULL);
	}
	

    elem_ret = my_hashtable.get(MT_INSERT_KEY);
    assert (elem_ret.ptr_!=NULL);
    printf("MT INSERT Result: bucket:%i slot:%i key:%lu, value:%lu\n", elem_ret.bucket_, elem_ret.slot_,
                elem_ret.ptr_->key, elem_ret.ptr_->value);
}
