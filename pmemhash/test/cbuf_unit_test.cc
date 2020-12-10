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
#include "hash_map.h"
#include "cbuf_per_thread.h"

int thread_run_run = 1;			// global switch

typedef void *(*thread_func_t)(void *arg);

void * insert_cbuf(void *arg)
{
    int tid = (int)(uint64_t)arg;
    printf("starting insert_cbuf tid=%d\n", tid);
    cbuf_resize(1024); // reduce buffer size to exercise corner cases

    while (thread_run_run) 
    {
	    cbuf_desc_t *desc;
        char *wdata = (char *)"0123456789";
	    for (auto idx = 0; idx < 1024; idx++) {
			char key[CBUF_MAX_KEY_LEN];
			sprintf(key, "%16d", idx);
		    desc = cbuf_put(key, wdata, strlen(wdata) + idx);
            if (desc) {
				cbuf_desc_rd_lock(desc);
                uint32_t ret;
                char buf[strlen(wdata)];
                ret = cbuf_read(desc, buf, sizeof(buf));
				cbuf_desc_rd_unlock(desc);
                assert(ret == sizeof(buf));
                assert(memcmp(buf, wdata, sizeof(buf)) == 0);
            } else {
				if (cbuf_size() > cbuf_record_size(idx + strlen(wdata)))
		        	printf("\tcbuf_put failed len = %ld, cbuf size = %ld\n", idx + strlen(wdata), cbuf_size());
            }
	    }
    }
    return NULL;
}

void run_parallel(int nthreads, int run_time/* #sec */, thread_func_t func)
{
	pthread_t tid[nthreads];
	// 
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
	run_parallel(10, 3, insert_cbuf);
}
