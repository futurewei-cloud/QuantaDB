#include <x86intrin.h>
#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <sched.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/time.h>
#include <errno.h>
#include <string>
#include <atomic>
#include <iostream>

#define MAX_CORE 32
sem_t child, parent;
bool child_start = false;

static inline u_int64_t rdtscp(u_int32_t &aux)
{
    u_int64_t rax,rdx;
    asm volatile ( "rdtscp\n" : "=a" (rax), "=d" (rdx), "=c" (aux) : : );
    return (rdx << 32) + rax;
}

int thread_run_run = 1;
std::atomic<uint64_t> tscbuf[MAX_CORE];

void *func(void *arg)
{
    u_int32_t core;
    uint32_t id = (uint32_t)(uint64_t)arg;

    printf("thread %d starting on core %d ...\n", id, sched_getcpu());

    while (thread_run_run) {
        uint32_t cmp_id = (id > 0)? id - 1 : MAX_CORE - 1;
        uint64_t tsc_cmp = tscbuf[cmp_id];
        #if (0)
        uint64_t tsc = __rdtsc();
        #else
        uint64_t tsc = __rdtscp(&core);
        #endif
        if (tsc < tsc_cmp) {
            printf("Error: tsc[%d] %ld, tsc[%d] %ld diff %ld\n", id, tsc, cmp_id, tsc_cmp, tsc_cmp - tsc); 
        }
        tscbuf[id] = tsc;
    }

}

int main(int ac, char *av[])
{
    int runtime = (ac == 1)? 5 : atoi(av[1]);
        
    pthread_t threads[MAX_CORE];
    for (int i=0; i<MAX_CORE; i++) {
        pthread_create(&threads[i], NULL, func, (void*)(u_int64_t)i);
    }

    sleep(runtime);

    thread_run_run = 0;

    for (int i=0; i<MAX_CORE; i++) {
        pthread_join(threads[i], NULL);
    }

    return 0;
}
