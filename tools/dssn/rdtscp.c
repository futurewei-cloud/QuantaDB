#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>

#define MAX_CORE 32
sem_t child, parent;
bool child_start = false;

static inline u_int64_t rdtscp(u_int32_t &aux)
{
    u_int64_t rax,rdx;
    asm volatile ( "rdtscp\n" : "=a" (rax), "=d" (rdx), "=c" (aux) : : );
    return (rdx << 32) + rax;
}


void *func(void *arg)
{
    u_int64_t tsc_start, tsc_end;
    u_int32_t core;
    struct timespec clock_start, clock_end;
    sem_post(&parent);
#ifdef CHILD_USE_SEM
    sem_wait(&child);
#else
    while (child_start == false) {;}
#endif
    tsc_start = rdtscp(core);
    clock_gettime(CLOCK_REALTIME, &clock_start);
    int i,j; for (i=0, j=0; i< 65536; i++) { j+= i*i; }
    tsc_end = rdtscp(core);
    clock_gettime(CLOCK_REALTIME, &clock_end);

    u_int64_t tsc_diff = tsc_end - tsc_start;
    int64_t clock_diff = clock_end.tv_nsec - clock_start.tv_nsec;
    clock_diff += (clock_diff < 0) ? 1000000000 : 0;

    float scale = (float) tsc_diff / (float) clock_diff;
    printf("t[%02lx] c[%04x] tsc %08ld %08ld clock %08ld %08ld tsc diff %08ld clock diff %08ld %1.4f\n",
        (u_int64_t)arg, core, tsc_start, tsc_end, clock_start.tv_nsec, clock_end.tv_nsec,
        tsc_diff, clock_diff, scale);

}

int main()
{
    pthread_t threads[MAX_CORE];
    sem_init(&child, 0, 0);    //shared within process, all blocked
    sem_init(&parent, 0, 0);

    for (int i=0; i<MAX_CORE; i++) {
        pthread_create(&threads[i], NULL, func, (void*)(u_int64_t)i);
    }

    //wait for all child to be ready.
    for (int i=0; i<MAX_CORE; i++) {
        sem_wait(&parent);
    } 

#ifdef CHILD_USE_SEM
    for (int i=0; i<MAX_CORE; i++) {
        sem_post(&child);
    }
#else
    child_start = true;
#endif

    for (int i=0; i<MAX_CORE; i++) {
        pthread_join(threads[i], NULL);
    }

    return 0;
}
