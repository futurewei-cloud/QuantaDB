/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#include <x86intrin.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <assert.h>
#include <iostream>
#include "Cycles.h"
#include "ClusterTimeService.h"

static int getifname(char ifname[], uint32_t len)
{
    FILE *f;
    char line[100] , *p , *c;

    f = fopen("/proc/net/route", "r");

    while(fgets(line , 100 , f))
    {
        p = strtok(line , " \t");
        c = strtok(NULL , " \t");
            
        if(p!=NULL && c!=NULL)
        {
            if(strcmp(c , "00000000") == 0)
            {
                assert(len > strlen(p));
                strcpy(ifname, p);
                return 0;
            }
        }
    }
    return ENOENT;;
}


static int getipaddr(char *ifname, char ipaddr[]/*out*/, uint32_t len)
{
    struct ifaddrs *ifaddr, *ifa;
	int family , s;
	char host[NI_MAXHOST];
    int ret = 0;

	if (getifaddrs(&ifaddr) == -1) 
	{
		perror("getifaddrs");
		exit(EXIT_FAILURE);
	}

	//Walk through linked list, maintaining head pointer so we can free list later
	for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) 
	{
		if (ifa->ifa_addr == NULL)
		{
			continue;
		}

		family = ifa->ifa_addr->sa_family;

		if(strcmp( ifa->ifa_name , ifname) == 0)
		{
			if (family == AF_INET)
			{
				s = getnameinfo( ifa->ifa_addr, (family == AF_INET) ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6) , host , NI_MAXHOST , NULL , 0 , NI_NUMERICHOST);
				
				if (s != 0) 
				{
					// printf("getnameinfo() failed: %s\n", gai_strerror(s));
					exit(EXIT_FAILURE);
				}
				// printf("address: %s", host);
				assert(strlen(host) < len);
                strcpy(ipaddr, host);
                goto out;
			}
			// printf("\n");
		}
	}
    ret = ENOENT;
out:
	freeifaddrs(ifaddr);
    return ret;
}


namespace DSSN {
using namespace RAMCloud;

void * ClusterTimeService::update_ts_tracker(void *arg)
{
    ClusterTimeService *ctsp = (ClusterTimeService*)arg;
    ts_tracker_t *tp = ctsp->tp;

    // See if another tracker may be already running
    if (tp->idx <= 1) {
        uint64_t nsec = tp->nt[tp->idx].last_nsec;
        usleep(3);
        if (nsec != tp->nt[tp->idx].last_nsec) {
            ctsp->thread_run_run = false;
            ctsp->tracker_init   = true;
            return NULL;
        }
    }

    // Init
    tp->nt[0].last_nsec = tp->nt[1].last_nsec = getnsec();
    tp->nt[0].ctr       = tp->nt[1].ctr       = 0;
    tp->idx = 0;
    ctsp->tracker_init = true;

    while (ctsp->thread_run_run) {
        nt_pair_t *ntp = &tp->nt[tp->idx];
        uint64_t nsec = getnsec();

        if (nsec > ntp->last_nsec + ntp->ctr) {
            int nidx = 1 - tp->idx;
            tp->nt[nidx].last_nsec = nsec;
            tp->nt[nidx].ctr = 0;
            tp->idx = nidx;
        }
        usleep(2);
    }

    return NULL;
}

ClusterTimeService::ClusterTimeService()
{
    char ifname[64], ipaddr[64];
    uint32_t i1, i2, i3, i4;
    int fd;

    // Parse IF address and get node id.
    if ((getifname(ifname, sizeof(ifname)) != 0) ||
        (getipaddr(ifname, ipaddr, sizeof(ipaddr)) != 0) ||
        (sscanf(ipaddr, "%d.%d.%d.%d", &i1, &i2, &i3, &i4) != 4)) {
        printf("Fatal Error: can not get default IP addr\n");
        *(int *)0 = 0;  // panic
    }
    node_id = i4; 

    // attached to shared delta tracker
    if ((fd = shm_open(TS_TRACKER_NAME, O_CREAT|O_RDWR, 0666)) == -1) {
        printf("Fatal Error: shm_open failed. Errno=%d\n", errno);
        *(int *)0 = 0;  // panic
    }

    struct stat st;
    int ret = fstat(fd, &st); assert(ret == 0); 
    if (st.st_size == 0) {
        ret = ftruncate(fd, sizeof(ts_tracker_t));
        assert(ret == 0);
    }

    tp = (ts_tracker_t *)mmap(NULL, sizeof(ts_tracker_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    assert(tp);

    close(fd);

    // Start tracker thread
    thread_run_run = true;
    tracker_init = false;
	pthread_create(&tid, NULL, update_ts_tracker, (void *)this);

    while(!tracker_init)
        usleep(1);
}

ClusterTimeService::~ClusterTimeService()
{
    if (thread_run_run) {
        void * ret;
        thread_run_run = false;
	    pthread_join(tid, &ret);
    }
    munmap(tp, sizeof(ts_tracker_t));
}

} // DSSN

