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
#include "Cycles.h"
#include "ClusterTimeService.h"

static inline uint64_t getusec()
{
	struct timeval tv;
	gettimeofday(&tv,NULL);
	return (uint64_t)(1000000*tv.tv_sec) + tv.tv_usec;
}

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

ClusterTimeService::ClusterTimeService()
{
    char ifname[64], ipaddr[64];
    uint32_t ret, i1, i2, i3, i4;

    ret = getifname(ifname, sizeof(ifname)); assert(ret == 0);
    ret = getipaddr(ifname, ipaddr, sizeof(ipaddr)); assert(ret == 0);
    ret = sscanf(ipaddr, "%d.%d.%d.%d", &i1, &i2, &i3, &i4); assert(ret == 4);

    node_id = i4; 
    last_usec = getusec();
    ctr  = 0;
}

// return a cluster unique logical time stamp
uint64_t ClusterTimeService::getClusterTime()
{
    uint64_t usec = getusec();
    if (usec > last_usec) {
        last_usec = usec;
        ctr = 0;
    }
    return (last_usec << 20) + (ctr++ << 10) + node_id;
}

// return a cluster unique time stamp + delta
uint64_t ClusterTimeService::getClusterTime(uint32_t delta/* nsec*/)
{
    uint64_t usec = getusec();
    if (usec > last_usec) {
        last_usec = usec;
        ctr = 0;
    }
    return (last_usec << 20) + ((ctr++ + delta) << 10) + node_id;
}

// return a local system clock time stamp
uint64_t ClusterTimeService::getLocalTime()
{
    uint64_t usec = getusec();
    if (usec != last_usec) {
        last_usec = usec;
        ctr = 0;
    }
    return (last_usec << 10) + ctr++;
}

// Convert a cluster time stamp to local clock time stamp
uint64_t ClusterTimeService::Cluster2Local(uint64_t cluster_ts)
{
    return (cluster_ts >> 10);
}

} // DSSN

