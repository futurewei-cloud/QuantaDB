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
#include "Sequencer.h"
#include "HashmapKVStore.h"

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

namespace QDB {

Sequencer::Sequencer()
{
    char ifname[64], ipaddr[64];
    uint32_t i1, i2, i3, i4;

    // Parse IF address and get node id.
    if ((getifname(ifname, sizeof(ifname)) != 0) ||
        (getipaddr(ifname, ipaddr, sizeof(ipaddr)) != 0) ||
        (sscanf(ipaddr, "%d.%d.%d.%d", &i1, &i2, &i3, &i4) != 4)) {
        printf("Fatal Error: can not get default IP addr\n");
        *(int *)0 = 0;  // panic
    }
    node_id = i4;
}

// Return CTS
__uint128_t Sequencer::getCTS()
{
    uint64_t uniqueId;
    uniqueId = (node_id << 48) | (getpid() & 0x0000FFFFFFFFFFFF);
    return ((__uint128_t)(clock.getLocalTime() + SEQUENCER_DELTA) << 64) + uniqueId;
}

} // QDB

