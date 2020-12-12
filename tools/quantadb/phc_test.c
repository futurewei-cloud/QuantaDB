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

/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <assert.h>
#include <net/if.h>
#include <linux/ptp_clock.h>
#include <linux/sockios.h>
#include <linux/ethtool.h>

#define CLOCKFD 3
#define FD_TO_CLOCKID(fd) ((~(clockid_t) (fd) << 3) | CLOCKFD)
#define CLOCKID_TO_FD(clk) ((unsigned int) ~((clk) >> 3))

#ifndef CLOCK_INVALID
#define CLOCK_INVALID -1
#endif

static inline int validate_clockid(clockid_t clkid)
{
    struct ptp_clock_caps caps;
    return ioctl(CLOCKID_TO_FD(clkid), PTP_CLOCK_GETCAPS, &caps);
}

#include <stdio.h>
#include <stdlib.h>

// Return PTP device name to the obuf
int get_ptp_device(char obuf[], int len)
{
  FILE *fp;
  char path[1024];
  int result = ENOENT;

  /* Open the command for reading. */
  fp = popen("ps -ef | fgrep ptp4l", "r");
  if (fp == NULL) {
    printf("FatalError: failed to run command ps -ef\n" );
    exit(1);
  }

  /* Read the output a line at a time - output it. */
  while (fgets(path, sizeof(path), fp) != NULL) {
    char UID[16],  PID[16], PPID[16], C[8],     STIME[16], TTY[16],
         TIME[16], CMD[64], ARG1[16], ARG2[16], ARG3[16], ARG4[16];
    int ret = sscanf(path, "%s %s %s %s %s %s %s %s %s %s %s %s",
                    UID, PID, PPID, C, STIME, TTY, TIME, CMD, ARG1, ARG2, ARG3, ARG4);
    if ((ret == 12) && (strcmp("-i", ARG3) == 0)) {
        // printf("CMD=%s ARG4=%s\n", CMD, ARG4);
        strncpy(obuf, ARG4, len);
        result = 0;
    }
  }

  /* close */
  pclose(fp);

  return result;
}

clockid_t get_clock_id(char *device, int *phc_index)
{
	/* check if device is CLOCK_REALTIME */
	if (!strcasecmp(device, "CLOCK_REALTIME")) {
		return CLOCK_REALTIME;
	}

	int clkid;
	char phc_device[19];
	struct ethtool_ts_info info;
	struct ifreq ifr;
	int fd, err;

	memset(&ifr, 0, sizeof(ifr));
	memset(&info, 0, sizeof(info));

	info.cmd = ETHTOOL_GET_TS_INFO;
	strncpy(ifr.ifr_name, device, IFNAMSIZ - 1);
	ifr.ifr_data = (char *) &info;
	fd = socket(AF_INET, SOCK_DGRAM, 0);

	if (fd < 0) {
		perror("socket failed:");
		return CLOCK_INVALID;
	}

	err = ioctl(fd, SIOCETHTOOL, &ifr);
	if (err < 0) {
		perror("ioctl SIOCETHTOOL failed:");
		close(fd);
		return CLOCK_INVALID;
	}

	close(fd);
    //-----------------------
	if (info.phc_index < 0) {
		printf("interface %s does not have a PHC", device);
		return CLOCK_INVALID;
	}

	snprintf(phc_device, sizeof(phc_device), "/dev/ptp%d", info.phc_index);
    fd = open(phc_device, O_RDONLY);
    if (fd < 0) {
		printf("fatalError: cannot open %s for read. Need to change mode to 644.\n", phc_device);
        return CLOCK_INVALID;
    }

    clkid = FD_TO_CLOCKID(fd);
	*phc_index = info.phc_index;

    assert (validate_clockid(clkid) == 0);

	return clkid;
}

clockid_t get_ptp_clock_id()
{
    int phc_index = -1;
    char ptpdev[16];

    int ret = get_ptp_device(ptpdev, sizeof(ptpdev));
    assert(ret == 0);

    printf("PTP device: %s\n", ptpdev);

    clockid_t clkid = get_clock_id(ptpdev, &phc_index);

    if (clkid == CLOCK_INVALID) {
        printf("FatalWarning: can not get PTP clock id. Use CLOCK_REALTIME instead.\n");
        clkid = CLOCK_REALTIME;
    }

    return clkid;
}

#ifdef MAIN_FUNCTION
int main(int ac, char *av[])
{
    int ret;
    int loopcnt = 10;
    clockid_t ptp = get_ptp_clock_id();
    timespec sys_ts, ptp_ts;

    if (ac >= 2)
        loopcnt = atoi(av[1]);

    while(loopcnt-- > 0) {
        ret = clock_gettime(CLOCK_REALTIME, &sys_ts); assert(ret == 0);
        ret = clock_gettime(ptp, &ptp_ts);            assert(ret == 0);

        printf("sys sec=%ld nsec=%ld \n", sys_ts.tv_sec, sys_ts.tv_nsec);
        printf("ptp sec=%ld nsec=%ld \n", ptp_ts.tv_sec, ptp_ts.tv_nsec);
        printf("diff sec=%ld, nsec=%ld\n\n", ptp_ts.tv_sec - sys_ts.tv_sec, ptp_ts.tv_nsec - sys_ts.tv_nsec);
        sleep(1);
    }
}

#endif // MAIN_FUNCTION
