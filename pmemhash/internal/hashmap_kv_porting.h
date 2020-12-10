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

/*
 * HashmapKV porting layer
 */
#include <sys/errno.h>

typedef int plogid_t;    // treat plogid_t as POSIX fd
int plog_read(plogid_t plogid, uint32_t offset, uint32_t length, char *buffer, uint32_t *bytes_read/*out*/);

/* Auxiliary function to retry plog_read
 * Call supply buffer[length]
 * Return errno or 0 on success
 */
int read_from_plog(plogid_t plog, uint32_t offset, uint32_t length, char *buffer)
{
	uint32_t dlen, retry = 0;
	do {
		uint32_t ret;
		ret = plog_read(plog, offset, length, buffer, &dlen);
		if (ret != 0) {
			printf("plog_read failed, ret = %d\n", ret);
			return ret;
		}
		assert(ret == 0 && dlen == length);
	} while ((dlen != length) && (retry++ < 5));

	return (dlen == length)? 0 : EIO;
}

// Porting layer
int plog_read(plogid_t plogid, uint32_t offset, uint32_t length, char *buffer, uint32_t *bytes_read/*out*/)
{
	// fill buffer with "plogid=xx"
	char buf[128];
	sprintf(buf, "plogid=%d ", plogid);
	uint32_t dlen = strlen(buf);
	uint32_t done, ncopy;
	for (done = ncopy = 0; done < length; done += ncopy)
	{
		uint32_t remain = length - done;
		ncopy = (remain > dlen)? dlen : remain;
		memcpy(buffer + done, buf, ncopy);
	}
	*bytes_read = done;
	return 0;
}
