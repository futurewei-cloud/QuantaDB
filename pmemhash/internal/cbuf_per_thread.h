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

/* By Mike H. 2019-09 
 *
 * Per Thread Datagram Circular Buffer.
 * - Datagram insert and remove - not stream data.
 * - Overrun when full - when buffer is full, old datagram is dropped to make room for new.
 *
 * API Summary:
 * - cbuf_desc_t *cbuf_put(string &key, char *data, int len)
 *   Write datagram <data,len> into circular buffer.
 *   Return a cbuf descriptor pointer. The cbuf descriptor references to the datagram in the cbuf.
 *	 Return NULL if datagram is too big to fit the cbuf. 
 * - void cbuf_pop()
 *   Remove data entry at the head to make room for new data.
 * - uint32_t cbuf_size()
 *   Return the size of cbuf
 * - void cbuf_resize(uint32_t new_size)
 *   Resize circular buffer to 'new_size'. This would result in an empty new buffer. Old data would be wiped out.
 * - uint32_t cbuf_read(cbuf_desc_t *desc, char *buf, uint32_t len)
 *	 Given a cbuf descriptor @desc, read data ito @buf of @len size. Return #bytes read.
 *	 Note that, a saved cbuf_desc_t * can be subsequently recycled and reused by the cbuf system. The cbuf_desc->key
 *	 field allows the caller to verify if the cbuf descriptor is still valid (by comparing the key)
 *
 * Implementation Summary:
 * - A thread private cicircular buffer is allocated upon the first invocation from a thread.
 * - A datagram model is used. Data is either all in, or all out.
 * - When free space is insufficient for a new datagram, old are dropped to make room.
 * - Datagran record format:
 *   [<cbuf_entry_hdr> <payload data> <padding>]
 *          |               |          |__________ padding to 16 bytes alignment
 *          |               |_____________________ payload
 *          |_____________________________________ record header
 */
#pragma once
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include "dlist.h"

/*
 * Macros
 */
#define align16(addr) 		(((addr) + 15) & (~15))
#define	cbuf_record_size(len)	(sizeof(cbuf_entry_hdr_t) + align16(len))
#define	CBUF_BUFFER_SIZE	(64*1024*1024)	/* default 64M buffer size */
#define	CBUF_ENTRY_SIG		0xAABBCCDD

/*
 * Data Structures
 */
// circular bufer
typedef struct cbuf {
	char * bgn, * end,	// buffer begin and end pointers
	     * head,* tail;	// head and tail ptr
	uint64_t	size;	// size of buffer
	uint32_t	sig;	// cbuf signature 
	~cbuf()
	{
		/*
		 * Some cbuf_desc_t may be still referencing us.
		 * Clear sig before free
		 */
		sig = 0;
		if (bgn) free(bgn);
	}
} cbuf_t;

thread_local cbuf_t cbuf = {NULL, NULL, NULL, NULL, 0, CBUF_ENTRY_SIG};

struct cbuf_entry_hdr;

// circular buffer entry descriptor
typedef struct cbuf_descriptor {
	#define	CBUF_MAX_KEY_LEN 48
	dlist_t             list;	// free list
	int volatile		rwlock;	// CAS rw lock
	struct cbuf_entry_hdr *hdr;	// cross ref
	cbuf_t *			cbuf;	// the cbuf that holds the data
	uint32_t 			len;	// data size, should match hdr->len
	char				key[CBUF_MAX_KEY_LEN];	//
	uint64_t			cbuf_custom_field_1;		// Custom field to be used by application code
	uint64_t			cbuf_custom_field_2;		// Custom field to be used by application code
    inline char * getKey() { return key; }
} cbuf_desc_t;

// Header struct for a record stored in cbuf
typedef struct cbuf_entry_hdr {
    cbuf_desc_t *desc;  // cross ref to cbuf_desc
    uint32_t    len;    // data len
    uint32_t    sig;    // header signature
} cbuf_entry_hdr_t;

static_assert (sizeof(cbuf_entry_hdr_t) == 16, "The cbuf_record_size() macro assumes cbuf_entry_hdr is 16 bytes");

static void cbuf_desc_free(cbuf_desc_t *desc);
static void cbuf_desc_wr_lock(cbuf_desc_t *desc);

// Allocation for cbuf_desc_t is one chunk at a time
typedef struct cbuf_desc_chunk {
	struct cbuf_desc_chunk * next;      // one way link list of all chunks
	cbuf_desc_t 		 array[1024];   // chunk size 1024
} cbuf_desc_chunk_t;

// Root pointer for cbuf_desc_chunks with auto destructor upon thread exit
typedef struct cbuf_desc_chunk_ptr {
	cbuf_desc_chunk_t * chunk;

	~cbuf_desc_chunk_ptr()  // called upon thread exit
	{
		// for each chunk, do
	    for(cbuf_desc_chunk_t * chunkp = chunk; chunkp; chunkp = chunkp->next)
	    {
			/*
		 	* Simply invalidate all descriptors in the chunk.
		 	* Can not free the chunk because somewhere (e.g., hashmap) may still be
		 	* holding reference to the descriptors.
		 	*/
			for(uint32_t idx = 0; idx < sizeof(chunkp->array)/sizeof(cbuf_desc_t); idx++) {
				cbuf_desc_t *desc = &chunkp->array[idx];
				if (dlist_is_detached(&desc->list)) {
					cbuf_desc_wr_lock(desc);
					cbuf_desc_free(desc);
				}
			}
	    }
	}
} cbuf_desc_chunk_ptr_t;
/*
 * Thread private golbals
 */
thread_local cbuf_desc_chunk_ptr_t chunk_root = {NULL};
thread_local dlist_head_t cbuf_desc_free_list = DLIST_HEAD_INIT(cbuf_desc_free_list);

/*
 * cbuf_desc functions
 */
// Add a new chunk of cbuf desc
void cbuf_desc_chunk_add()
{
    cbuf_desc_chunk_t *chunk = (cbuf_desc_chunk_t *)malloc(sizeof(cbuf_desc_chunk_t));
    assert(chunk);
    // add chunk to chunk list
    chunk->next = chunk_root.chunk;
    chunk_root.chunk  = chunk;
    // Add all cbuf_desc to the free list
    for(uint32_t idx = 0; idx < sizeof(chunk->array)/sizeof(cbuf_desc_t); idx++) {
        cbuf_desc_t * desc = &chunk->array[idx];
        desc->rwlock = 0;
        desc->hdr   = NULL;
		desc->cbuf	= &cbuf;
        dlist_init(&desc->list);
        dlist_insert(&desc->list, &cbuf_desc_free_list); 
    }
    //printf("cbuf_desc_chunk_add free desc = %d\n", dlist_count(&cbuf_desc_free_list));
}

static inline void cbuf_desc_rd_lock(cbuf_desc_t *desc)
{
	int old;
	do {
		while ((old = desc->rwlock) < 0) ; /* wait for writer */
	} while (!__sync_bool_compare_and_swap (&desc->rwlock, old, old + 1));
} 

static inline void cbuf_desc_rd_unlock(cbuf_desc_t *desc)
{
	int old;
	do {
		old = desc->rwlock;
		assert (old > 0);
	} while (!__sync_bool_compare_and_swap (&desc->rwlock, old, old - 1));
} 

static inline void cbuf_desc_wr_lock(cbuf_desc_t *desc)
{
	do {
		/* Wait for readers and writers
		 * In this case, since we are the only writer, waiting for readers,
		 * Readers simply does a memory copy, so let's wait.
		 */
		while (desc->rwlock != 0) assert(desc->rwlock >= 0);
	} while (!__sync_bool_compare_and_swap (&desc->rwlock, 0, -1));
}

static inline void cbuf_desc_wr_unlock(cbuf_desc_t *desc)
{
	assert (desc->rwlock == -1);
	while (!__sync_bool_compare_and_swap (&desc->rwlock, -1, 0));
}

// Get a cbuf_desc from the free list.
// Replenish free list, if empty.
cbuf_desc_t * cbuf_desc_alloc()
{
    while (dlist_is_empty(&cbuf_desc_free_list)) {
        cbuf_desc_chunk_add();
    }
    dlist_t * list = dlist_get_head(&cbuf_desc_free_list);
    assert(list);
    return dlist_get_struct(cbuf_desc_t, list, list); 
}

// Caller must be holding write lock of the rwlock
static inline void cbuf_desc_free(cbuf_desc_t *desc)
{
    desc->hdr = NULL;
	desc->len = 0;
	desc->key[0] = 0;
	assert(desc->rwlock == -1);
    cbuf_desc_wr_unlock(desc);
    dlist_append(&desc->list, &cbuf_desc_free_list);
}

// Validate the descriptor is referencing to a valid record
// Caller should hold desc->rwlock
inline bool cbuf_desc_validate(cbuf_desc_t *desc)
{
    assert(desc);
    if (desc->hdr &&
        (desc->hdr->desc == desc) &&
        (desc->hdr->sig == CBUF_ENTRY_SIG) &&
		(desc->cbuf->sig == CBUF_ENTRY_SIG) &&
		(desc->len == desc->hdr->len)) {
		return true;
	}
    return false;
}

/*
 * cbuf Functions
 */
inline uint64_t cbuf_freespace()
{
	return (cbuf.tail >= cbuf.head)?
		cbuf.size - (cbuf.tail - cbuf.head) :
		cbuf.head - cbuf.tail;
}

inline uint64_t cbuf_size() { return cbuf.size; }

inline void cbuf_resize(uint32_t new_size)
{
	if (cbuf.bgn)
		free(cbuf.bgn);
	cbuf.size = new_size;
	cbuf.bgn = cbuf.head = cbuf.tail = (char *)malloc(cbuf.size);
	assert(cbuf.bgn);
	cbuf.end  = cbuf.bgn + cbuf.size;
}

// Remove record at head (to make space)
void cbuf_pop()
{
    // Wrap head if it is at the end
	if (cbuf.head == cbuf.end)
		cbuf.head = cbuf.bgn;

    cbuf_entry_hdr_t * hdr = (cbuf_entry_hdr_t*)cbuf.head;
    assert(hdr->sig == CBUF_ENTRY_SIG);

    cbuf_desc_t *desc = hdr->desc; assert(desc->hdr == hdr);

    cbuf_desc_wr_lock(desc);

	cbuf.head += cbuf_record_size(hdr->len);
	if (cbuf.head > cbuf.end) {
		cbuf.head = cbuf.bgn + (cbuf.head - cbuf.end);
		// printf("\thead wrapped\n");
	}
    
    cbuf_desc_free(desc);
	// printf("\tremoved a recod of %d, free sp = %lu\n", hdr->len, cbuf_freespace());
}

// Put datagram <data,len> into cbuf and  return the cbuf_desc of the data. 
// When cbuf is full, it will over-written old data records.
// Note: the cbuf is thread private, no locking is required.
cbuf_desc_t * cbuf_put(char *key, char *data, uint32_t len)
{
	uint32_t record_size = cbuf_record_size(len);

	// Initialize cbuf, if not already.
	if (!cbuf.bgn)
		cbuf_resize(CBUF_BUFFER_SIZE);

	// printf("b4 put %d, record_size = %d free sp = %lu\n", len, record_size, cbuf_freespace());

	if (record_size >= cbuf_size()) {
		// printf("Fatal: cbuf too small to hold data\n");
		return NULL;
	}

    // Make room for this put
	while (cbuf_freespace() <= record_size) {
		cbuf_pop(); // remove old record to make space
	}

    // Wrap tail, if it is at the end
	if (cbuf.tail == cbuf.end)
		cbuf.tail = cbuf.bgn;

    // Setup cbuf_desc and cbuf_entry_hdr
    cbuf_desc_t *desc = cbuf_desc_alloc();
    cbuf_entry_hdr_t hdr = {desc, len, CBUF_ENTRY_SIG};

	*(cbuf_entry_hdr *)cbuf.tail = hdr;
    desc->hdr                    = (cbuf_entry_hdr_t *)cbuf.tail; // ref to cbuf entry
	desc->len					 = len;
	strcpy(desc->key, key);

	char *payload = cbuf.tail + sizeof(cbuf_entry_hdr_t); assert(payload <= cbuf.end);

	// copy data into cbuf
	uint32_t tail_space = cbuf.end - payload;
	uint32_t done_size = (tail_space > len)? len : tail_space;

	memcpy(payload, data, done_size);

    if (done_size == len) {
		cbuf.tail += record_size;	// store record sie
    } else {
	    memcpy(cbuf.bgn, data + done_size, len - done_size);
		cbuf.tail = cbuf.bgn + align16(len - done_size);
		// printf("\ttail wrapped\n");
    }

	return desc;
}

// Read data to buf. Caller should hold cbuf_desc_rd_lock(desc)
uint32_t cbuf_read(cbuf_desc_t *desc, char *buf, uint32_t len)
{
    uint32_t ret = 0;
    // cbuf_desc_rdlock(desc);
    if (cbuf_desc_validate(desc)) {
        char *payload = (char *)&desc->hdr[1];
        uint32_t tail_space = desc->cbuf->end - payload;
        uint32_t todo_size = (desc->len > len)? len : desc->len;
        uint32_t done_size = (tail_space > todo_size)? todo_size : tail_space;
        
        memcpy(buf, payload, done_size);

        if (todo_size > done_size) {
            memcpy(buf + done_size, desc->cbuf->bgn, todo_size - done_size);
        }
        ret = todo_size;
    }
    // cbuf_desc_unlock(desc);
    return ret;
}
