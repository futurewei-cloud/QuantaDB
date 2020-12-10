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
 * Double link list operations - from Mike Hsu.
 */
#pragma once

#include <assert.h>

#define	dlist_get_struct(body, member, memberaddr) ((body *)((char *)memberaddr - (char *)&((body *)0)->member))

struct dlist_head;

typedef struct dlist {
	struct dlist_head *head;
	struct dlist *prev /* tail */, *next /* head */;
} dlist_t;

typedef struct dlist_head {
	dlist_t				list;
	int 				count;
} dlist_head_t;
	
#define	dlist_is_detached(lst)	(((lst)->head == NULL) && \
								((((lst)->next == NULL ) && ((lst)->prev == NULL)) || \
								(((lst)->next == (lst)) && ( (lst)->prev == (lst)))))

#define	dlist_is_attached(lst)	(!dlist_is_detached(lst))

#define	dlist_is_empty(head)	((head)->count == 0)

#define	dlist_for_each(head, lst) for (lst = (head)->list.next; lst != &(head)->list; lst = lst->next)

#define DLIST_HEAD_INIT(head)   {{&head, &head.list, &head.list}, 0}

	
static inline int dlist_count(dlist_head_t *head) { return head->count; } // Return # of items in dlist, excluding the head
static inline void	dlist_init(dlist_t *lst)	{ lst->prev = lst->next = lst; lst->head = NULL; }

static inline void dlist_head_init(dlist_head_t *head)
{
	head->list.prev = head->list.next = &head->list; 
	head->list.head = head;
	head->count = 0;
}

// Add lst to the head (ie, next) of 'head' list
static inline void dlist_insert ( dlist_t *lst, dlist_head_t *head )
{
	assert(lst->head == NULL);
	lst->next		= head->list.next;
	lst->prev		= &head->list;
	head->list.next->prev = lst;
	head->list.next = lst;
	lst->head		= head;
	head->count++;
}

// Append lst to the tail (ie, prev) of 'head' list
static inline void	dlist_append(dlist_t *lst, dlist_head_t *head)
{
	assert(lst->head == NULL);
	lst->next = &head->list;
	lst->prev = head->list.prev;
	head->list.prev->next = lst;
	head->list.prev = lst;
	lst->head = head;
	head->count++;
}

static inline void dlist_drop(dlist_t *lst)
{
	lst->next->prev = lst->prev;
	lst->prev->next = lst->next;
	lst->next = lst->prev = lst;
	if (lst->head) {
		lst->head->count--;
		lst->head = NULL;
	}
}

static inline dlist_t * dlist_get_head(dlist_head_t *head)
{
	dlist_t *dp = NULL;
	if (dlist_count(head) > 0) {
		dp = head->list.next;
		assert(dp != &head->list);
		dlist_drop(dp);
	}
	return dp;
}

static inline dlist_t * dlist_get_tail(dlist_head_t *head)
{
	dlist_t *dp = NULL;
	if (dlist_count(head) > 0) {
		dp = head->list.prev;
		dlist_drop(dp);
	}
	return dp;
}
