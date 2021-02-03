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

#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <mutex>

namespace QDB {
/*
 * User mode Slab Allocator
 * API
 * slab_t * slab_create()
 * void *   slab_alloc(slab_ptr_t)
 * void     slab_free(void *)
 * void     slab_destroy(slab_ptr_t)
 */
#define ROUNDUP(x,align) (((x/align) + (x % align ? 1UL : 0UL))*align)
#define ROUNDDOWN(x, align) ((x/align)*align)
#define SIZEOF8(x) ROUNDUP(x,8)

template<uint32_t MAGAZINE_CAPACITY = 1024*1024>
class Slab {
  private:
    typedef struct objhdr {
        #define SLAB_OBJ_SIG    0x5BB5C44C
        uint64_t        sig;
        struct objhdr * next;
    } objhdr_t;

    typedef struct magazine {
        #define SLAB_MAG_SIG    0x6D7C8E9F
        uint64_t        sig;
        struct  magazine * next;
        objhdr_t * objptr;
    } magazine_t;

  public:
    Slab(uint32_t objsize) {
        objsz = objsize;
        bullet_size = sizeof(objhdr_t) + SIZEOF8(objsz);
        add_magazine();
    }

    ~Slab() {
        while (head) {
            magazine_t * mag = head;
            assert(mag->sig == SLAB_MAG_SIG);
            head = head->next;
            free(mag);
        }
    };

    void * get()
    {
        objhdr_t *obj;
        do {
            while (!(obj = objfree))
                add_magazine();
        } while (!__sync_bool_compare_and_swap(&objfree, obj, obj->next));
        return &obj[1];
    }

    void put(void *ptr)
    {
        objhdr_t *obj = &((objhdr_t*)ptr)[-1];
        assert(obj->sig == SLAB_OBJ_SIG);
        do {
            obj->next = objfree;
        } while (!__sync_bool_compare_and_swap(&objfree, obj->next, obj));
    }

    uint32_t count_free()
    {
        objhdr_t *obj = objfree;
        uint32_t cnt = 0;
        while (obj) {
            cnt++;
            obj = obj->next;
        }
        return cnt;
    }

  private:
    void add_magazine()
    {
        magazine_t *mag = (magazine_t *)malloc(sizeof(magazine_t) + bullet_size * MAGAZINE_CAPACITY);
        mag->sig    = SLAB_MAG_SIG;
        mag->objptr = (objhdr_t *)&mag[1]; 

        mtx.lock();
        mag->next = head;
        head = mag;
        mtx.unlock();

        objhdr_t *obj = mag->objptr;
        for (uint32_t idx = 0; idx < MAGAZINE_CAPACITY; idx++) {
            obj->sig = SLAB_OBJ_SIG;
            put(&obj[1]);
            obj = (objhdr_t*)((uint64_t)obj + bullet_size);
        }
    }

    objhdr_t * objfree = NULL;
    magazine_t * head = NULL;
    uint32_t objsz; 
    uint32_t bullet_size;
    std::mutex mtx;
};
} // End QDB namespace
