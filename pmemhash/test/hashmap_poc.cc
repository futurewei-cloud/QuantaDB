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

#include <stdio.h>
#include "hash_map.h"

class Element
{
public:
    uint64_t key;
    uint64_t value;

    Element(uint64_t k, uint64_t v) { key=k; value=v; }
    Element() { key=0; value=0; }

    inline uint64_t getKey() { return key; }
};

hash_table<Element, uint64_t, uint64_t, std::hash<uint64_t>> my_hashtable;

void print_header(int bucket)
{
    uint8_t *l_sig8 = my_hashtable.sig(bucket);
    struct bucket_header l_hdr = my_hashtable.hdr(bucket);

    printf("\tvalid:%08x ", l_hdr.valid_);
    for (int i=0; i<BUCKET_SIZE; i++) {
        printf("%x ", l_sig8[i]);
    }
    printf("\n");
}

int main(void)
{
    Element elem_1(2049, 101);
    Element elem_2(2049, 102);
    Element elem_3(2049, 103);
    Element elem_4(1, 104);
    elem_pointer<Element> elem_ret;

#if 0
    elem_ret = my_hashtable.put(elem_1.key, &elem_1);
    printf("put key:%lu bucket:%i slot:%i value:%li\n", elem_1.key, elem_ret.bucket_, elem_ret.slot_, elem_1.value);
    print_header(elem_ret.bucket_);
    elem_ret = my_hashtable.put(elem_1.key, &elem_2);
    printf("put key:%lu bucket:%i slot:%i value:%li\n", elem_1.key, elem_ret.bucket_, elem_ret.slot_, elem_2.value);
    print_header(elem_ret.bucket_);
    elem_ret = my_hashtable.put(elem_1.key, &elem_3);
    printf("put key:%lu bucket:%i slot:%i value:%li\n", elem_1.key, elem_ret.bucket_, elem_ret.slot_, elem_3.value);
    print_header(elem_ret.bucket_);
    

    elem_ret = my_hashtable.put(elem_4.key, &elem_4);
    printf("put key:%lu bucket:%i slot:%i value:%li\n", elem_4.key, elem_ret.bucket_, elem_ret.slot_, elem_4.value);
    print_header(elem_ret.bucket_);

    uint64_t key = 2049;
    elem_ret = my_hashtable.get(key);
    if (elem_ret.ptr_!=NULL)
        printf("get key:%lu bucket:%i slot:%i key:%lu, value:%lu\n", key, elem_ret.bucket_, elem_ret.slot_,
            elem_ret.ptr_->key, elem_ret.ptr_->value);

    key = 1;
    elem_ret = my_hashtable.get(key);
    if (elem_ret.ptr_!=NULL)
        printf("get key:%lu bucket:%i slot:%i key:%lu, value:%lu\n", key, elem_ret.bucket_, elem_ret.slot_,
            elem_ret.ptr_->key, elem_ret.ptr_->value);
    else 
        printf("get key:%lu failed\n", key);
#endif
    printf("========== Test Phase 2 ==========\n");
    #define ELEM_SIZE   65536
    Element elem_array[ELEM_SIZE];
    for (int i = 1; i < ELEM_SIZE; i+=4096) {
        Element *elem = &elem_array[i];
        elem->key = elem->value = i;
        elem_ret = my_hashtable.put(elem->key, elem);
        uint8_t sig = my_hashtable.signature(elem->key);
        printf("put key:%lu sig:%i bucket:%i slot:%i value:%li\n", elem->key, sig, elem_ret.bucket_, elem_ret.slot_, elem->value);
        print_header(elem_ret.bucket_);
 
        for (int j = 1; j <= i; j+=4096) {
            uint64_t key = j;
            elem_ret = my_hashtable.get(key);
            sig = my_hashtable.signature(key);
            if (elem_ret.ptr_!=NULL)
                printf("get key:%lu sig:%i bucket:%i slot:%i key:%lu, value:%lu\n", key, sig, elem_ret.bucket_, elem_ret.slot_,
                    elem_ret.ptr_->key, elem_ret.ptr_->value);
            else
                printf("get key:%lu sig:%i failed\n", key, sig);
        }
    }

    for (int i = 1; i < ELEM_SIZE; i+=4096) {
        uint64_t key = i;
        elem_ret = my_hashtable.get(key);
        if (elem_ret.ptr_!=NULL)
            printf("get key:%lu bucket:%i slot:%i key:%lu, value:%lu\n", key, elem_ret.bucket_, elem_ret.slot_,
                elem_ret.ptr_->key, elem_ret.ptr_->value);
        else
            printf("get key:%lu failed\n", key);
    }
}
