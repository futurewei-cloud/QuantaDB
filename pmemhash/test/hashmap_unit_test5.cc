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

/*
 * Test hash_table with non-lossy mode
 */

using namespace std;

class Element
{
public:
    uint64_t key;
    uint64_t value;

    Element(uint64_t k = 0, uint64_t v = 0) { key=k; value=v; }
    inline uint64_t getKey() { return key; }
};

struct simple_hash {
	size_t operator()(uint64_t key)
	{
		return key >> 8;
	}
};

hash_table<Element, uint64_t, uint64_t, simple_hash> my_hashtable(DEFAULT_BUCKET_COUNT, false);

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
    Element elem[64];
    elem_pointer<Element> elem_ret;

    for (uint32_t idx = 0; idx < sizeof(elem)/sizeof(Element); idx++) {
        elem[idx].key = idx;
        elem[idx].value = idx;
        elem_ret = my_hashtable.put(elem[idx].key, &elem[idx]);
        /*
        print_header(elem_ret.bucket_);
        printf("put key:%ld bucket:%i slot:%i ptr:%p\n",
                elem[idx].key, elem_ret.bucket_, elem_ret.slot_, elem_ret.ptr_);
        */
        assert(idx <=30 || elem_ret.ptr_ == NULL);
    }

    printf("Non-lossy mode pmemhash test OK\n");
}
