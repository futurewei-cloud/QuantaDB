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
#include <string.h>
#include "hash_map.h"
#include "c_str_util_classes.h"

/*
 * Test hash_table with 'char *' key type
 */

using namespace std;

class Element
{
public:
    char key[32];
    string value;

    Element(char * k, std::string v) { strcpy(key, k); value=v; }
    inline char * getKey() { return key; }
};

hash_table<Element, char *, std::string, hash_c_str> my_hashtable;

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
    Element elem_1((char *)"2049", "101");
    Element elem_2((char *)"2049", "102");
    Element elem_3((char *)"2049", "103");
    Element elem_4((char *)"1", "104");
    elem_pointer<Element> elem_ret;

    elem_ret = my_hashtable.put(elem_1.key, &elem_1);
    printf("put key:%s bucket:%i slot:%i value:%s\n", elem_1.key, elem_ret.bucket_, elem_ret.slot_, elem_1.value.c_str());
    print_header(elem_ret.bucket_);
    elem_ret = my_hashtable.put(elem_2.key, &elem_2);
    printf("put key:%s bucket:%i slot:%i value:%s\n", elem_1.key, elem_ret.bucket_, elem_ret.slot_, elem_2.value.c_str());
    print_header(elem_ret.bucket_);
    elem_ret = my_hashtable.put(elem_3.key, &elem_3);
    printf("put key:%s bucket:%i slot:%i value:%s\n", elem_1.key, elem_ret.bucket_, elem_ret.slot_, elem_3.value.c_str());
    print_header(elem_ret.bucket_);
    
    elem_ret = my_hashtable.put(elem_4.key, &elem_4);
    printf("put key:%s bucket:%i slot:%i value:%s\n", elem_4.key, elem_ret.bucket_, elem_ret.slot_, elem_4.value.c_str());
    print_header(elem_ret.bucket_);

    char *key = (char *)"2049";
    elem_ret = my_hashtable.get(key);
    if (elem_ret.ptr_!=NULL) {
        printf("get key:%s bucket:%i slot:%i key:%s, value:%s\n", key, elem_ret.bucket_, elem_ret.slot_,
            elem_ret.ptr_->key, elem_ret.ptr_->value.c_str());
	} else {
        printf("get key:%s failed\n", key);
    }

    key = (char *)"1";
    elem_ret = my_hashtable.get(key);
    if (elem_ret.ptr_!=NULL) {
        printf("get key:%s bucket:%i slot:%i key:%s, value:%s\n", key, elem_ret.bucket_, elem_ret.slot_,
            elem_ret.ptr_->key, elem_ret.ptr_->value.c_str());
    } else {
        printf("get key:%s failed\n", key);
	}

#if (0)
    printf("========== Test Phase 2 ==========\n");
    for (int i = 1; i < 65536; i+=4096) {
        Element *elem = new Element(to_string(i), to_string(i));
        elem_ret = my_hashtable.put(elem->key, elem);
        uint8_t sig = my_hashtable.signature(elem->key);
        printf("put key:%s sig:%i bucket:%i slot:%i value:%s\n", elem->key.c_str(), sig, elem_ret.bucket_, elem_ret.slot_, elem->value.c_str());

        print_header(elem_ret.bucket_);
        for (int j = 1; j <= i; j+=4096) {
            string key = to_string(j);
            elem_ret = my_hashtable.get(key);
            sig = my_hashtable.signature(key);
            if (elem_ret.ptr_!=NULL)
                printf("get key:%s sig:%i bucket:%i slot:%i key:%s, value:%s\n", key.c_str(), sig, elem_ret.bucket_, elem_ret.slot_,
                    elem_ret.ptr_->key.c_str(), elem_ret.ptr_->value.c_str());
            else
                printf("get key:%s sig:%i failed\n", key.c_str(), sig);
        }
    }

    for (int i = 1; i < 65536; i+=4096) {
        string key = to_string(i);
        elem_ret = my_hashtable.get(key);
        if (elem_ret.ptr_!=NULL)
            printf("get key:%s bucket:%i slot:%i key:%s, value:%s\n", key.c_str(), elem_ret.bucket_, elem_ret.slot_,
                elem_ret.ptr_->key.c_str(), elem_ret.ptr_->value.c_str());
        else
            printf("get key:%s failed\n", key.c_str());
    }
#endif
}
