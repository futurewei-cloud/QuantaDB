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

#include "TestUtil.h"
#include "hash_map.h"
#include "c_str_util_classes.h"
#include "Cycles.h"

#define	HASH_TABLE_TEMPLATE		Element, char *, uint64_t, hash_c_str
#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

class Element {
  public:
    Element()
    {
    }  

    ~Element()
    {
        // delete kv;
    }
    char *getKey() {return key;}
    void * v;
    char key[64];
  private:
};


class HashmapTest : public ::testing::Test {
  public:
  HashmapTest()
  {
	my_hashtable = new hash_table<HASH_TABLE_TEMPLATE>(1024);
    #define ELEM_SIZE   1024
    elems = new Element[ELEM_SIZE];

    for (int idx = 0; idx < ELEM_SIZE; idx++) {
        sprintf(elems[idx].key, "HashmapTest-key-%04d", idx);
        elems[idx].v = (void *)(uint64_t)idx;
    }
  };

  ~HashmapTest()
  {
    delete my_hashtable;
    delete elems;
  };

  DISALLOW_COPY_AND_ASSIGN(HashmapTest);

  hash_table<HASH_TABLE_TEMPLATE> * my_hashtable;
  Element *elems;
  uint32_t  bucket_count;
};

TEST_F(HashmapTest, bench) {
    GTEST_COUT << "HashmapTest put" << std::endl;
    int loop = 1024;
    uint64_t start, stop;
    //
    start = __rdtsc();
    for (int idx = 0; idx < ELEM_SIZE; idx++) {
        EXPECT_EQ(NULL, NULL);
    }
    stop = __rdtsc();
    uint32_t overhead = Cycles::toNanoseconds(stop - start)/loop;
    //
    start = __rdtsc();
    for (int idx = 0; idx < ELEM_SIZE; idx++) {
        elem_pointer<Element> lptr = my_hashtable->put(elems[idx].key, &elems[idx]);
        EXPECT_EQ(lptr.ptr_ , &elems[idx]);
        // GTEST_COUT << "key=" << elems[idx].key << "slot=" << lptr.slot_ << " bucket=" << lptr.bucket_ << std::endl;
    }
    stop = __rdtsc();
    uint32_t nsec_per = Cycles::toNanoseconds(stop - start)/loop - overhead;
    GTEST_COUT << "HashmapTest put: " << nsec_per << " nano sec per call " << std::endl;

    GTEST_COUT << "HashmapTest get" << std::endl;
    start = __rdtsc();
    for (int idx = 0; idx < ELEM_SIZE; idx++) {
        elem_pointer<Element> lptr = my_hashtable->get(elems[idx].key);
        EXPECT_EQ(lptr.ptr_ , &elems[idx]);
        // GTEST_COUT << "key=" << elems[idx].key << "slot=" << lptr.slot_ << " bucket=" << lptr.bucket_ << std::endl;
    }
    stop = __rdtsc();
    nsec_per = Cycles::toNanoseconds(stop - start)/loop - overhead;
    GTEST_COUT << "HashmapTest get: " << nsec_per << " nano sec per call " << std::endl;
}

}  // namespace RAMCloud
