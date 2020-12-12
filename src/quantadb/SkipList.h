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
 * C++ Program to Implement Skip List 
 */
#pragma once

#include <iostream>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <assert.h>
#include <atomic>

#define MAX_LEVEL 16

using namespace std;

namespace QDB {

/*
 * Skip Node Declaration
 */
template <typename Key_t>
struct SkipNode
{
    Key_t key;
    void * value;
    SkipNode<Key_t> *forw[MAX_LEVEL+1];

    SkipNode(Key_t k, void *v) : key(k), value(v)
    {
        bzero(forw, sizeof(forw));
    }
};

/*
 * Skip List Declaration
 */
template <typename Key_t>
class SkipList {
  public:
    SkipNode<Key_t> *head;
    int level;
    SkipList(float p = 0.5) : level(0)
    {
        probability = p;
        head = new SkipNode<Key_t>(0, (void *)const_cast<char *>("head"));
        ctr = 0;
        lock_ = 0;
    }

    ~SkipList() 
    {
        // delete all nodes
        SkipNode<Key_t> * tmp;
        SkipNode<Key_t> * nxt;
        tmp = head;
        while ( tmp )
        {
            nxt = tmp->forw[0];
            delete tmp;
            tmp = nxt;
        }
    }

    /*
     * Display Elements of Skip List
     */
    void print() 
    {
        std::cout <<"=======\n";

        SkipNode<Key_t> * node = head;
        do {
            std::cout << "key: " << node->key << " val: " << (char *)node->value;
            for(int lvl = 0; lvl < MAX_LEVEL; lvl++) {
                if (!node->forw[lvl])
                    break;
                std::cout << " [" << lvl << "]" << "->key:" << node->forw[lvl]->key;
            }
            std::cout <<"\n";
        } while ((node = node->forw[0]) != nullptr);

        std::cout <<"=======\n";
    }

    inline bool contains(Key_t key)
    {
        lock();
        bool ret = find(key) != NULL;
        unlock();
        return ret;
    }

    /*
     * Insert Element to Skip List
     */
    bool insert(Key_t key, void *value)
    {
        lock();
        SkipNode<Key_t> *x = head;	
        SkipNode<Key_t> *update[MAX_LEVEL + 1];
        memset(update, 0, sizeof(SkipNode<Key_t>*) * (MAX_LEVEL + 1));

        for (int i = level; i >= 0; i--)
        {
            while (x->forw[i] != NULL && x->forw[i]->key < key) 
            {
                x = x->forw[i];
            }
            update[i] = x; 
        }

        x = x->forw[0];

        if (x == NULL || x->key != key) 
        {        
            int lvl = random_level();
            if (lvl > level) 
            {
                for (int i = level + 1;i <= lvl;i++) 
                {
                    update[i] = head;
                }
                level = lvl;
            }

            // x = new SkipNode(lvl, key, value);
            x = new SkipNode<Key_t>(key, value);
            if (x == NULL) {
                unlock();
                return false;
            }
            ctr++;

            for (int i = 0;i <= lvl;i++) 
            {
                x->forw[i] = update[i]->forw[i];
                update[i]->forw[i] = x;
            }
        } else {
            x->value = value;
        }
        unlock();
        return true;
    }

    /*
     * Delete Element from Skip List
     */
    void remove(Key_t key) 
    {
        lock();
        remove_internal(key);
        unlock();
    }

    inline void * get() { return (head->forw[0])?  head->forw[0]->value : NULL; }
    
    inline void * get(Key_t key)
    {
        lock();
        SkipNode<Key_t> * n = find(key);
        unlock();
        return (n)? n->value : NULL;
    }

    inline void * pop()
    {
        void * val = NULL;
        lock();
        if (head->forw[0]) { 
            val = head->forw[0]->value;
            remove_internal(head->forw[0]->key);
        }
        unlock();
        return val;
    }

    inline void * try_pop(Key_t key)
    {
        void * val = NULL;
        lock();
        if (head->forw[0] && (key >= head->forw[0]->key)) {
            val = head->forw[0]->value;
            remove_internal(head->forw[0]->key);
        }
        unlock();
        return val;
    }

    inline uint64_t firstkey()  { return (head->forw[0])?  head->forw[0]->key : head->key; }

    inline uint64_t lastkey()
    {
        SkipNode<Key_t> *tmp = head;
        while (tmp->forw[0]) tmp = tmp->forw[0];
        return tmp->key;
    }
    
    uint32_t maxLevel = MAX_LEVEL;
    float probability;
    atomic<uint32_t> ctr;

  private:

    inline int random_level()
    {
        int v = 1;

        while ((((double)std::rand() / RAND_MAX)) < probability && 
           std::abs(v) < MAX_LEVEL) {
            v += 1;
        }
        return std::abs(v);
        //return std::rand() % MAX_LEVEL;
    }

    /*
     * Search Elemets in Skip List
     */
    SkipNode<Key_t> * find(Key_t key) 
    {
        SkipNode<Key_t> *x = head, *ret;

        for (int i = level; i >= 0; i--)
        {
            while (x->forw[i] != NULL && x->forw[i]->key < key)
            {
                x = x->forw[i];
            }
        }

        x = x->forw[0];
        ret = (x != NULL && x->key == key)? x : NULL;
        return ret;
    }

    /*
     * Delete Element from Skip List
     */
    void remove_internal(Key_t key)
    {
        SkipNode<Key_t> *x = head;
        SkipNode<Key_t> *update[MAX_LEVEL + 1];
        memset (update, 0, sizeof(SkipNode<Key_t>*) * (MAX_LEVEL + 1));

        for (int i = level; i >= 0; i--)
        {
            while (x->forw[i] != NULL && x->forw[i]->key < key)
            {
                x = x->forw[i];
            }
            update[i] = x;
        }

        x = x->forw[0];

        if (x != NULL && x->key == key) 
        {
            for (int i = 0;i <= level;i++)
            {
                if (update[i]->forw[i] != x)
                    break;

                update[i]->forw[i] = x->forw[i];
            }

            delete x;
            ctr--;
            while (level > 0 && head->forw[level] == NULL)
            {
                level--;
            }
        }
    }

	int volatile lock_;	// CAS spin lock
    inline void lock() { while (!__sync_bool_compare_and_swap (&lock_, 0, 1)) sched_yield(); }
    inline void unlock() { assert(lock_ == 1); while (!__sync_bool_compare_and_swap (&lock_, 1, 0)); }
};

} // QDB
