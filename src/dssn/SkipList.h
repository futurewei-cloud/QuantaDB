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

namespace DSSN {

/*
 * Skip Node Declaration
 */
struct SkipNode
{
    uint64_t key;
    void * value;
    SkipNode *forw[MAX_LEVEL+1];

    SkipNode(uint64_t k, void *v) : key(k), value(v)
    {
        bzero(forw, sizeof(forw));
    }
};

/*
 * Skip List Declaration
 */
class SkipList {
  public:
    SkipNode *head;
    int level;
    SkipList(float p = 0.5) : level(0)
    {
        probability = p;
        head = new SkipNode(0, (void *)const_cast<char *>("head"));
        ctr = 0;
        lock = 0;
    }

    ~SkipList() 
    {
        // delete all nodes
        SkipNode* tmp;
        SkipNode* nxt;
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

        SkipNode* node = head;
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

    inline bool contains(uint64_t key) { return find(key) != NULL; }

    /*
     * Insert Element in Skip List
     */
    void insert(uint64_t key, void *value) 
    {
        spin_lock();
        SkipNode *x = head;	
        SkipNode *update[MAX_LEVEL + 1];
        memset(update, 0, sizeof(SkipNode*) * (MAX_LEVEL + 1));

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
            x = new SkipNode(key, value);
            ctr++;

            for (int i = 0;i <= lvl;i++) 
            {
                x->forw[i] = update[i]->forw[i];
                update[i]->forw[i] = x;
            }
        } else {
            x->value = value;
        }
        spin_unlock();
    }

    /*
     * Delete Element from Skip List
     */
    void remove(uint64_t key) 
    {
        spin_lock();
        SkipNode *x = head;	
        SkipNode *update[MAX_LEVEL + 1];
        memset (update, 0, sizeof(SkipNode*) * (MAX_LEVEL + 1));

        for (int i = level; i >= 0; i--)
        {
            while (x->forw[i] != NULL && x->forw[i]->key < key)
            {
                x = x->forw[i];
            }
            update[i] = x; 
        }

        x = x->forw[0];

        if (x->key == key) 
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
        spin_unlock();
    }

    inline void * get() { return (head->forw[0])?  head->forw[0]->value : NULL; }
    
    inline void * get(uint64_t key)
    {
        SkipNode * n = find(key);
        return (n)? const_cast<void *>(n->value) : NULL;
    }

    inline void * pop()
    {
        void * val = NULL;
        if (head->forw[0]) { 
            val = head->forw[0]->value;
            remove(head->forw[0]->key);
        }
        return val;
    }

    inline void * try_pop(uint64_t key)
    {
        void * val = NULL;
        if (head->forw[0] && (key <= head->forw[0]->key)) {
            val = head->forw[0]->value;
            remove(head->forw[0]->key);
        }
        return val;
    }

    inline uint64_t firstkey()  { return (head->forw[0])?  head->forw[0]->key : head->key; }

    inline uint64_t lastkey()
    {
        SkipNode *tmp = head;
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
    SkipNode * find(uint64_t key) 
    {
        SkipNode *x = head;

        for (int i = level; i >= 0; i--)
        {
            while (x->forw[i] != NULL && x->forw[i]->key < key)
            {
                x = x->forw[i];
            }
        }

        x = x->forw[0];
        return (x != NULL && x->key == key)? x : NULL;
    }

	int volatile lock;	// CAS spin lock
    inline void spin_lock() { while (!__sync_bool_compare_and_swap (&lock, 0, 1)); }
    inline void spin_unlock() { assert(lock == 1); while (!__sync_bool_compare_and_swap (&lock, 1, 0)); }
};

} // DSSN
