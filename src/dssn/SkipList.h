/*
 * C++ Program to Implement Skip List 
 */

#include <iostream>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <assert.h>

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

    void print();
    inline bool contains(uint64_t);
    void insert(uint64_t, void *);
    void remove(uint64_t);        

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

    uint32_t maxLevel = MAX_LEVEL;
    float probability;

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

    SkipNode * find(uint64_t key); 

	int volatile lock;	// CAS spin lock
    inline void spin_lock() { while (!__sync_bool_compare_and_swap (&lock, 0, 1)); }
    inline void spin_unlock() { assert(lock == 1); while (!__sync_bool_compare_and_swap (&lock, 1, 0)); }
};

/*
 * Insert Element in Skip List
 */
void SkipList::insert(uint64_t key, void *value) 
{
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

        for (int i = 0;i <= lvl;i++) 
        {
            x->forw[i] = update[i]->forw[i];
            update[i]->forw[i] = x;
        }
    } else {
        x->value = value;
    }
}

/*
 * Delete Element from Skip List
 */
void SkipList::remove(uint64_t key) 
{
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
        while (level > 0 && head->forw[level] == NULL) 
        {
            level--;
        }
    }
}

/*
 * Display Elements of Skip List
 */
void SkipList::print() 
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

/*
 * Search Elemets in Skip List
 */
SkipNode * SkipList::find(uint64_t key) 
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

inline bool SkipList::contains(uint64_t key) 
{
    return find(key) != NULL;
}

} // DSSN
