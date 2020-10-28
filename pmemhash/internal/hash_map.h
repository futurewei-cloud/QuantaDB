#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include <cstddef>
#include <stdint.h>
#include <immintrin.h>
#include <functional>
#include <atomic>
#include <vector>
#include <assert.h>

#define DEFAULT_BUCKET_COUNT 1024*1024
#define BUCKET_SIZE 32
#define VICTIM_LIST_SIZE (BUCKET_SIZE)

//#define PMEMHASH_STAT

#ifdef PMEMHASH_STAT
    #define LOOKUP_CNT_INCR() { lookup_ctr_++; }
    #define CLT_BELEM_SRCH_CNT_INCR(delta) {culminated_search_ctr_ += delta; }
    #warning  "PMEMHASH_STAT turned on. This will greatly reduce pmemhash benchmark result."
#else
    #define LOOKUP_CNT_INCR() do {} while(0)
    #define CLT_BELEM_SRCH_CNT_INCR(delta) do {} while(0)
#endif

struct bucket_header
{
    uint32_t valid_;        // valid vector, max 32 entries
    uint32_t victim_idx_;   // index into victim_list to find next victim.
};

union bucket_hdr64
{
    struct bucket_header hdr;
    uint64_t hdr64;
};

template <typename Elem>
struct alignas(32) hash_bucket
{
    union signatures {
        uint8_t sig8_[BUCKET_SIZE];
        __m256i sig256_;
    } sig_;
    struct bucket_header hdr_;
    Elem* ptr_[BUCKET_SIZE];
};

template <typename Elem>
class elem_pointer
{
public:
    uint32_t bucket_;
    uint8_t slot_;
    Elem* ptr_;

    elem_pointer() { bucket_ = 0; slot_ = 0; ptr_ = NULL; }

    elem_pointer(uint32_t b, uint8_t s, Elem* p) { 
        bucket_ = b; slot_ = s; ptr_ = p;
    }
};

template <typename Elem, typename K, typename V, typename Hash>
class hash_table
{
public:
    hash_table(uint32_t bucket_count=DEFAULT_BUCKET_COUNT, bool lossy_mode = true)
    {
        buckets_ = new hash_bucket<Elem>[bucket_count];
        victim_.resize(VICTIM_LIST_SIZE);
        victim_ = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
            29, 30, 31, 0};
        bucket_count_ = bucket_count;
		for (uint32_t idx = 0; idx < bucket_count; idx++) {
			buckets_[idx].hdr_.valid_ = 0;
		}
        lossy_mode_ = lossy_mode;
        evict_ctr_ = insert_ctr_ = update_ctr_ = 0;
	    culminated_search_ctr_ = lookup_ctr_ = 0;
    }

    ~hash_table()
    {
        if (buckets_)
            delete buckets_;
    }

    elem_pointer<Elem> get(const K & key) {
        return find_or_prepare_insert(key);
    }

    elem_pointer<Elem> put(const K & key, Elem *ptr) {
        elem_pointer<Elem> l_hint;

        do {
            l_hint = find_or_prepare_insert(key);
            if (l_hint.ptr_ == NULL) {
                l_hint = insert_internal(key, ptr, l_hint);
                break;
            }
        } while (update_internal(key, ptr, l_hint) == false);

        return l_hint;
    }

    uint8_t* sig(int bucket) {
        return buckets_[bucket].sig_.sig8_;
    }

    struct bucket_header hdr(int bucket) {
        return buckets_[bucket].hdr_;
    }

    // Returns true if the insert succeeds (false if an element with the 
    // specific key is already present) Only one operation should succeed
    // if multiple threads are inserting the same key at the same time.
    bool insert(const K &key, Elem *ptr) { return true; }

    // Return false if there is no value stored at the specified key,
    // otherwise this function atomically update the stored value to new
    bool update(const K &key, Elem *ptr) { return true; }

    // update the current value, if one is present, also return false
    // Otherwise, the element is inserted as a new element, return true.
    bool insert_or_update(const K &key, Elem *ptr) { return true; }

    bool update_internal(const K & key, Elem *ptr, elem_pointer<Elem> hint) {

        // find the bucket.
        auto bucket = bucketize(key);
        //std::vector<int> & l_victim_list = victim_; // or use at()

        if (hint.ptr_ != NULL && // valid hint, replace old ptr if neccessary
            ((buckets_[bucket].hdr_.valid_ & (1 << hint.slot_)) != 0) ) { // this slot is still valid.
            if (buckets_[bucket].ptr_[hint.slot_] == hint.ptr_) {
                buckets_[bucket].ptr_[hint.slot_] = ptr;
                #ifndef  PMEMHASH_STAT
                update_ctr_++;
                #endif  // PMEMHASH_STAT
                return true;
            }
        }

        return false;
    }

    elem_pointer<Elem> insert_internal(const K & key, Elem *ptr, elem_pointer<Elem> hint) {
        bool successful;
        #ifndef PMEMHASH_STAT
        bool evict;
        #endif
        uint8_t l_slot, l_victim_slot;

        // find the bucket.
        uint32_t bucket = bucketize(key);
        struct bucket_header * hdr_ptr = &(buckets_[bucket].hdr_);
        std::vector<int> & l_victim_list = victim_; // or use at()

        elem_pointer<Elem> ret = {bucket, 0, NULL};

        do {
            union bucket_hdr64 l_hdr;
            l_hdr.hdr = *hdr_ptr;

            auto l_victim_idx = hdr_ptr->victim_idx_;
            auto l_valid = hdr_ptr->valid_;

            if (!lossy_mode_ && bucket_is_full(l_valid)) {
                return ret;
            }

            // find an empty slot, if successfully found, its signature should be set to INVALID by
            // previous victimization step.
            l_slot = find_empty(l_valid);

            // pick the next victim.
            l_victim_slot = l_victim_list[l_victim_idx];

            #ifndef PMEMHASH_STAT
            evict = (l_valid & (1ULL << l_victim_slot)) != 0;
            #endif

            union bucket_hdr64 l_new_hdr;
            l_new_hdr.hdr.victim_idx_ = (l_victim_idx +1) % VICTIM_LIST_SIZE;
            l_new_hdr.hdr.valid_ = ((l_valid | (1ULL << l_slot))     // set my slot
                                & ~(1ULL << l_victim_slot));            // and clear victim's

            successful = __sync_bool_compare_and_swap((uint64_t*)hdr_ptr, (uint64_t)l_hdr.hdr64, (uint64_t)l_new_hdr.hdr64);
        } while (!successful);

        #ifndef  PMEMHASH_STAT
        insert_ctr_++;
        if (evict)
            evict_ctr_++;
        #endif

        buckets_[bucket].ptr_[l_slot] = ptr; //new index
        buckets_[bucket].sig_.sig8_[l_slot] = signature(key);
        ret.slot_ = l_slot;
        ret.ptr_ = ptr;

        return ret;
    }

    elem_pointer<Elem> find_or_prepare_insert(const K& key) {
        uint8_t l_slot;
        auto bucket = bucketize(key);
        struct hash_bucket<Elem>& l_bucket = buckets_[bucket];

        uint8_t l_sig = signature(key);
        __m256i l_sig256 = _mm256_setr_epi8(
                l_sig, l_sig, l_sig, l_sig, l_sig, l_sig, l_sig, l_sig,
                l_sig, l_sig, l_sig, l_sig, l_sig, l_sig, l_sig, l_sig,
                l_sig, l_sig, l_sig, l_sig, l_sig, l_sig, l_sig, l_sig,
                l_sig, l_sig, l_sig, l_sig, l_sig, l_sig, l_sig, l_sig);
        //uint32_t sig_matching_bits = _mm256_cmpeq_epi8_mask(l_bucket.sig_.sig256, l_sig256);
        __m256i l_cmpeq_ret = _mm256_cmpeq_epi8(l_bucket.sig_.sig256_, l_sig256);
        uint32_t sig_matching_bits = _mm256_movemask_epi8(l_cmpeq_ret);
        uint32_t valid_matching_sig = sig_matching_bits & l_bucket.hdr_.valid_;
	    uint32_t search_cnt = 0;
	    LOOKUP_CNT_INCR ();

        do {
	        search_cnt++;
            l_slot = __builtin_ffs(valid_matching_sig);
            if (l_slot == 0) break;
            Elem *l_ptr = l_bucket.ptr_[l_slot-1];

            //FIXME: make this getKey to be in a KeyExtractor
            if (l_ptr->getKey() == key) {
                    CLT_BELEM_SRCH_CNT_INCR (search_cnt);
                return elem_pointer<Elem>(bucket, l_slot-1, l_ptr);
            }
            valid_matching_sig &= ~(1ULL << (l_slot-1));
        } while (l_slot < BUCKET_SIZE);

        CLT_BELEM_SRCH_CNT_INCR (search_cnt);
        return elem_pointer<Elem>(0, 0, NULL);
    }

    uint32_t get_evict_count() { return evict_ctr_; }
    uint32_t get_insert_count() { return insert_ctr_; }
    uint32_t get_update_count() { return update_ctr_; }
    uint64_t get_lookup_count() { return lookup_ctr_; }
    uint32_t get_avg_elem_iter_len() {
        if (lookup_ctr_) {
	        uint64_t c_search_count = culminated_search_ctr_;
	        uint64_t search_count = lookup_ctr_;
	        return c_search_count/search_count;
	    }
	    return 0;
    }

    uint8_t signature(const K & key) { return (Hash{}(key) / bucket_count_) & 0xFF; }
private:
    int bucketize(const K & key) { return Hash{}(key) % bucket_count_; }
    int find_empty(uint32_t valid) { return __builtin_ffs(~valid) - 1; }
    inline bool bucket_is_full(uint32_t valid_mask)
    {
        uint32_t n_avail = 0;
        uint32_t avail = ~valid_mask;
        assert(avail != 0);
        while (avail) {
            if (++n_avail > 1)
                return false;
            avail &= avail - 1;
        }
        assert(n_avail == 1);
        return true;
    }

    // Variables
    uint32_t bucket_count_;
    hash_bucket<Elem> *buckets_;
    std::vector<int> victim_;
    bool lossy_mode_;
    std::atomic<uint32_t> evict_ctr_;
    std::atomic<uint32_t> insert_ctr_;
    std::atomic<uint32_t> update_ctr_;
    std::atomic<uint64_t> lookup_ctr_;
    std::atomic<uint64_t> culminated_search_ctr_;
};


#endif //HASH_TABLE_H
