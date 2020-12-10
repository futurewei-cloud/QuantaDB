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


// Copyright 2019 PelagoDB developers.
// Copyright 2018 The Abseil Authors.
//
// Design:
// The hash table groups entries into buckets, each bucket contains three parts;
//   [bitmap of entries, the signatures of entries, and the entrys themselves]
//
// For example, with an N-way (N=16) bucket, the layout looks like
//
//   [Bucket i]:
//   +----------------+-------------------------------------------------+
//   |0100100010001101| Sig15 | Sig14 | Sig13 | ... | ... | Sig1 | Sig0 |
//   +----------------+----------------+----------------+---------------+
//   |     Slot15     |     Slot14     |     Slot13     |     ...       |
//   +----------------+----------------+----------------+---------------+
//   |      ...       |      ...       |      ...       |     ...       |
//   +----------------+----------------+----------------+---------------+
//   |      ...       |      ...       |      Slot1     |     Slot0     |
//   +----------------+----------------+----------------+---------------+
//
// Each entry can be persistent/volatile pointer to a <K,V> pair, or
// just the <K,V> pair itself.
//
// Lookup will take two hash functions, the main hash function will locate
// the corresponding bucket, and the sig hash function will generate a
// signature that will match up to N signatures, filtered by the Valid bitmap,
// resulting most likely one single hit, if hit at all.
//
// Insert has two access versions, multi-writer and single-writer. 
// The single-writer version will quadratically probe other buckets, and use
// store instruction instead of compare_and_swap to update the Valid bitmap.
//
// The multi-writer version will close-address the only bucket, and it uses
// compare_and_swap to update the Valid bitmap. 
//
// *CAUTION* Multi-writer version demands lockless allocator to be effective.
//
// Insert has two store modes, lossy and lossless. Both mode will locate the
// bucket using main hash function. an empty slot is located and its 
// corresponding Signature is overwritten with PENDING signature, then the 
// valid bitmap is compare_and_swap'ed to secure that slot. 
//
// *PENDING Signature* is a signature that's ILLEGAL for sig hash.
//
// In lossy mode, we always evict one entry to make room for the next
// insertion when performing compare_and_swap; In lossless mode, the insertion
// will fail when the bucket is full.
//
// With empty slot secured, the Entry is updated prior to the installation of
// correct signature.
//
// Some of the implementation is copied from Abseil-cpp's raw_hash_set, like
// hash_policy, node_handle and node_handle<mapped_type>

#ifndef PELAGODB_INTERNAL_RAW_HASH_SET_H
#define PELAGODB_INTERNAL_RAW_HASH_SET_H

namespace pelagodb {


template <class Entry, class Hash, class Eq, class Alloc>
class raw_hash_set;

// The node_handle concept from C++17.
// We specialize node_handle for sets and maps. node_handle_base holds the
// common API of both.
template <typename Entry, typename Alloc>
class node_handle_base {
    protected:
        using EntryTraits = hash_entry_traits<Entry>;
        using slot_type = typename EntryTraits::slot_type;

    public:
        using allocator_type = Alloc;

        constexpr node_handle_base() {}
        node_handle_base(node_handle_base&& other) noexcept {
            *this = std::move(other);
        }
        ~node_handle_base() { destroy(); }
        node_handle_base& operator=(node_handle_base&& other) {
            destroy();
            if (!other.empty()) {
                alloc_ = other.alloc_;
                EntryTraits::transfer(alloc(), slot(), other.slot());
                other.reset();
            }
            return *this;
        }

        bool empty() const noexcept { return !alloc_; }
        explicit operator bool() const noexcept { return !empty(); }
        allocator_type get_allocator() const { return *alloc_; }

    protected:
        template <typename, typename, typename, typename>
            friend class raw_hash_set;

        node_handle_base(const allocator_type& a, slot_type* s) : alloc_(a) {
            EntryTraits::transfer(alloc(), slot(), s);
        }

        void destroy() {
            if (!empty()) {
                EntryTraits::destroy(alloc(), slot());
                reset();
            }
        }

        void reset() {
            assert(alloc_.has_value());
            alloc_ = absl::nullopt;
        }

        slot_type* slot() const {
            assert(!empty());
            return reinterpret_cast<slot_type*>(std::addressof(slot_space_));
        }
        allocator_type* alloc() { return std::addressof(*alloc_); }

    private:
        absl::optional<allocator_type> alloc_;
        mutable absl::aligned_storage_t<sizeof(slot_type), alignof(slot_type)>
            slot_space_;
};

// For sets.
template <typename Entry, typename Alloc, typename = void>
class node_handle : public node_handle_base<Entry, Alloc> {
    using Base = typename node_handle::node_handle_base;

    public:
    using value_type = typename Base::EntryTraits::value_type;

    constexpr node_handle() {}

    value_type& value() const {
        return Base::EntryTraits::element(this->slot());
    }

    private:
    template <typename, typename, typename, typename>
        friend class raw_hash_set;

    node_handle(const Alloc& a, typename Base::slot_type* s) : Base(a, s) {}
};

// For maps.
template <typename Entry, typename Alloc>
class node_handle<Entry, Alloc, pelagodb::void_t<typename Entry::mapped_type>>
    : public node_handle_base<Entry, Alloc> {
    using Base = typename node_handle::node_handle_base;

        public:
    using key_type = typename Entry::key_type;
    using mapped_type = typename Entry::mapped_type;

    constexpr node_handle() {}

    auto key() const -> decltype(Base::EntryTraits::key(this->slot())) {
        return Base::EntryTraits::key(this->slot());
    }

    mapped_type& mapped() const {
        return Base::EntryTraits::value(
                &Base::EntryTraits::element(this->slot()));
    }

        private:
    template <typename, typename, typename, typename>
        friend class raw_hash_set;

    node_handle(const Alloc& a, typename Base::slot_type* s) : Base(a, s) {}
    };

// Implement the insert_return_type<> concept of C++17.
template <class Iterator, class NodeType>
struct insert_return_type {
    Iterator position;
    bool inserted;
    NodeType node;
};

} //namespace pelagodb

#endif // PELAGODB_INTERNAL_RAW_HASH_SET_H
