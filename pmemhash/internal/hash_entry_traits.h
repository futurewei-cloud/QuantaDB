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
//

#ifndef PELAGODB_INTERNAL_HASH_ENTRY_TRAITS_H
#define PELAGODB_INTERNAL_HASH_ENTRY_TRAITS_H

namespace pelagodb {

template <class EntryType, class = void>
struct hash_entry_traits {
    private:

    public:
        using slot_type = typename EntryType::slot_type;
        using key_type = typename EntryType::key_type;

    template <class Alloc, class... Args>
    static void construct(Alloc* alloc, slot_type* slot, Args&&... args) {
        EntryType::construct(alloc, slot, std::forward<Args>(args)...);
    }

    template <class Alloc>
    static void destroy(Alloc* alloc, slot_type* slot) {
        EntryType::destroy(alloc, slot);
    }

    template <class E = EntryType>
    static auto element(slot_type* slot) -> decltype(E::element(slot)) {
        return E::element(slot);
    }

    // used for node handle manipulation
    template <class E = EntryType>
    static auto key(slot_type *slot) -> decltype(E::key(slot)) {
        return E::key(slot);
    }

    template <class T, class E = EntryType>
    static auto value(T *elem) -> decltype(E::value(elem)) {
        return E::value(elem);
    }
};

}   //namespace pelagodb

#endif // PELAGODB_INTERNAL_HASH_ENTRY_TRAITS_H
