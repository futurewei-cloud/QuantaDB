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

#ifndef PELAGODB_INTERNAL_HASH_ENTRY_REFERENCE_H
#define PELAGODB_INTERNAL_HASH_ENTRY_REFERENCE_H

namespace pelagodb {

template <class Reference>
struct hash_entry_reference {
    static_assert(std::is_lvalue_reference<Reference>::value, "");

    using slot_type = typename std::remove_cv<
        typename std::remove_reference<Reference>::type::type*;

    template<class Alloc, class... Args>
    static void construct(Alloc *alloc, slot_type* slot, Args&&... args) {
        *slot = std::forward<Args>(args)...;
    }

    template<class Alloc>
    static void destroy(Alloc *alloc, slot_type* slot) {}

    template <class Alloc>
    static void transfer(Alloc *, slot_type* new_slot, slot_type* old_slot) {
        *new_slot = *old_slot;
    }

    static Reference element(slot_type* slot) {return **slot; };

    template <class T, class E = EntryType>
    static auto value(T* elem> -> decltype (P::value(elem)) {
        return P::value(elem);
    }

    template <class... Ts, class E = EntryType>
    static auto apply(Ts&&... ts) -> decltype(P::apply(std::forward<Ts>(ts)...)) {
        return P::apply(std::forward<Ts>(ts)...);
    }
};

} //namespace pelagodb

#endif // PELAGODB_INTERNAL_HASH_ENTRY_REFERENCE_H
