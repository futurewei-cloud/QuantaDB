
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
