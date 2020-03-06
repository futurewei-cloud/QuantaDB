
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
