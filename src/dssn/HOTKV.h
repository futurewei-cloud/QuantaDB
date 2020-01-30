// Copyright (c) 2020 Futurewei Technologies Inc

#ifndef __HOTKV_H__
#define __HOTKV_H__

#include "KVInterface.h"

namespace DSSN
{
/**
 * This class implements the KVInterface class using the Height Optimized
 * Trie data structure.
 */
    class HOTKV : public KVInterface {
    public:
        HOTKV();
        HOTKV(bool versionChain);
        bool put(const std::string &key, const std::string &value, const dssnMeta& meta);
	bool updateMeta(const std::string &searchKey, const dssnMeta& meta);
	const std::string* get(const std::string &searchKey, dssnMeta& meta) const;
	bool getMeta(const std::string &searchKey, dssnMeta &meta);
	void removeVersion(const std::string &searchKey, const dssnMeta& meta);
	// Remove all versions belonging to the key 
	void remove(const std::string &searchKey);
    private:
	bool updateMetaThreadSafe(const std::string &searchKey, const dssnMeta& meta);
	// Pointer to the underlying HOT data structure
        void* kvStore;
	// Indicates if multiversion is enabled
	bool enableVersionChain;
    };
}

#endif /* __HOTKV_H__ */
