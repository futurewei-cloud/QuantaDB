// Copyright (c) 2020 Futurewei Technologies Inc

#include "HOTKV.h"
#include <hot/rowex/HOTRowex.hpp>

namespace DSSN
{
  using HotKeyValueType = hot::rowex::HOTRowex<DSSN::KeyValue*, KeyExtractor>;
  HOTKV::HOTKV() {
      enableVersionChain = false;
      kvStore = new HotKeyValueType();
      assert(kvStore);
  }
  
  HOTKV::HOTKV(bool versionChain) {
      enableVersionChain = versionChain;
      kvStore = new HotKeyValueType();
      assert(kvStore);
  }

  bool
  HOTKV::put(const std::string &key, const std::string &value, const uint64_t meta)
  {
      bool result = false;

      KeyValue* kv = new KeyValue;
      if (kv) {
	  kv->isTombStone = false;
	  kv->meta = meta;
	  kv->key = std::move(key);
	  kv->value = std::move(value);
	  result = ((HotKeyValueType *)kvStore)->insert(kv);
      }
      return result;
  }

  const std::string&
  HOTKV::get(const std::string &searchKey, uint64_t* meta)
  {

      HotKeyValueType::KeyType key;
      key = searchKey.c_str();
      idx::contenthelpers::OptionalValue<KeyValue*> ret = ((HotKeyValueType *)kvStore)->lookup(key);
      if (ret.mIsValid) {
	  KeyValue* kv = ret.mValue;
	  if (!kv->isTombStone) {
	      *meta = kv->meta;
	      return kv->value;
	  }
      }

      return NULL;
  }
  
  void
  HOTKV::removeVersion(const std::string &searchKey, const uint64_t meta)
  {
      HotKeyValueType::KeyType key;
      key = searchKey.c_str();

      if (!enableVersionChain) {
	  idx::contenthelpers::OptionalValue<KeyValue*> ret = ((HotKeyValueType *)kvStore)->lookup(key);
	  if (ret.mIsValid) {
	      KeyValue* kv = ret.mValue;
	      kv->isTombStone = true;
	  }
      }
  }

  //TODO: implement the remove all versions
  void
  HOTKV::remove(const std::string &searchKey)
  {
      uint64_t meta = 0xFFFFFFFF;
      removeVersion(searchKey, meta);
  }
}
