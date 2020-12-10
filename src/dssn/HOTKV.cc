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
  HOTKV::put(const std::string &key, const std::string &value, const DSSNMeta &meta)
  {
      bool result = false;

      KeyValue* kv = new KeyValue;
      if (kv) {
	  kv->isTombStone = false;
	  kv->meta = std::move(meta);
	  kv->key = std::move(key);
	  kv->value = std::move(value);
 
	  idx::contenthelpers::OptionalValue<DSSN::KeyValue*> ret = ((HotKeyValueType *)kvStore)->upsert(kv);
	  if (ret.mIsValid == true && ret.mValue != kv) {
	      KeyValue* oldkv = ret.mValue;
	      delete oldkv;
	      assert(ret.mValue);
	  }
	  result = true;
      }
      return result;
  }

  const std::string*
  HOTKV::get(const std::string &searchKey, DSSNMeta &meta) const
  {

      HotKeyValueType::KeyType key;
      key = searchKey.c_str();
      idx::contenthelpers::OptionalValue<KeyValue*> ret = ((HotKeyValueType *)kvStore)->lookup(key);
      if (ret.mIsValid) {
	  KeyValue* kv = ret.mValue;
	  if (!kv->isTombStone) {
	      kv->lock();
	      meta = kv->meta;
	      kv->unlock();
	      return &kv->value;
	  }
      }
      return NULL;
  }
  
  bool
  HOTKV::getMeta(const std::string &searchKey, DSSNMeta &meta)
  {
      HotKeyValueType::KeyType key;
      key = searchKey.c_str();
      bool result = false;
      idx::contenthelpers::OptionalValue<KeyValue*> ret = ((HotKeyValueType *)kvStore)->lookup(key);
      if (ret.mIsValid) {
    	  KeyValue* kv = ret.mValue;
    	  /*if (!kv->isTombStone) {
    		  kv->lock();
    		  meta = kv->meta;
    		  kv->unlock();
    		  result = true;
    	  }*/ // by Henry
		  meta = kv->meta;
		  result = true;
      }
      return result;
  }

  bool
  HOTKV::updateMeta(const std::string &searchKey, const DSSNMeta &meta)
  {
    return updateMetaThreadSafe(searchKey, meta);
  }

  bool
  HOTKV::updateMetaThreadSafe(const std::string &searchKey, const DSSNMeta &meta)
  {
      bool result = false;
      HotKeyValueType::KeyType key;
      key = searchKey.c_str();
      idx::contenthelpers::OptionalValue<KeyValue*> ret = ((HotKeyValueType *)kvStore)->lookup(key);
      if (ret.mIsValid) {
	  KeyValue* kv = ret.mValue;
	  if (!kv->isTombStone) {
	      //Acquire lock
	      kv->lock();
	      kv->meta = meta;
	      result = true;
	      kv->unlock();
	  }
      }
      return result;
  }

  void
  HOTKV::removeVersion(const std::string &searchKey, const DSSNMeta &meta)
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
      DSSNMeta meta;;
      removeVersion(searchKey, meta);
  }
}
