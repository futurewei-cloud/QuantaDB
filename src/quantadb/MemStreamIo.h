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

#pragma once

#include <string.h>
#include <assert.h>
#include <vector>

namespace QDB {

class inMemStream {
    public:
    inMemStream (uint8_t* buf, size_t sz)
    {
        bgn = buf;
        end  = buf + sz;
        cur = bgn;
    }

    inline void read( void* p, size_t sz)
    {
        assert(cur + sz <= end);
        memcpy(p, cur, sz);
        cur += sz;
    }

    inline void toString( std::string* s)
    {
        size_t sz;
        read(&sz, sizeof(sz));
        assert(cur + sz <= end);
        new( s ) std::string(reinterpret_cast<const char*>(cur), sz );
        cur += sz;
    }

    template<class T>
    inline void toVector(std::vector<T>* v)
    {
        size_t sz;
        read(&sz, sizeof(sz));
        new ( v ) std::vector<T>;
        for( size_t idx = 0; idx < sz; idx++ ) {
            v->push_back( T(this) );
        }
    }

    inline size_t dsize(void)
    {
        return (size_t)(end - cur);
    }

    private:
    uint8_t* bgn;
    uint8_t* cur;
    uint8_t* end;
};

class outMemStream {
    public:
    outMemStream (uint8_t* buf, size_t sz)
    {
        bgn = buf;
        end  = buf + sz;
        cur = bgn;
    }

    inline void write(const void* p, size_t sz)
    {
        assert(cur + sz <= end);
        memcpy(cur, p, sz);
        cur += sz;
    }

    inline void write(const std::string& s)
    {
        size_t l = s.length();
        write(&l, sizeof(l));
        write(s.c_str(), l);
    }

    template<class T>
    inline void writeVector(const std::vector<T>& v)
    {
        size_t sz = v.size();
        write(&sz, sizeof(sz));
        for (auto it : v)
            it.serialize(this);
    }

    inline size_t dsize(void)
    {
        return (size_t)(cur - bgn);
    }

    inline size_t free_space(void)
    {
        return (size_t)(end - cur);
    }

    private:
    uint8_t *cur,
            *bgn,
            *end;
};

}
