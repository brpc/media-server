// Media Server - Host or proxy live multimedia streams.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Ge,Jun (jge666@gmail.com)
//          Jiashun Zhu(zhujiashun2010@gmail.com)

#ifndef MEDIA_SERVER_SHARED_MAP_H
#define MEDIA_SERVER_SHARED_MAP_H

#include "butil/containers/flat_map.h"
#include "butil/scoped_lock.h"
#include "butil/intrusive_ptr.hpp"

// A map for sharing values between threads.
// The value type must be manageable by butil::intrusive_ptr<>, for example,
// the value type can inherit from brpc::SharedObject.
template <typename K, typename V,
          typename Hasher = butil::DefaultHasher<K>,
          typename EqualTo = butil::DefaultEqualTo<K> >
class SharedMap {
public:
    typedef butil::FlatMap<K, butil::intrusive_ptr<V>, Hasher, EqualTo> map_type;
    typedef typename map_type::key_type key_type;
    typedef typename map_type::mapped_type mapped_type;
    typedef typename map_type::value_type value_type;
    typedef typename map_type::iterator unlocked_iterator;
    typedef typename map_type::const_iterator const_unlocked_iterator;

    SharedMap(size_t initial_nbucket = 32);
    ~SharedMap();

    // Put the value associated with `key' into `value_out'.
    // Returns true on found.
    template <typename K2>
    bool get(const K2& key, butil::intrusive_ptr<V>* value_out);

    // Put the value associated with `key' into `value_out'. If the key does
    // not exist, constructing a new value by calling `new V(key)' and associate
    // it with the key and set *did_new to true if did_new is not NULL.
    // Returns true iff the key does not exist before.
    bool get_or_new(const K& key, butil::intrusive_ptr<V>* value_out);

    // Put value associated with `key' into this map. If the key exists, the
    // value is replaced.
    // Returns true iff the key does not exist before.
    bool put(const K& key, butil::intrusive_ptr<V> & value);
 
    // Put value associated with `key` into this map. If the key exists, the
    // value is hold.
    // Returns true iff the key does not exist before.
    bool put_or_hold(const K& key, butil::intrusive_ptr<V> & value);

    // Remove the value associated with `key'.
    // Returns 1 on found, 0 otherwise.
    template <typename K2>
    size_t remove(const K2& key);

    // Remove all values whose keys make `filter(key)' true and put the values
    // into `removed'.
    // Returns number of removed values.
    template <typename Filter>
    size_t remove_by(Filter & filter,
                     std::vector<std::pair<K, butil::intrusive_ptr<V> > >* removed);

    // Remove all entries.
    void clear();

    // Number of keys in this map.
    size_t size();

    // Copy all values into *values.
    void copy_to(std::vector<butil::intrusive_ptr<V> >* values);

    // To iterate this map when only one thread is using it.
    unlocked_iterator unlocked_begin() { return _map.begin(); }
    unlocked_iterator unlocked_end() { return _map.end(); }
    const_unlocked_iterator unlocked_begin() const { return _map.begin(); }
    const_unlocked_iterator unlocked_end() const { return _map.end(); }

private:
    pthread_mutex_t _mutex;
    size_t _initial_nbucket;
    map_type _map;
};

template <typename K, typename V, typename H, typename E>
SharedMap<K, V, H, E>::SharedMap(size_t initial_nbucket)
    : _initial_nbucket(initial_nbucket) {
    pthread_mutex_init(&_mutex, NULL);
}

template <typename K, typename V, typename H, typename E>
SharedMap<K, V, H, E>::~SharedMap() {
    pthread_mutex_destroy(&_mutex);
}

template <typename K, typename V, typename H, typename E>
template <typename K2>
bool SharedMap<K, V, H, E>::get(const K2& key, butil::intrusive_ptr<V>* value_out) {
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    butil::intrusive_ptr<V>* ptr = _map.seek(key);
    if (ptr) {
        *value_out = *ptr;
        return true;
    }
    return false;
}

template <typename K, typename V, typename H, typename E>
bool SharedMap<K, V, H, E>::put(const K& key, butil::intrusive_ptr<V> & value) {
    butil::intrusive_ptr<V> replaced_val;
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    if (!_map.initialized()) {
        CHECK_EQ(0, _map.init(_initial_nbucket));
    }
    butil::intrusive_ptr<V> & old_val = _map[key];
    if (old_val != NULL) {
        replaced_val.swap(old_val);
        old_val = value;
        return false;
    } else {
        old_val = value;
        return true;
    }
}

template <typename K, typename V, typename H, typename E>
bool SharedMap<K, V, H, E>::put_or_hold(const K& key, butil::intrusive_ptr<V> & value) {
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    if (!_map.initialized()) {
        CHECK_EQ(0, _map.init(_initial_nbucket));
    }
    butil::intrusive_ptr<V> & old_val = _map[key];
    if (old_val != NULL) {
        return false;
    } else {
        old_val = value;
        return true;
    }
}

template <typename K, typename V, typename H, typename E>
bool SharedMap<K, V, H, E>::get_or_new(
    const K& key, butil::intrusive_ptr<V>* value_out) {
    {
        std::unique_lock<pthread_mutex_t> mu(_mutex);
        butil::intrusive_ptr<V>* ptr = _map.seek(key);
        if (ptr) {
            *value_out = *ptr;
            return false;
        }
    }
    // optimistic creation to reduce critical section.
    butil::intrusive_ptr<V> new_val(new V(key));
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    if (!_map.initialized()) {
        CHECK_EQ(0, _map.init(_initial_nbucket));
    }
    butil::intrusive_ptr<V>& old_val = _map[key];
    if (old_val == NULL) {
        old_val = new_val;
        mu.unlock();
        *value_out = new_val;
        return true;
    } else {
        *value_out = old_val;
        return false;
        // new_val will be deleted.
    }
}

template <typename K, typename V, typename H, typename E>
template <typename K2>
size_t SharedMap<K, V, H, E>::remove(const K2& key) {
    butil::intrusive_ptr<V> val;
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    butil::intrusive_ptr<V>* ptr = _map.seek(key);
    if (ptr) {
        ptr->swap(val); // make the value deallocated outside lock.
        _map.erase(key);
        mu.unlock();
        return 1;
    }
    return 0;
}

template <typename K, typename V, typename H, typename E>
template <typename Filter>
size_t SharedMap<K, V, H, E>::remove_by(
    Filter & filter,
    std::vector<std::pair<K, butil::intrusive_ptr<V> > >* removed) {
    size_t c = 0;
    removed->clear();
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    for (typename map_type::iterator it = _map.begin(); it != _map.end(); ++it) {
        if (filter(*it->second)) {
            removed->push_back(std::make_pair(it->first, it->second));
            ++c;
        }
    }
    for (size_t j = 0; j < removed->size(); ++j) {
        _map.erase((*removed)[j].first);
    }
    return c;
}

template <typename K, typename V, typename H, typename E>
void SharedMap<K, V, H, E>::copy_to(std::vector<butil::intrusive_ptr<V> >* values) {
    values->clear();
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    if (values->capacity() < _map.size()) {
        const size_t sz = _map.size();
        mu.unlock();
        values->reserve(sz);
        mu.lock();
    }
    for (typename map_type::iterator it = _map.begin(); it != _map.end(); ++it) {
        values->push_back(it->second);
    }
}

template <typename K, typename V, typename H, typename E>
void SharedMap<K, V, H, E>::clear() {
    map_type empty_map;
    {
        std::unique_lock<pthread_mutex_t> mu(_mutex);
        empty_map.swap(_map);
    }
}

template <typename K, typename V, typename H, typename E>
size_t SharedMap<K, V, H, E>::size() {
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    return _map.size();
}

#endif // MEDIA_SERVER_SHARED_MAP_H
