// drf_cache.cc (copied from race_protocol)
#include "drf_cache.h"
#include <algorithm>
#include <iostream>

CacheDRF::~CacheDRF() {
    clear_all();
}

bool CacheDRF::thread_has_internal(GAddr addr, size_t thread_id) const {
    auto it = cache_drf_map.find(addr);
    if (it == cache_drf_map.end()) return false;
    return it->second->thread_ids.find(thread_id) != it->second->thread_ids.end();
}

void* CacheDRF::insert_or_get(GAddr addr, size_t size, DirtyState dirty, uint64_t thread_id, 
                              MemoryAllocCallback alloc_cb, MemoryFreeCallback free_cb,
                              NetworkReadCallback read_cb) {
    // Simplified placeholder implementation for compilation; full implementation lives in race_protocol
    std::lock_guard<boost::shared_mutex> lock(rw_lock_);
    auto it = cache_drf_map.find(addr);
    if (it != cache_drf_map.end()) {
        return it->second->data;
    }
    auto entry = std::make_shared<CacheEntryDRF>();
    entry->size = size;
    entry->dirty = dirty;
    if (alloc_cb) entry->data = alloc_cb(size);
    cache_drf_map[addr] = entry;
    return entry->data;
}

void* CacheDRF::insert_or_get_for_ele(GAddr base, size_t offset, size_t object_size, size_t ele_size, DirtyState dirty, uint64_t thread_id, 
                              MemoryAllocCallback alloc_cb, MemoryFreeCallback free_cb,
                              NetworkReadCallback read_cb) {
    // Simplified: allocate element buffer
    std::lock_guard<boost::shared_mutex> lock(rw_lock_);
    auto it = cache_drf_map.find(base);
    if (it == cache_drf_map.end()) {
        auto entry = std::make_shared<CacheEntryDRF>();
        entry->size = object_size;
        entry->dirty = dirty;
        if (alloc_cb) entry->data = alloc_cb(object_size);
        cache_drf_map[base] = entry;
        return (void*)((char*)entry->data + offset);
    }
    return (void*)((char*)it->second->data + offset);
}

std::shared_ptr<CacheEntryDRF> CacheDRF::find(GAddr addr) {
    boost::shared_lock<boost::shared_mutex> lock(rw_lock_);
    auto it = cache_drf_map.find(addr);
    if (it == cache_drf_map.end()) return nullptr;
    return it->second;
}

void CacheDRF::erase(GAddr addr) {
    std::lock_guard<boost::shared_mutex> lock(rw_lock_);
    cache_drf_map.erase(addr);
}

bool CacheDRF::contains(GAddr addr) {
    boost::shared_lock<boost::shared_mutex> lock(rw_lock_);
    return cache_drf_map.find(addr) != cache_drf_map.end();
}

size_t CacheDRF::size() const {
    boost::shared_lock<boost::shared_mutex> lock(rw_lock_);
    return cache_drf_map.size();
}

bool CacheDRF::thread_has(GAddr addr, size_t thread_id) const {
    return thread_has_internal(addr, thread_id);
}

void CacheDRF::thread_insert(GAddr addr, size_t thread_id) {
    std::lock_guard<boost::shared_mutex> lock(rw_lock_);
    auto it = cache_drf_map.find(addr);
    if (it != cache_drf_map.end()) {
        it->second->thread_ids.insert(thread_id);
    }
}

void CacheDRF::clear_thread_cache(size_t thread_id, 
                           std::vector<std::pair<GAddr, std::shared_ptr<CacheEntryDRF>>>& addrs_to_invalidate,
                           std::vector<std::pair<GAddr, std::shared_ptr<CacheEntryDRF>>>& dirty_entries) {
    // Simplified: move all entries into addrs_to_invalidate
    std::lock_guard<boost::shared_mutex> lock(rw_lock_);
    for (auto &p : cache_drf_map) {
        addrs_to_invalidate.push_back(p);
    }
    cache_drf_map.clear();
}

void CacheDRF::get_all_entries(std::vector<std::pair<GAddr, std::shared_ptr<CacheEntryDRF>>>& all_entries) {
    std::lock_guard<boost::shared_mutex> lock(rw_lock_);
    for (auto &p : cache_drf_map) all_entries.push_back(p);
}

void CacheDRF::clear_all() {
    std::lock_guard<boost::shared_mutex> lock(rw_lock_);
    cache_drf_map.clear();
}
