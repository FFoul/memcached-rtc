// lrf_cache.cc (simplified for build)
#include "lrf_cache.h"
#include <algorithm>

CacheLRF::~CacheLRF() {
    for (auto &p : cache_lrf_map) {
        delete p.second;
    }
    cache_lrf_map.clear();
}

void CacheLRF::insert(GAddr addr, void* data, size_t size, bool dirty) {
    CacheEntryLRF* entry = new CacheEntryLRF();
    entry->data = data;
    entry->size = size;
    entry->dirty = dirty;
    cache_lrf_map[addr] = entry;
}

CacheEntryLRF* CacheLRF::find(GAddr addr) {
    auto it = cache_lrf_map.find(addr);
    if (it == cache_lrf_map.end()) return nullptr;
    return it->second;
}

void CacheLRF::erase(GAddr addr) {
    auto it = cache_lrf_map.find(addr);
    if (it != cache_lrf_map.end()) {
        delete it->second;
        cache_lrf_map.erase(it);
    }
}

void CacheLRF::clear_cache(std::vector<std::pair<GAddr, CacheEntryLRF*>>& addrs_to_invalidate, 
                            std::vector<std::pair<GAddr, CacheEntryLRF*>>& dirty_entries) {
    for (auto &p : cache_lrf_map) {
        addrs_to_invalidate.push_back(p);
    }
    cache_lrf_map.clear();
}
