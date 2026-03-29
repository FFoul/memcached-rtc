#ifndef INCLUDE_LRF_CACHE_H_
#define INCLUDE_LRF_CACHE_H_

#include <unordered_map>
#include <memory>
#include <vector>
#include <unordered_set>
#include <functional>
#include "structure.h"


class CacheEntryLRF {
public:
    void* data;
    size_t size;
    bool dirty;
    
    CacheEntryLRF() : data(nullptr), size(0), dirty(false) {}
    ~CacheEntryLRF() = default;
};

class CacheLRF {
private:
    std::unordered_map<GAddr, CacheEntryLRF*> cache_lrf_map;

public:
    CacheLRF() = default;
    ~CacheLRF();

    void insert(GAddr addr, void* data, size_t size, bool dirty);

    CacheEntryLRF* find(GAddr addr);

    void erase(GAddr addr);

    void clear_cache(std::vector<std::pair<GAddr, CacheEntryLRF*>>& addrs_to_invalidate, 
                            std::vector<std::pair<GAddr, CacheEntryLRF*>>& dirty_entries);

};

#endif /* INCLUDE_LRF_CACHE_H_ */
