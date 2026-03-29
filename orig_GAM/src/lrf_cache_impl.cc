#include "lrf_cache.h"

CacheLRF::~CacheLRF() {
    for (auto &p : cache_lrf_map) {
        delete p.second;
    }
    cache_lrf_map.clear();
}
