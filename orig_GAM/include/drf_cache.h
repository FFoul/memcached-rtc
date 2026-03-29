#ifndef INCLUDE_DRF_CACHE_H_
#define INCLUDE_DRF_CACHE_H_

#include <unordered_map>
#include <memory>
#include <vector>
#include <unordered_set>
#include <functional>
#include <boost/thread/shared_mutex.hpp>
#include "structure.h"

// 前向声明
class CacheDRF;

// Dirty state enum for cache entries
enum class DirtyState {
    CLEAN = 0,      // 未修改
    DIRTY = 1,      // 整体修改
    ELE_DIRTY = 2   // 元素级修改（仅用于数组）
};

// 内存分配回调函数类型
using MemoryAllocCallback = std::function<void*(size_t)>;

// 内存释放回调函数类型
using MemoryFreeCallback = std::function<void(void*, size_t)>;

// 网络读取回调函数类型 
using NetworkReadCallback = std::function<int(void*, GAddr, size_t)>;

// Structure to track individual dirty elements
struct DirtyElement {
    void* data;          // 元素的数据副本
    size_t offset;       // 在object中的偏移
    size_t size;         // 元素大小
    
    DirtyElement() : data(nullptr), offset(0), size(0) {}
    DirtyElement(void* d, size_t o, size_t s) : data(d), offset(o), size(s) {}
};

class CacheEntryDRF {
public:
    void* data;
    size_t size;
    DirtyState dirty;  // Changed from bool to DirtyState
    std::unordered_set<uint64_t> thread_ids;
    
    // Dirty element list: maps offset to dirty element info
    std::unordered_map<size_t, DirtyElement> dirty_elements;
    
    CacheEntryDRF() : data(nullptr), size(0), dirty(DirtyState::CLEAN) {}
    ~CacheEntryDRF() = default;
};

class CacheDRF {
private:
    std::unordered_map<GAddr, std::shared_ptr<CacheEntryDRF>> cache_drf_map;
    std::unordered_map<size_t, std::vector<GAddr>> thread_map;
    mutable boost::shared_mutex rw_lock_;
    
    bool thread_has_internal(GAddr addr, size_t thread_id) const;

public:
    CacheDRF() = default;
    ~CacheDRF();

    void* insert_or_get(GAddr addr, size_t size, DirtyState dirty, uint64_t thread_id, 
                        MemoryAllocCallback alloc_cb, MemoryFreeCallback free_cb = nullptr,
                        NetworkReadCallback read_cb = nullptr);

    void* insert_or_get_for_ele(GAddr base, size_t offset, size_t object_size, size_t ele_size, DirtyState dirty, uint64_t thread_id, 
                              MemoryAllocCallback alloc_cb, MemoryFreeCallback free_cb,
                              NetworkReadCallback read_cb);

    std::shared_ptr<CacheEntryDRF> find(GAddr addr);

    void erase(GAddr addr);

    bool contains(GAddr addr);

    size_t size() const;

    bool thread_has(GAddr addr, size_t thread_id) const;

    void thread_insert(GAddr addr, size_t thread_id);
    
    void clear_thread_cache(size_t thread_id, 
                           std::vector<std::pair<GAddr, std::shared_ptr<CacheEntryDRF>>>& addrs_to_invalidate,
                           std::vector<std::pair<GAddr, std::shared_ptr<CacheEntryDRF>>>& dirty_entries);
    
    void get_all_entries(std::vector<std::pair<GAddr, std::shared_ptr<CacheEntryDRF>>>& all_entries);
    
    void clear_all();
};

#endif /* INCLUDE_DRF_CACHE_H_ */
