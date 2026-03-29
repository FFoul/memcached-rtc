// drf_manager.cc (adapted for orig_GAM build)
#include <cstring>
#include "drf_manager.h"
#include "worker.h"
#include "client.h"
#include "slabs.h"
#include "log.h"

Worker* drf_manager::getWorker() {
  return wh->GetWorker();
}

drf_manager::drf_manager(Worker* worker)
    : wh(new WorkerHandle(worker)) {
}
drf_manager::~drf_manager() {
  std::vector<std::pair<GAddr, std::shared_ptr<CacheEntryDRF>>> all_entries;
  cache_drf.get_all_entries(all_entries);
  Worker* worker = getWorker();
  for (auto& pair : all_entries) {
    GAddr addr = pair.first;
    auto& entry = pair.second;
    if (!entry || !entry->data) continue;
    worker->sb.fast_free(entry->data, entry->size);
    entry->data = nullptr;
  }
  cache_drf.clear_all();
  epicLog(LOG_INFO, "drf_manager destroyed: cleaned %zu cache entries", all_entries.size());
  delete wh;
}


void* drf_manager::GetReadCache(GAddr addr, size_t thread_id, size_t size) {
  size_t object_size = size;
  void* local_ptr = GetLocal(addr);
  if(local_ptr) return local_ptr;
  auto cache_entry = cache_drf.find(addr);
  if (cache_entry) {
    if(thread_id != (uint64_t)-1) {
        if(!cache_drf.thread_has(addr, thread_id)) {
            cache_drf.thread_insert(addr, thread_id);
        }
        cache_entry->thread_ids.insert(thread_id);
    }
    return cache_entry->data;
  }

  Client* cli = getWorker()->GetClient(addr);
  if(!cli) {
    epicPanic("Failed to get client for worker %d\n", WID(addr));
    return nullptr;
  }

  Worker* worker = getWorker();
  auto alloc_callback = [worker](size_t size) -> void* { return worker->sb.fast_alloc(size); };
  auto free_callback = [worker](void* ptr, size_t size) -> void { worker->sb.fast_free(ptr, size); };
  auto read_callback = [cli](void* data, GAddr addr, size_t size) -> int {
    ssize_t ret = cli->SyncRead((raddr)data, (raddr)cli->ToLocal(addr), size, 0);
    return (ret < 0) ? ret : 0;
  };
  void* cache_data = cache_drf.insert_or_get(addr, object_size, DirtyState::CLEAN, thread_id, alloc_callback, free_callback, read_callback);
  if (!cache_data) {
    epicLog(LOG_WARNING, "Failed to allocate or load cache data for addr %lx, size %zu", addr, object_size);
    return nullptr;
  }
  return cache_data;
}

void* drf_manager::GetWriteCache(GAddr addr, size_t thread_id, size_t size, bool prefetch) {
  size_t object_size = size;
  void* local_ptr = GetLocal(addr);
  if(local_ptr) return local_ptr;
  auto cache_entry = cache_drf.find(addr);
  if (cache_entry) {
    if(thread_id != (uint64_t)-1) {
        if(!cache_drf.thread_has(addr, thread_id)) {
            cache_drf.thread_insert(addr, thread_id);
        }
        cache_entry->thread_ids.insert(thread_id);
    }
    cache_entry->dirty = DirtyState::DIRTY;
    return cache_entry->data;
  }

  Client* cli = getWorker()->GetClient(addr);
  if(!cli) { epicPanic("Failed to get client for worker %d\n", WID(addr)); return nullptr; }
  Worker* worker = getWorker();
  auto alloc_callback = [worker](size_t size) -> void* { return worker->sb.fast_alloc(size); };
  auto free_callback = [worker](void* ptr, size_t size) -> void { worker->sb.fast_free(ptr, size); };
  auto read_callback = [cli](void* data, GAddr addr, size_t size) -> int {
    ssize_t ret = cli->SyncRead((raddr)data, (raddr)cli->ToLocal(addr), size, 0);
    return (ret < 0) ? ret : 0;
  };
  void* cache_data = nullptr;
  if(prefetch) {
    cache_data = cache_drf.insert_or_get(addr, object_size, DirtyState::DIRTY, thread_id, alloc_callback, free_callback, read_callback);
  } else {
    cache_data = cache_drf.insert_or_get(addr, object_size, DirtyState::DIRTY, thread_id, alloc_callback, free_callback, nullptr);
  }
  if (!cache_data) {
    epicLog(LOG_WARNING, "Failed to allocate cache memory of size %zu", object_size);
    return nullptr;
  }
  return cache_data;
}

void* drf_manager::GetReadEleCache(GAddr base, size_t offset, size_t thread_id, size_t elem_size) {
  void* local_ptr = GetLocal(base);
  if(local_ptr) return (void*)((char*)local_ptr + offset);
  auto cache_entry = cache_drf.find(base);
  if (cache_entry) {
    if(thread_id != (uint64_t)-1) {
        if(!cache_drf.thread_has(base, thread_id)) {
            cache_drf.thread_insert(base, thread_id);
        }
        cache_entry->thread_ids.insert(thread_id);
    }
    auto ele_it = cache_entry->dirty_elements.find(offset);
    if (ele_it != cache_entry->dirty_elements.end()) return ele_it->second.data;
    if (cache_entry->data != nullptr) return (void*)((char*)cache_entry->data + offset);
  }
  Worker* worker = getWorker();
  Client* cli = worker->GetClient(base);
  if(!cli) { epicPanic("Failed to get client for worker %d\n", WID(base)); return nullptr; }
  size_t object_size = get_gaddr_object_size(base);
  auto alloc_callback = [worker](size_t size) -> void* { return worker->sb.fast_alloc(size); };
  auto free_callback = [worker](void* ptr, size_t size) -> void { worker->sb.fast_free(ptr, size); };
  auto read_callback = [cli](void* data, GAddr addr, size_t size) -> int {
    ssize_t ret = cli->SyncRead((raddr)data, (raddr)cli->ToLocal(addr), size, 0);
    return (ret < 0) ? ret : 0;
  };
  void* cache_ele_data = cache_drf.insert_or_get_for_ele(base, offset, object_size, elem_size, DirtyState::CLEAN, thread_id, alloc_callback, free_callback, read_callback);
  if (!cache_ele_data) {
    epicLog(LOG_WARNING, "Failed to allocate or load cache element for base %lx offset %zu, size %zu", base, offset, elem_size);
    return nullptr;
  }
  return cache_ele_data;
}

void* drf_manager::GetWriteEleCache(GAddr base, size_t offset, size_t thread_id, size_t elem_size) {
  void* local_ptr = GetLocal(base);
  if(local_ptr) return (void*)((char*)local_ptr + offset);
  auto cache_entry = cache_drf.find(base);
  if (cache_entry) {
    if(thread_id != (uint64_t)-1) {
        if(!cache_drf.thread_has(base, thread_id)) {
            cache_drf.thread_insert(base, thread_id);
        }
        cache_entry->thread_ids.insert(thread_id);
    }
    if (cache_entry->dirty != DirtyState::DIRTY) cache_entry->dirty = DirtyState::ELE_DIRTY;
    auto ele_it = cache_entry->dirty_elements.find(offset);
    if (ele_it != cache_entry->dirty_elements.end()) return ele_it->second.data;
  }
  Worker* worker = getWorker();
  Client* cli = worker->GetClient(base);
  if(!cli) { epicPanic("Failed to get client for worker %d\n", WID(base)); return nullptr; }
  size_t object_size = get_gaddr_object_size(base);
  auto alloc_callback = [worker](size_t size) -> void* { return worker->sb.fast_alloc(size); };
  auto free_callback = [worker](void* ptr, size_t size) -> void { worker->sb.fast_free(ptr, size); };
  void* cache_ele_data = cache_drf.insert_or_get_for_ele(base, offset, object_size, elem_size, DirtyState::ELE_DIRTY, thread_id, alloc_callback, free_callback, nullptr);
  if (!cache_ele_data) {
    epicLog(LOG_WARNING, "Failed to allocate cache element for base %lx offset %zu, size %zu", base, offset, elem_size);
    return nullptr;
  }
  return cache_ele_data;
}

void drf_manager::invalidate(GAddr addr) {
  auto cache_entry = cache_drf.find(addr);
  epicLog(LOG_FATAL, "Invalidating cache entry for addr %lx", addr);
  if (cache_entry && cache_entry->data) {
    Worker* worker = getWorker();
    epicLog(LOG_FATAL, "Freeing cache entry for addr %lx", addr);
    worker->sb.fast_free(cache_entry->data, cache_entry->size);
  }
  epicLog(LOG_FATAL, "Erasing cache entry for addr %lx", addr);
  cache_drf.erase(addr);
  epicLog(LOG_FATAL, "Cache entry for addr %lx invalidated", addr);
}

void drf_manager::clear_cache(size_t thread_id) {
  std::vector<std::pair<GAddr, std::shared_ptr<CacheEntryDRF>>> addrs_to_invalidate;
  std::vector<std::pair<GAddr, std::shared_ptr<CacheEntryDRF>>> dirty_entries;
  epicLog(LOG_DEBUG, "Clearing cache for thread %zu", thread_id);
  cache_drf.clear_thread_cache(thread_id, addrs_to_invalidate, dirty_entries);
  Worker* worker = getWorker();
  epicLog(LOG_DEBUG, "Writing back %zu dirty entries for thread %zu", dirty_entries.size(), thread_id);
  std::unordered_set<Client*> clients_used;
  for (auto& pair : dirty_entries) {
    GAddr addr = pair.first;
    auto& entry = pair.second;
    Client* cli = worker->GetClient(addr);
    if (!cli) { epicLog(LOG_WARNING, "Failed to get client for write-back of addr %lx", addr); continue; }
    clients_used.insert(cli);
    if (entry->dirty == DirtyState::DIRTY) {
      if (entry->data && entry->size > 0) {
        cli->Write((raddr)cli->ToLocal(addr), (raddr)entry->data, entry->size, true);
        epicLog(LOG_DEBUG, "Async write back full object for addr %lx, size %zu", addr, entry->size);
      }
    } else if (entry->dirty == DirtyState::ELE_DIRTY) {
      for (auto& ele_pair : entry->dirty_elements) {
        const DirtyElement& dirty_ele = ele_pair.second;
        GAddr ele_addr = addr + dirty_ele.offset;
        cli->Write((raddr)cli->ToLocal(ele_addr), (raddr)dirty_ele.data, dirty_ele.size, true);
        epicLog(LOG_DEBUG, "Async write back dirty element for addr %lx offset %zu, size %zu", addr, dirty_ele.offset, dirty_ele.size);
      }
    }
  }
  std::vector<char*> sync_flags;
  if (!clients_used.empty()) {
    for (auto& cli : clients_used) {
      char* buff = cli->RdmaSync_async();
      if (buff) sync_flags.push_back(buff);
    }
  }
  for (auto flag_buffer : sync_flags) {
    while (*flag_buffer == 0) { }
    worker->sb.fast_free(flag_buffer, 1);
  }
  for (auto& pair : addrs_to_invalidate) {
    GAddr addr = pair.first;
    auto& entry = pair.second;
    for (auto& ele_pair : entry->dirty_elements) {
      const DirtyElement& dirty_ele = ele_pair.second;
      if (entry->data == nullptr || dirty_ele.data < entry->data || dirty_ele.data >= (void*)((char*)entry->data + entry->size)) {
        worker->sb.fast_free(dirty_ele.data, dirty_ele.size);
      }
    }
    if (entry->data) {
      worker->sb.fast_free(entry->data, entry->size);
    }
    epicLog(LOG_DEBUG, "Freed memory for cache entry addr %lx (thread %zu)", addr, thread_id);
  }
  epicLog(LOG_INFO, "Cleared cache for thread %zu: %zu entries invalidated, %zu dirty entries written back", thread_id, addrs_to_invalidate.size(), dirty_entries.size());
}

size_t drf_manager::get_cache_size() const { return cache_drf.size(); }
