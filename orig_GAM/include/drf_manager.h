#ifndef INCLUDE_DRF_MANAGER_H_
#define INCLUDE_DRF_MANAGER_H_

#include "structure.h"
#include "drf_cache.h"
#include "worker_handle.h"

class Worker;
class Client;

class drf_manager {
private:
  WorkerHandle* wh;  //handle to communicate with local worker
  CacheDRF cache_drf; // DRF cache
  
  Worker* getWorker();

public:
  drf_manager(Worker* worker);
  ~drf_manager();

  void* GetReadCache(GAddr addr, size_t thread_id, size_t size = 0);

  void* GetWriteCache(GAddr addr, size_t thread_id, size_t size = 0, bool prefetch = true);

  void* GetReadEleCache(GAddr base, size_t offset, size_t thread_id, size_t elem_size);

  void* GetWriteEleCache(GAddr base, size_t offset, size_t thread_id, size_t elem_size);

  
  inline int GetID() {
    return wh->GetWorkerId();
  }

  inline bool IsLocal(GAddr addr) {
    return WID(addr) == GetID();
  }

  /*
   * return the local pointer of the global address addr
   * if not local, return nullptr
   */
  inline void* GetLocal(GAddr addr) {
    if (!IsLocal(addr)) {
      return nullptr;
    } else {
      return wh->GetLocal(addr);
    }
  }

  /**
   * @brief Invalidate a cache entry
   * @param addr Address to invalidate
   */
  void invalidate(GAddr addr);

  /**
   * @brief Clear cache entries for a specific thread
   * Removes thread from all accessed entries, writes back dirty entries if no other threads,
   * and invalidates entries that are no longer accessed by any thread.
   * @param thread_id Thread ID to clear cache for
   */
  void clear_cache(size_t thread_id);

  /**
   * @brief Get cache statistics
   */
  size_t get_cache_size() const;
};

#endif /* INCLUDE_DRF_MANAGER_H_ */
