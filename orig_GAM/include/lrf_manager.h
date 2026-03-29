#ifndef INCLUDE_LRF_MANAGER_H_
#define INCLUDE_LRF_MANAGER_H_

// Define this to enable RDMA operations statistics
// #define LRF_ENABLE_RDMA_STATS

#include "structure.h"
#include "lrf_cache.h"
#include "gallocator.h"
#include "worker_handle.h"
#include <vector>
#include <cstdarg>

class Worker;
class Client;

class lrf_manager {
private:
  WorkerHandle* wh;  //handle to communicate with local worker
  CacheLRF cache_lrf; // LRF cache
  Worker* getWorker();

public:

  int tem1;
  int tem2;

#ifdef LRF_ENABLE_RDMA_STATS
  // Statistics for RDMA operations
  uint64_t sync_cas_count;
  uint64_t sync_cas_latency;
  uint64_t sync_write_count;
  uint64_t sync_write_latency;
  uint64_t sync_read_count;
  uint64_t sync_read_latency;
  uint64_t sync_write_cas_count;
  uint64_t sync_write_cas_latency;
  uint64_t sync_cas_read_count;
  uint64_t sync_cas_read_latency;
#endif


  lrf_manager(Worker* worker);
  ~lrf_manager();

  inline int GetID() {
    return wh->GetWorkerId();
  }

  inline bool IsLocal(GAddr addr) {
    return WID(addr) == GetID();
  }

  inline void* GetLocal(GAddr addr) {
    if (!IsLocal(addr)) {
      return nullptr;
    } else {
      return wh->GetLocal(addr);
    }
  }

  GAddr MutexMalloc();
  void MutexFree(GAddr addr);
  int MutexLock(GAddr lockgaddr, vector<GAddr> addrs = {}, vector<size_t> sizes = {});
  int MutexUnlock(GAddr lockgaddr);
  int Read(GAddr addr, void* buf, size_t size);
  int Write(GAddr addr, const void* buf, size_t size, bool rules = false);
  bool LocalCas(GAddr addr, uint64_t expected, uint64_t desired);

  void* GetWriteCache(GAddr addr, size_t size);
  void* GetReadCache(GAddr addr, size_t size);

#ifdef LRF_ENABLE_RDMA_STATS
  void ResetStatistics();
  void PrintStatistics();
#endif

};

#endif /* INCLUDE_LRF_MANAGER_H_ */
