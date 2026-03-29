// Copyright (c) 2018 The GAM Authors 

#ifndef INCLUDE_ACCESS_TIMING_H_
#define INCLUDE_ACCESS_TIMING_H_

#include <atomic>
#include "structure.h"
#include "workrequest.h"
#include "lockwrapper.h"

// RDMA call statistics - protected by RdmaContext lock, no need for atomic
struct RdmaCallStats {
  unsigned long send_count;
  unsigned long write_count;
  unsigned long write_imm_count;
  unsigned long read_count;
  unsigned long cas_count;
  unsigned long total_count;
  
  RdmaCallStats() : send_count(0), write_count(0), write_imm_count(0), 
                    read_count(0), cas_count(0), total_count(0) {}
  
  void Reset() {
    send_count = 0;
    write_count = 0;
    write_imm_count = 0;
    read_count = 0;
    cas_count = 0;
    total_count = 0;
  }
};

// Global RDMA statistics collector (enabled flag controlled by AccessTimingCollector)
extern RdmaCallStats g_rdma_stats;
extern bool g_rdma_stats_enabled;

// Inline function to record RDMA calls - called from rdma.cc
inline void RecordRdmaCall(int op_type) {
  if (!g_rdma_stats_enabled) return;
  
  g_rdma_stats.total_count++;
  switch (op_type) {
    case 0:  // IBV_WR_SEND
      g_rdma_stats.send_count++;
      break;
    case 1:  // IBV_WR_RDMA_WRITE
      g_rdma_stats.write_count++;
      break;
    case 2:  // IBV_WR_RDMA_WRITE_WITH_IMM
      g_rdma_stats.write_imm_count++;
      break;
    case 3:  // IBV_WR_RDMA_READ
      g_rdma_stats.read_count++;
      break;
    case 4:  // IBV_WR_ATOMIC_CMP_AND_SWP (CAS)
      g_rdma_stats.cas_count++;
      break;
  }
}

// Access timing statistics - in-memory only
struct AccessTimingStats {
  // Local access stats
  std::atomic<unsigned long> local_count;
  std::atomic<unsigned long long> local_total_time;
  std::atomic<unsigned long long> local_t1_total;
  std::atomic<unsigned long long> local_t2_request_total;  // Request phase
  std::atomic<unsigned long long> local_t2_reply_total;    // Reply phase
  std::atomic<unsigned long long> local_t3_total;
  
  // Cache hit stats
  std::atomic<unsigned long> cache_hit_count;
  std::atomic<unsigned long long> cache_hit_total_time;
  std::atomic<unsigned long long> cache_hit_t1_total;
  std::atomic<unsigned long long> cache_hit_t2_request_total;
  std::atomic<unsigned long long> cache_hit_t2_reply_total;
  std::atomic<unsigned long long> cache_hit_t3_total;
  
  // Cache miss simple stats (direct fetch from home node)
  std::atomic<unsigned long> cache_miss_simple_count;
  std::atomic<unsigned long long> cache_miss_simple_total_time;
  std::atomic<unsigned long long> cache_miss_simple_t1_total;
  std::atomic<unsigned long long> cache_miss_simple_t2_request_total;
  std::atomic<unsigned long long> cache_miss_simple_t2_reply_total;
  std::atomic<unsigned long long> cache_miss_simple_t3_total;
  
  // Cache miss complex stats (requires invalidate/forward)
  std::atomic<unsigned long> cache_miss_complex_count;
  std::atomic<unsigned long long> cache_miss_complex_total_time;
  std::atomic<unsigned long long> cache_miss_complex_t1_total;
  std::atomic<unsigned long long> cache_miss_complex_t2_request_total;
  std::atomic<unsigned long long> cache_miss_complex_t2_reply_total;
  std::atomic<unsigned long long> cache_miss_complex_t3_total;
  
  AccessTimingStats() : 
    local_count(0), local_total_time(0), local_t1_total(0), local_t2_request_total(0), local_t2_reply_total(0), local_t3_total(0),
    cache_hit_count(0), cache_hit_total_time(0), cache_hit_t1_total(0), cache_hit_t2_request_total(0), cache_hit_t2_reply_total(0), cache_hit_t3_total(0),
    cache_miss_simple_count(0), cache_miss_simple_total_time(0), cache_miss_simple_t1_total(0), cache_miss_simple_t2_request_total(0), cache_miss_simple_t2_reply_total(0), cache_miss_simple_t3_total(0),
    cache_miss_complex_count(0), cache_miss_complex_total_time(0), cache_miss_complex_t1_total(0), cache_miss_complex_t2_request_total(0), cache_miss_complex_t2_reply_total(0), cache_miss_complex_t3_total(0) {}
  
  void Reset() {
    local_count = 0; local_total_time = 0; local_t1_total = 0; local_t2_request_total = 0; local_t2_reply_total = 0; local_t3_total = 0;
    cache_hit_count = 0; cache_hit_total_time = 0; cache_hit_t1_total = 0; cache_hit_t2_request_total = 0; cache_hit_t2_reply_total = 0; cache_hit_t3_total = 0;
    cache_miss_simple_count = 0; cache_miss_simple_total_time = 0; cache_miss_simple_t1_total = 0; cache_miss_simple_t2_request_total = 0; cache_miss_simple_t2_reply_total = 0; cache_miss_simple_t3_total = 0;
    cache_miss_complex_count = 0; cache_miss_complex_total_time = 0; cache_miss_complex_t1_total = 0; cache_miss_complex_t2_request_total = 0; cache_miss_complex_t2_reply_total = 0; cache_miss_complex_t3_total = 0;
  }
};

class AccessTimingCollector {
private:
  AccessTimingStats stats_;
  int worker_id_;
  bool enabled_;
  
public:
  AccessTimingCollector() : worker_id_(0), enabled_(false) {}
  
  void Init(int worker_id) {
    worker_id_ = worker_id;
    enabled_ = true;
    stats_.Reset();
    // Enable global RDMA stats collection
    g_rdma_stats_enabled = true;
    g_rdma_stats.Reset();
  }
  
  void RecordAccess(const WorkRequest* wr);
  
  void PrintStats();
  
  void ResetStats() { 
    stats_.Reset(); 
    g_rdma_stats.Reset();
  }
  
  void SetEnabled(bool enabled) { 
    enabled_ = enabled; 
    g_rdma_stats_enabled = enabled;
  }
  bool IsEnabled() const { return enabled_; }
  
  // Get RDMA statistics
  const RdmaCallStats& GetRdmaStats() const { return g_rdma_stats; }
};

#endif /* INCLUDE_ACCESS_TIMING_H_ */
