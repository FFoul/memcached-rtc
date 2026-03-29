// Copyright (c) 2018 The GAM Authors

#include "access_timing.h"
#include "util.h"
#include "log.h"
#include <stdio.h>

// Global RDMA statistics variables
RdmaCallStats g_rdma_stats;
bool g_rdma_stats_enabled = false;

void AccessTimingCollector::RecordAccess(const WorkRequest* wr) {
  if (!enabled_) {
    return;
  }
  
  // Only record READ and WRITE operations
  if (wr->op != READ && wr->op != WRITE) {
    return;
  }
  
  // Skip if timing data is incomplete
  if (wr->access_start_time == 0 || wr->access_end_time == 0) {
    return;
  }
  
  // Calculate timing components
  long total_time = wr->access_end_time - wr->access_start_time;
  long t1 = (wr->cache_check_end_time > 0) ? 
            (wr->cache_check_end_time - wr->access_start_time) : 0;
  
  // T2: Remote communication time
  // We measure the total RTT using local timestamps, then subtract home node processing time
  long t2_total = 0;
  long t2_request = 0;
  long t2_reply = 0;
  
  if (wr->request_send_time > 0 && wr->reply_receive_time > 0) {
    if (wr->reply_receive_time >= wr->request_send_time) {
      // Total RTT measured on request node (using local clock, accurate)
      long rtt = wr->reply_receive_time - wr->request_send_time;
      
      // Home processing time measured on home node (using local clock, accurate)
      long home_proc = wr->home_processing_time;
      
      // Network time = RTT - home processing time
      long network_time = rtt - home_proc;
      if (network_time < 0) network_time = 0;  // Sanity check
      
      // Split network time equally between request and reply (symmetric network assumption)
      t2_request = network_time / 2;
      t2_reply = network_time - t2_request;
      t2_total = t2_request + t2_reply;
      
    } else {
      epicLog(LOG_WARNING, "Invalid timing: reply_receive_time < request_send_time (wr=%p, recv=%ld, send=%ld)",
              wr, wr->reply_receive_time, wr->request_send_time);
    }
  }
  
  long t3 = total_time - t1 - t2_total;
  if (t3 < 0) t3 = 0;
  
  // Determine category based on access_category field:
  // 0 = LOCAL, 1 = CACHE_HIT, 2 = CACHE_MISS_SIMPLE, 3 = CACHE_MISS_COMPLEX
  int category = wr->access_category;
  
  // For cache miss (category == 2), check if T3 is unusually large
  // If T3 exceeds a single RTT threshold, it indicates invalidate/forward operations
  // which means this is actually a cache miss complex, not simple
  if (category == 2 && t2_total > 0) {
    if (t3 > 3000) {  // Also require T3 > 3000ns (3us) to avoid false positives
      category = 3;  // Reclassify as CACHE_MISS_COMPLEX
    }
  }
  
  if (category == 0) {
    // Local access
    stats_.local_count++;
    // Store totals in microseconds to reduce overflow risk
    stats_.local_total_time += total_time / 1000;
    stats_.local_t1_total += t1 / 1000;
    stats_.local_t2_request_total += t2_request / 1000;
    stats_.local_t2_reply_total += t2_reply / 1000;
    stats_.local_t3_total += t3 / 1000;
  } else if (category == 1) {
    // Cache hit
    stats_.cache_hit_count++;
    stats_.cache_hit_total_time += total_time / 1000;
    stats_.cache_hit_t1_total += t1 / 1000;
    stats_.cache_hit_t2_request_total += t2_request / 1000;
    stats_.cache_hit_t2_reply_total += t2_reply / 1000;
    stats_.cache_hit_t3_total += t3 / 1000;
  } else if (category == 2) {
    // Cache miss simple (direct fetch from home node)
    stats_.cache_miss_simple_count++;
    stats_.cache_miss_simple_total_time += total_time / 1000;
    stats_.cache_miss_simple_t1_total += t1 / 1000;
    stats_.cache_miss_simple_t2_request_total += t2_request / 1000;
    stats_.cache_miss_simple_t2_reply_total += t2_reply / 1000;
    stats_.cache_miss_simple_t3_total += t3 / 1000;
  } else if (category == 3) {
    // Cache miss complex (requires invalidate/forward)
    stats_.cache_miss_complex_count++;
    stats_.cache_miss_complex_total_time += total_time / 1000;
    stats_.cache_miss_complex_t1_total += t1 / 1000;
    stats_.cache_miss_complex_t2_request_total += t2_request / 1000;
    stats_.cache_miss_complex_t2_reply_total += t2_reply / 1000;
    stats_.cache_miss_complex_t3_total += t3 / 1000;
  }
}

void AccessTimingCollector::PrintStats() {
  printf("\n");
  printf("================================================================================\n");
  printf("                    Access Timing Statistics (Worker %d)\n", worker_id_);
  printf("================================================================================\n");
  printf("\n");
  
  unsigned long local_cnt = stats_.local_count.load();
  unsigned long cache_hit_cnt = stats_.cache_hit_count.load();
  unsigned long cache_miss_simple_cnt = stats_.cache_miss_simple_count.load();
  unsigned long cache_miss_complex_cnt = stats_.cache_miss_complex_count.load();
  unsigned long total_cnt = local_cnt + cache_hit_cnt + cache_miss_simple_cnt + cache_miss_complex_cnt;
  
  printf("Total Accesses: %lu\n", total_cnt);
  printf("  - Local:             %lu (%.2f%%)\n", local_cnt, 
         total_cnt > 0 ? (100.0 * local_cnt / total_cnt) : 0.0);
  printf("  - Cache Hit:         %lu (%.2f%%)\n", cache_hit_cnt,
         total_cnt > 0 ? (100.0 * cache_hit_cnt / total_cnt) : 0.0);
  printf("  - Cache Miss Simple: %lu (%.2f%%)\n", cache_miss_simple_cnt,
         total_cnt > 0 ? (100.0 * cache_miss_simple_cnt / total_cnt) : 0.0);
  printf("  - Cache Miss Complex:%lu (%.2f%%)\n", cache_miss_complex_cnt,
         total_cnt > 0 ? (100.0 * cache_miss_complex_cnt / total_cnt) : 0.0);
  printf("\n");
  
  printf("--------------------------------------------------------------------------------\n");
    printf("%-15s %10s %10s %10s %12s %10s %10s\n", 
      "Type", "Count", "Total", "T1", "T2-Req", "T2-Rep", "T3");
  printf("--------------------------------------------------------------------------------\n");
    printf("Note: Totals are shown in microseconds (us); Averages are shown in nanoseconds (ns).\n\n");
  
  // Local access stats
  if (local_cnt > 0) {
      unsigned long long local_total = stats_.local_total_time.load();
      unsigned long long local_t1 = stats_.local_t1_total.load();
      unsigned long long local_t2_req = stats_.local_t2_request_total.load();
      unsigned long long local_t2_rep = stats_.local_t2_reply_total.load();
      unsigned long long local_t3 = stats_.local_t3_total.load();
      // Totals are stored in microseconds already
      unsigned long long local_total_us = local_total;
      unsigned long long local_t1_us = local_t1;
      unsigned long long local_t2_req_us = local_t2_req;
      unsigned long long local_t2_rep_us = local_t2_rep;
      unsigned long long local_t3_us = local_t3;
      printf("%-15s %10lu %10llu %10llu %12llu %10llu %10llu\n",
        "Local-Total(us)",
        local_cnt,
        local_total_us,
        local_t1_us,
        local_t2_req_us,
        local_t2_rep_us,
        local_t3_us);
      printf("%-15s %10s %10.2f %10.2f %12.2f %10.2f %10.2f\n",
        "Local-Avg(ns)",
        "-",
        // Averages printed in nanoseconds
        (double)local_total * 1000.0 / local_cnt,
        (double)local_t1 * 1000.0 / local_cnt,
        (double)local_t2_req * 1000.0 / local_cnt,
        (double)local_t2_rep * 1000.0 / local_cnt,
        (double)local_t3 * 1000.0 / local_cnt);
    printf("\n");
  }
  
  // Cache hit stats
  if (cache_hit_cnt > 0) {
      unsigned long long hit_total = stats_.cache_hit_total_time.load();
      unsigned long long hit_t1 = stats_.cache_hit_t1_total.load();
      unsigned long long hit_t2_req = stats_.cache_hit_t2_request_total.load();
      unsigned long long hit_t2_rep = stats_.cache_hit_t2_reply_total.load();
      unsigned long long hit_t3 = stats_.cache_hit_t3_total.load();
      unsigned long long hit_total_us = hit_total;
      unsigned long long hit_t1_us = hit_t1;
      unsigned long long hit_t2_req_us = hit_t2_req;
      unsigned long long hit_t2_rep_us = hit_t2_rep;
      unsigned long long hit_t3_us = hit_t3;
      printf("%-15s %10lu %10llu %10llu %12llu %10llu %10llu\n",
        "CacheHit-Total(us)",
        cache_hit_cnt,
        hit_total_us,
        hit_t1_us,
        hit_t2_req_us,
        hit_t2_rep_us,
        hit_t3_us);
      printf("%-15s %10s %10.2f %10.2f %12.2f %10.2f %10.2f\n",
        "CacheHit-Avg(ns)",
        "-",
        (double)hit_total * 1000.0 / cache_hit_cnt,
        (double)hit_t1 * 1000.0 / cache_hit_cnt,
        (double)hit_t2_req * 1000.0 / cache_hit_cnt,
        (double)hit_t2_rep * 1000.0 / cache_hit_cnt,
        (double)hit_t3 * 1000.0 / cache_hit_cnt);
    printf("\n");
  }
  
  // Cache miss simple stats
  if (cache_miss_simple_cnt > 0) {
      unsigned long long miss_simple_total = stats_.cache_miss_simple_total_time.load();
      unsigned long long miss_simple_t1 = stats_.cache_miss_simple_t1_total.load();
      unsigned long long miss_simple_t2_req = stats_.cache_miss_simple_t2_request_total.load();
      unsigned long long miss_simple_t2_rep = stats_.cache_miss_simple_t2_reply_total.load();
      unsigned long long miss_simple_t3 = stats_.cache_miss_simple_t3_total.load();
      unsigned long long miss_simple_total_us = miss_simple_total;
      unsigned long long miss_simple_t1_us = miss_simple_t1;
      unsigned long long miss_simple_t2_req_us = miss_simple_t2_req;
      unsigned long long miss_simple_t2_rep_us = miss_simple_t2_rep;
      unsigned long long miss_simple_t3_us = miss_simple_t3;
      printf("%-15s %10lu %10llu %10llu %12llu %10llu %10llu\n",
        "MissSimple-Tot(us)",
        cache_miss_simple_cnt,
        miss_simple_total_us,
        miss_simple_t1_us,
        miss_simple_t2_req_us,
        miss_simple_t2_rep_us,
        miss_simple_t3_us);
      printf("%-15s %10s %10.2f %10.2f %12.2f %10.2f %10.2f\n",
        "MissSimple-Avg(ns)",
        "-",
        (double)miss_simple_total * 1000.0 / cache_miss_simple_cnt,
        (double)miss_simple_t1 * 1000.0 / cache_miss_simple_cnt,
        (double)miss_simple_t2_req * 1000.0 / cache_miss_simple_cnt,
        (double)miss_simple_t2_rep * 1000.0 / cache_miss_simple_cnt,
        (double)miss_simple_t3 * 1000.0 / cache_miss_simple_cnt);
    printf("\n");
  }
  
  // Cache miss complex stats
  if (cache_miss_complex_cnt > 0) {
      unsigned long long miss_complex_total = stats_.cache_miss_complex_total_time.load();
      unsigned long long miss_complex_t1 = stats_.cache_miss_complex_t1_total.load();
      unsigned long long miss_complex_t2_req = stats_.cache_miss_complex_t2_request_total.load();
      unsigned long long miss_complex_t2_rep = stats_.cache_miss_complex_t2_reply_total.load();
      unsigned long long miss_complex_t3 = stats_.cache_miss_complex_t3_total.load();
      unsigned long long miss_complex_total_us = miss_complex_total;
      unsigned long long miss_complex_t1_us = miss_complex_t1;
      unsigned long long miss_complex_t2_req_us = miss_complex_t2_req;
      unsigned long long miss_complex_t2_rep_us = miss_complex_t2_rep;
      unsigned long long miss_complex_t3_us = miss_complex_t3;
      printf("%-15s %10lu %10llu %10llu %12llu %10llu %10llu\n",
        "MissComplex-Tot(us)",
        cache_miss_complex_cnt,
        miss_complex_total_us,
        miss_complex_t1_us,
        miss_complex_t2_req_us,
        miss_complex_t2_rep_us,
        miss_complex_t3_us);
      printf("%-15s %10s %10.2f %10.2f %12.2f %10.2f %10.2f\n",
        "MissComplex-Avg(ns)",
        "-",
        (double)miss_complex_total * 1000.0 / cache_miss_complex_cnt,
        (double)miss_complex_t1 * 1000.0 / cache_miss_complex_cnt,
        (double)miss_complex_t2_req * 1000.0 / cache_miss_complex_cnt,
        (double)miss_complex_t2_rep * 1000.0 / cache_miss_complex_cnt,
        (double)miss_complex_t3 * 1000.0 / cache_miss_complex_cnt);
  }
  
  printf("--------------------------------------------------------------------------------\n");
  printf("\n");
  
  // Calculate overall totals across all access types
  if (total_cnt > 0) {
    unsigned long long overall_total = stats_.local_total_time.load() +
                                       stats_.cache_hit_total_time.load() +
                                       stats_.cache_miss_simple_total_time.load() +
                                       stats_.cache_miss_complex_total_time.load();
    
    unsigned long long overall_t1 = stats_.local_t1_total.load() +
                                    stats_.cache_hit_t1_total.load() +
                                    stats_.cache_miss_simple_t1_total.load() +
                                    stats_.cache_miss_complex_t1_total.load();
    
    unsigned long long overall_t2_req = stats_.local_t2_request_total.load() +
                                        stats_.cache_hit_t2_request_total.load() +
                                        stats_.cache_miss_simple_t2_request_total.load() +
                                        stats_.cache_miss_complex_t2_request_total.load();
    
    unsigned long long overall_t2_rep = stats_.local_t2_reply_total.load() +
                                        stats_.cache_hit_t2_reply_total.load() +
                                        stats_.cache_miss_simple_t2_reply_total.load() +
                                        stats_.cache_miss_complex_t2_reply_total.load();
    
    unsigned long long overall_t3 = stats_.local_t3_total.load() +
                                    stats_.cache_hit_t3_total.load() +
                                    stats_.cache_miss_simple_t3_total.load() +
                                    stats_.cache_miss_complex_t3_total.load();
    
    unsigned long long overall_t1_t2req = overall_t1 + overall_t2_req;
    unsigned long long overall_t2rep_t3 = overall_t2_rep + overall_t3;
    
    double percent_t1_t2req = (overall_total > 0) ? (100.0 * overall_t1_t2req / overall_total) : 0.0;
    double percent_t2rep_t3 = (overall_total > 0) ? (100.0 * overall_t2rep_t3 / overall_total) : 0.0;
    
    printf("================================================================================\n");
    printf("                    Overall Statistics (All Accesses)\n");
    printf("================================================================================\n");
    printf("\n");
    printf("Total Accesses: %lu\n", total_cnt);
    printf("\n");
    printf("Time Component Totals (in microseconds):\n");
    printf("  Total Time:          %15llu us\n", overall_total);
    printf("  T1 Total:            %15llu us\n", overall_t1);
    printf("  T2-Req Total:        %15llu us\n", overall_t2_req);
    printf("  T2-Rep Total:        %15llu us\n", overall_t2_rep);
    printf("  T3 Total:            %15llu us\n", overall_t3);
    printf("  (T1 + T2-Req):       %15llu us  (%.2f%% of Total)\n", 
           overall_t1_t2req, percent_t1_t2req);
    printf("  (T2-Rep + T3):       %15llu us  (%.2f%% of Total)\n", 
           overall_t2rep_t3, percent_t2rep_t3);
    printf("\n");
  }
  
  // RDMA Call Statistics
  printf("================================================================================\n");
  printf("                    RDMA Call Statistics (Worker %d)\n", worker_id_);
  printf("================================================================================\n");
  printf("\n");
  printf("Total RDMA Calls: %lu\n", g_rdma_stats.total_count);
  printf("  - Send:              %lu (%.2f%%)\n", g_rdma_stats.send_count,
         g_rdma_stats.total_count > 0 ? (100.0 * g_rdma_stats.send_count / g_rdma_stats.total_count) : 0.0);
  printf("  - Write:             %lu (%.2f%%)\n", g_rdma_stats.write_count,
         g_rdma_stats.total_count > 0 ? (100.0 * g_rdma_stats.write_count / g_rdma_stats.total_count) : 0.0);
  printf("  - Write w/ Imm:      %lu (%.2f%%)\n", g_rdma_stats.write_imm_count,
         g_rdma_stats.total_count > 0 ? (100.0 * g_rdma_stats.write_imm_count / g_rdma_stats.total_count) : 0.0);
  printf("  - Read:              %lu (%.2f%%)\n", g_rdma_stats.read_count,
         g_rdma_stats.total_count > 0 ? (100.0 * g_rdma_stats.read_count / g_rdma_stats.total_count) : 0.0);
  printf("  - CAS:               %lu (%.2f%%)\n", g_rdma_stats.cas_count,
         g_rdma_stats.total_count > 0 ? (100.0 * g_rdma_stats.cas_count / g_rdma_stats.total_count) : 0.0);
  printf("\n");
  
  // Summary: Cache miss count vs RDMA calls
  unsigned long total_cache_miss = cache_miss_simple_cnt + cache_miss_complex_cnt;
  unsigned long rdma_minus_miss = g_rdma_stats.total_count - total_cache_miss;
  printf("--------------------------------------------------------------------------------\n");
  printf("Summary:\n");
  printf("  Total Cache Miss:           %lu\n", total_cache_miss);
  printf("  Total RDMA Calls:           %lu\n", g_rdma_stats.total_count);
  printf("  RDMA Calls - Cache Miss:    %lu\n", rdma_minus_miss);
  printf("--------------------------------------------------------------------------------\n");
  printf("\n");
  
  printf("Legend:\n");
  printf("  Total:   Total access time in microseconds\n");
  printf("  T1:      Cache check time\n");
  printf("  T2-Req:  Request propagation time (request node -> home node)\n");
  printf("  T2-Rep:  Reply propagation time (home node -> request node)\n");
  printf("  T3:      Other maintenance overhead\n");
  printf("               T3 = Total - T1 - T2-Req - T2-Rep\n");
  printf("\n");
  printf("  MissSimple:  Direct fetch from home node\n");
  printf("  MissComplex: Requires invalidate/forward operations\n");
  printf("================================================================================\n");
  printf("\n");
}
