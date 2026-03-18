#ifndef KV_MEMCACHED_H
#define KV_MEMCACHED_H

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef enum {
    MEMC_OK = 0,
    MEMC_NOT_FOUND,
    MEMC_EXISTS,
    MEMC_NOT_STORED,
    MEMC_NO_MEMORY,
    MEMC_NON_NUMERIC,
    MEMC_INVALID
} memc_result;

typedef struct {
    size_t expected_items;
    size_t worker_count;
    size_t memory_limit_bytes;
    size_t value_size;
    uint32_t default_ttl;
    int hashpower_init;
    bool use_cas;
} memc_config;

typedef struct {
    uint64_t get_cmds;
    uint64_t set_cmds;
    uint64_t delete_cmds;
    uint64_t incr_cmds;
    uint64_t decr_cmds;
    uint64_t flush_cmds;
    uint64_t get_hits;
    uint64_t get_misses;
    uint64_t evictions;
    uint64_t curr_bytes;
    uint64_t curr_items;
    uint64_t total_items;
} memc_runtime_stats;

typedef struct {
    pthread_mutex_t stats_lock;
    memc_runtime_stats runtime_stats;
    size_t value_size;
    uint32_t default_ttl;
} MemcStore;

MemcStore *memc_store_new(const memc_config *config);
void memc_store_destroy(MemcStore *store);

memc_result memc_get(MemcStore *store, uint64_t key, char *value_out,
                     size_t value_cap, size_t *value_len_out,
                     uint64_t *cas_out);
memc_result memc_set(MemcStore *store, uint64_t key, const char *value,
                     size_t value_len, uint32_t ttl, uint64_t *cas_out);
memc_result memc_add(MemcStore *store, uint64_t key, const char *value,
                     size_t value_len, uint32_t ttl, uint64_t *cas_out);
memc_result memc_replace(MemcStore *store, uint64_t key, const char *value,
                         size_t value_len, uint32_t ttl, uint64_t *cas_out);
memc_result memc_append(MemcStore *store, uint64_t key, const char *value,
                        size_t value_len, uint64_t *cas_out);
memc_result memc_prepend(MemcStore *store, uint64_t key, const char *value,
                         size_t value_len, uint64_t *cas_out);
memc_result memc_cas(MemcStore *store, uint64_t key, const char *value,
                     size_t value_len, uint32_t ttl, uint64_t expected_cas,
                     uint64_t *new_cas_out);
memc_result memc_delete(MemcStore *store, uint64_t key);
memc_result memc_incr(MemcStore *store, uint64_t key, uint64_t delta,
                      uint64_t *value_out, uint64_t *cas_io);
memc_result memc_decr(MemcStore *store, uint64_t key, uint64_t delta,
                      uint64_t *value_out, uint64_t *cas_io);
void memc_flush_all(MemcStore *store, uint32_t ttl);
void memc_get_stats(MemcStore *store, memc_runtime_stats *out);

#endif
