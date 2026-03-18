#include "kv_memcached.h"

#ifndef RTC_BENCHMARK
#define RTC_BENCHMARK 1
#endif
#include "memcached.h"

#include <inttypes.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define NUM_WORKERS 4
#define NUM_OPERATIONS 8
#define READ_RATIO 90
#define VALUE_SIZE 32
#define MEMORY_MB 256
#define DEFAULT_TTL 0
#define TRACE_DIR "./testdata"
#define REALTIME_MAXDELTA (60 * 60 * 24 * 30)

time_t process_started;

typedef struct {
  size_t thread_id;
  MemcStore *store;
  uint64_t operations_done;
  double duration_ms;
} worker_args;

static uint64_t mstime(void) {
  struct timespec ts;

  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static void refresh_current_time(void) {
  time_t now = time(NULL);

  if (process_started == 0) {
    process_started = now - 2;
  }
  current_time = (rel_time_t)(now - process_started);
}

static rel_time_t rtc_realtime(time_t exptime) {
  if (exptime == 0) {
    return 0;
  }

  if (exptime > REALTIME_MAXDELTA) {
    if (exptime <= process_started) {
      return 1;
    }
    return (rel_time_t)(exptime - process_started);
  }

  return (rel_time_t)(exptime + current_time);
}

static int choose_hashpower(size_t expected_items) {
  int power = HASHPOWER_DEFAULT;
  size_t target =
      expected_items == 0 ? (1UL << HASHPOWER_DEFAULT) : expected_items * 2;

  while (power < 31) {
    size_t buckets = 1ULL << power;
    if ((buckets * 3) / 2 >= target) {
      break;
    }
    power++;
  }

  return power;
}

static void init_stats(void) {
  memset(&stats, 0, sizeof(stats));
  pthread_mutex_init(&stats.mutex, NULL);
  stats.accepting_conns = true;
  stats.started = time(NULL) - 2;
  stats_prefix_init();
}

static void init_settings(const memc_config *config) {
  memset(&settings, 0, sizeof(settings));
  settings.use_cas = config->use_cas;
  settings.access = 0700;
  settings.port = 11211;
  settings.udpport = 11211;
  settings.maxbytes = config->memory_limit_bytes;
  settings.maxconns = 1024;
  settings.oldest_live = 0;
  settings.evict_to_free = 1;
  settings.factor = 1.25;
  settings.chunk_size = 48;
  settings.num_threads =
      (int)(config->worker_count == 0 ? 1 : config->worker_count);
  settings.num_threads_per_udp = 0;
  settings.prefix_delimiter = ':';
  settings.detail_enabled = 0;
  settings.reqs_per_event = 20;
  settings.binding_protocol = ascii_prot;
  settings.backlog = 1024;
  settings.item_size_max = 1024 * 1024;
  settings.sasl = false;
  settings.maxconns_fast = false;
  settings.hashpower_init = config->hashpower_init > 0
                                ? config->hashpower_init
                                : choose_hashpower(config->expected_items);
}

static void fill_keybuf(uint64_t key, char *keybuf, size_t keybuf_size,
                        size_t *nkey_out) {
  int written = snprintf(keybuf, keybuf_size, "%" PRIu64, key);

  if (written < 0 || (size_t)written >= keybuf_size) {
    keybuf[0] = '\0';
    *nkey_out = 0;
    return;
  }

  *nkey_out = (size_t)written;
}

static void sync_runtime_stats_locked(MemcStore *store) {
  store->runtime_stats.curr_items = stats.curr_items;
  store->runtime_stats.curr_bytes = stats.curr_bytes;
  store->runtime_stats.total_items = stats.total_items;
  store->runtime_stats.evictions = stats.evictions;
}

static void sync_runtime_stats(MemcStore *store) {
  pthread_mutex_lock(&store->stats_lock);
  sync_runtime_stats_locked(store);
  pthread_mutex_unlock(&store->stats_lock);
}

static memc_result from_store_result(enum store_item_type result) {
  switch (result) {
  case STORED:
    return MEMC_OK;
  case EXISTS:
    return MEMC_EXISTS;
  case NOT_FOUND:
    return MEMC_NOT_FOUND;
  case NOT_STORED:
  default:
    return MEMC_NOT_STORED;
  }
}

static void get_load_path(char *path_out, size_t path_size) {
  snprintf(path_out, path_size, "%s/load_%d.csv", TRACE_DIR, NUM_OPERATIONS);
}

static void get_run_path(char *path_out, size_t path_size, size_t thread_id) {
  snprintf(path_out, path_size, "%s/run_%d_%d_%zu.csv", TRACE_DIR,
           NUM_OPERATIONS, READ_RATIO, thread_id);
}

static int ensure_test_traces(void) {
  char path[PATH_MAX];
  size_t i;

  get_load_path(path, sizeof(path));
  if (access(path, F_OK) != 0) {
    fprintf(stderr, "missing load trace: %s\n", path);
    return -1;
  }

  for (i = 0; i < NUM_WORKERS; ++i) {
    get_run_path(path, sizeof(path), i);
    if (access(path, F_OK) != 0) {
      fprintf(stderr, "missing run trace: %s\n", path);
      return -1;
    }
  }

  return 0;
}

static char *make_value(void) {
  char *value = malloc(VALUE_SIZE + 1);

  if (value == NULL) {
    return NULL;
  }

  memset(value, 'x', VALUE_SIZE);
  value[VALUE_SIZE] = '\0';
  return value;
}

static memc_result store_value(MemcStore *store, uint64_t key,
                               const char *value, size_t value_len,
                               uint32_t ttl, int mode, uint64_t cas_in,
                               uint64_t *cas_out) {
  char keybuf[32];
  size_t nkey = 0;
  item *it;
  conn c = {0};
  enum store_item_type result;

  if (store == NULL || value == NULL || value_len == 0 ||
      value_len > INT_MAX - 2) {
    return MEMC_INVALID;
  }

  refresh_current_time();
  fill_keybuf(key, keybuf, sizeof(keybuf), &nkey);
  if (nkey == 0) {
    return MEMC_INVALID;
  }

  it = item_alloc(keybuf, nkey, 0, rtc_realtime(ttl), (int)value_len + 2);
  if (it == NULL) {
    return MEMC_NO_MEMORY;
  }

  memcpy(ITEM_data(it), value, value_len);
  memcpy(ITEM_data(it) + value_len, "\r\n", 2);
  if (mode == NREAD_CAS) {
    ITEM_set_cas(it, cas_in);
  }

  result = store_item(it, mode, &c);
  item_remove(it);

  if (result == STORED && settings.detail_enabled) {
    stats_prefix_record_set(keybuf, nkey);
  }

  pthread_mutex_lock(&store->stats_lock);
  store->runtime_stats.set_cmds++;
  sync_runtime_stats_locked(store);
  pthread_mutex_unlock(&store->stats_lock);

  if (cas_out != NULL) {
    *cas_out = c.cas;
  }

  return from_store_result(result);
}

static memc_result delta_value(MemcStore *store, uint64_t key, uint64_t delta,
                               bool incr, uint64_t *value_out,
                               uint64_t *cas_io) {
  char keybuf[32];
  size_t nkey = 0;
  char buf[INCR_MAX_STORAGE_LEN];
  conn c = {0};
  enum delta_result_type result;
  uint64_t cas = cas_io != NULL ? *cas_io : 0;

  if (store == NULL) {
    return MEMC_INVALID;
  }

  refresh_current_time();
  fill_keybuf(key, keybuf, sizeof(keybuf), &nkey);
  if (nkey == 0) {
    return MEMC_INVALID;
  }

  result = add_delta(&c, keybuf, nkey, incr, (int64_t)delta, buf,
                     cas_io != NULL ? &cas : NULL);

  pthread_mutex_lock(&store->stats_lock);
  if (incr) {
    store->runtime_stats.incr_cmds++;
  } else {
    store->runtime_stats.decr_cmds++;
  }
  sync_runtime_stats_locked(store);
  pthread_mutex_unlock(&store->stats_lock);

  if (result == OK) {
    if (value_out != NULL) {
      (void)safe_strtoull(buf, value_out);
    }
    if (cas_io != NULL) {
      *cas_io = cas;
    }
    return MEMC_OK;
  }
  if (result == DELTA_ITEM_NOT_FOUND) {
    return MEMC_NOT_FOUND;
  }
  if (result == NON_NUMERIC) {
    return MEMC_NON_NUMERIC;
  }

  return MEMC_NOT_STORED;
}

static int populate_store(MemcStore *store) {
  char path[PATH_MAX];
  char line[128];
  char *value;
  FILE *fp;
  size_t count = 0;
  uint64_t start;
  uint64_t end;

  get_load_path(path, sizeof(path));
  fp = fopen(path, "r");
  if (fp == NULL) {
    perror("fopen load trace");
    return -1;
  }

  value = make_value();
  if (value == NULL) {
    fclose(fp);
    return -1;
  }

  start = mstime();
  while (count < NUM_OPERATIONS && fgets(line, sizeof(line), fp) != NULL) {
    uint64_t key = strtoull(line, NULL, 10);

    if (memc_set(store, key, value, VALUE_SIZE, DEFAULT_TTL, NULL) != MEMC_OK) {
      fprintf(stderr, "populate failed for key %" PRIu64 "\n", key);
      free(value);
      fclose(fp);
      return -1;
    }
    count++;
  }
  end = mstime();

  printf("populate: %zu keys in %.2f ms\n", count, (double)(end - start));

  free(value);
  fclose(fp);
  return 0;
}

static void run_request(MemcStore *store, const char *op, uint64_t key,
                        char *value, char *read_value) {
  if (strcmp(op, "READ") == 0) {
    (void)memc_get(store, key, read_value, VALUE_SIZE + 1, NULL, NULL);
  } else if (strcmp(op, "UPDATE") == 0) {
    (void)memc_set(store, key, value, VALUE_SIZE, DEFAULT_TTL, NULL);
  } else if (strcmp(op, "INSERT") == 0) {
    (void)memc_add(store, key, value, VALUE_SIZE, DEFAULT_TTL, NULL);
  } else if (strcmp(op, "DELETE") == 0) {
    (void)memc_delete(store, key);
  }
}

static void *benchmark_worker(void *arg) {
  worker_args *worker = arg;
  char path[PATH_MAX];
  char line[256];
  char *value;
  char *read_value;
  FILE *fp;
  uint64_t start;
  uint64_t end;
  double throughput;

  get_run_path(path, sizeof(path), worker->thread_id);
  fp = fopen(path, "r");
  if (fp == NULL) {
    perror("fopen benchmark trace");
    return NULL;
  }

  value = make_value();
  read_value = malloc(VALUE_SIZE + 1);
  if (value == NULL || read_value == NULL) {
    free(value);
    free(read_value);
    fclose(fp);
    return NULL;
  }

  start = mstime();
  while (fgets(line, sizeof(line), fp) != NULL) {
    char *comma = strchr(line, ',');
    uint64_t key;

    if (comma == NULL) {
      continue;
    }

    *comma = '\0';
    key = strtoull(comma + 1, NULL, 10);
    run_request(worker->store, line, key, value, read_value);
    worker->operations_done++;
  }
  end = mstime();

  worker->duration_ms = (double)(end - start);
  throughput = worker->duration_ms > 0.0
                   ? worker->operations_done * 1000.0 / worker->duration_ms
                   : 0.0;

  printf("worker %zu: %.2f ms, %.2f ops/s\n", worker->thread_id,
         worker->duration_ms, throughput);

  free(value);
  free(read_value);
  fclose(fp);
  return NULL;
}

void append_stat(const char *name, ADD_STAT add_stats, conn *c, const char *fmt,
                 ...) {
  char val_str[STAT_VAL_LEN];
  int vlen;
  va_list ap;

  if (add_stats == NULL || name == NULL || fmt == NULL) {
    return;
  }

  va_start(ap, fmt);
  vlen = vsnprintf(val_str, sizeof(val_str), fmt, ap);
  va_end(ap);

  if (vlen < 0) {
    return;
  }
  if ((size_t)vlen >= sizeof(val_str)) {
    vlen = (int)sizeof(val_str) - 1;
    val_str[vlen] = '\0';
  }

  add_stats(name, (uint16_t)strlen(name), val_str, (uint32_t)vlen, c);
}

MemcStore *memc_store_new(const memc_config *config) {
  MemcStore *store;

  if (config == NULL || config->memory_limit_bytes == 0 ||
      config->value_size == 0) {
    return NULL;
  }

  store = calloc(1, sizeof(*store));
  if (store == NULL) {
    return NULL;
  }

  pthread_mutex_init(&store->stats_lock, NULL);
  store->value_size = config->value_size;
  store->default_ttl = config->default_ttl;

  init_stats();
  init_settings(config);
  process_started = stats.started;
  refresh_current_time();
  assoc_init(settings.hashpower_init);
  slabs_init(settings.maxbytes, settings.factor, false);
  thread_init(settings.num_threads, NULL);
  if (start_assoc_maintenance_thread() != 0) {
    pthread_mutex_destroy(&store->stats_lock);
    free(store);
    return NULL;
  }
  sync_runtime_stats(store);

  return store;
}

void memc_store_destroy(MemcStore *store) {
  if (store == NULL) {
    return;
  }

  stop_assoc_maintenance_thread();
  pthread_mutex_destroy(&store->stats_lock);
  free(store);
}

memc_result memc_get(MemcStore *store, uint64_t key, char *value_out,
                     size_t value_cap, size_t *value_len_out,
                     uint64_t *cas_out) {
  char keybuf[32];
  size_t nkey = 0;
  item *it;
  size_t payload;

  if (store == NULL) {
    return MEMC_INVALID;
  }

  refresh_current_time();
  fill_keybuf(key, keybuf, sizeof(keybuf), &nkey);
  if (nkey == 0) {
    return MEMC_INVALID;
  }

  pthread_mutex_lock(&store->stats_lock);
  store->runtime_stats.get_cmds++;
  pthread_mutex_unlock(&store->stats_lock);

  it = item_get(keybuf, nkey);
  if (it == NULL) {
    pthread_mutex_lock(&store->stats_lock);
    store->runtime_stats.get_misses++;
    sync_runtime_stats_locked(store);
    pthread_mutex_unlock(&store->stats_lock);
    return MEMC_NOT_FOUND;
  }

  payload = (size_t)it->nbytes - 2;
  if (value_out != NULL && value_cap > 0) {
    size_t copied = payload < value_cap - 1 ? payload : value_cap - 1;

    memcpy(value_out, ITEM_data(it), copied);
    value_out[copied] = '\0';
  }
  if (value_len_out != NULL) {
    *value_len_out = payload;
  }
  if (cas_out != NULL) {
    *cas_out = ITEM_get_cas(it);
  }
  if (settings.detail_enabled) {
    stats_prefix_record_get(keybuf, nkey, true);
  }

  item_update(it);
  item_remove(it);

  pthread_mutex_lock(&store->stats_lock);
  store->runtime_stats.get_hits++;
  sync_runtime_stats_locked(store);
  pthread_mutex_unlock(&store->stats_lock);

  return MEMC_OK;
}

memc_result memc_set(MemcStore *store, uint64_t key, const char *value,
                     size_t value_len, uint32_t ttl, uint64_t *cas_out) {
  return store_value(store, key, value, value_len, ttl, NREAD_SET, 0, cas_out);
}

memc_result memc_add(MemcStore *store, uint64_t key, const char *value,
                     size_t value_len, uint32_t ttl, uint64_t *cas_out) {
  return store_value(store, key, value, value_len, ttl, NREAD_ADD, 0, cas_out);
}

memc_result memc_replace(MemcStore *store, uint64_t key, const char *value,
                         size_t value_len, uint32_t ttl, uint64_t *cas_out) {
  return store_value(store, key, value, value_len, ttl, NREAD_REPLACE, 0,
                     cas_out);
}

memc_result memc_append(MemcStore *store, uint64_t key, const char *value,
                        size_t value_len, uint64_t *cas_out) {
  return store_value(store, key, value, value_len, 0, NREAD_APPEND, 0, cas_out);
}

memc_result memc_prepend(MemcStore *store, uint64_t key, const char *value,
                         size_t value_len, uint64_t *cas_out) {
  return store_value(store, key, value, value_len, 0, NREAD_PREPEND, 0,
                     cas_out);
}

memc_result memc_cas(MemcStore *store, uint64_t key, const char *value,
                     size_t value_len, uint32_t ttl, uint64_t expected_cas,
                     uint64_t *new_cas_out) {
  return store_value(store, key, value, value_len, ttl, NREAD_CAS, expected_cas,
                     new_cas_out);
}

memc_result memc_delete(MemcStore *store, uint64_t key) {
  char keybuf[32];
  size_t nkey = 0;
  item *it;

  if (store == NULL) {
    return MEMC_INVALID;
  }

  refresh_current_time();
  fill_keybuf(key, keybuf, sizeof(keybuf), &nkey);
  if (nkey == 0) {
    return MEMC_INVALID;
  }

  pthread_mutex_lock(&store->stats_lock);
  store->runtime_stats.delete_cmds++;
  pthread_mutex_unlock(&store->stats_lock);

  it = item_get(keybuf, nkey);
  if (it == NULL) {
    sync_runtime_stats(store);
    return MEMC_NOT_FOUND;
  }

  item_unlink(it);
  item_remove(it);
  if (settings.detail_enabled) {
    stats_prefix_record_delete(keybuf, nkey);
  }

  sync_runtime_stats(store);
  return MEMC_OK;
}

memc_result memc_incr(MemcStore *store, uint64_t key, uint64_t delta,
                      uint64_t *value_out, uint64_t *cas_io) {
  return delta_value(store, key, delta, true, value_out, cas_io);
}

memc_result memc_decr(MemcStore *store, uint64_t key, uint64_t delta,
                      uint64_t *value_out, uint64_t *cas_io) {
  return delta_value(store, key, delta, false, value_out, cas_io);
}

void memc_flush_all(MemcStore *store, uint32_t ttl) {
  if (store == NULL) {
    return;
  }

  refresh_current_time();
  if (ttl > 0) {
    settings.oldest_live = rtc_realtime(ttl) - 1;
  } else {
    settings.oldest_live = current_time - 1;
  }
  item_flush_expired();

  pthread_mutex_lock(&store->stats_lock);
  store->runtime_stats.flush_cmds++;
  sync_runtime_stats_locked(store);
  pthread_mutex_unlock(&store->stats_lock);
}

void memc_get_stats(MemcStore *store, memc_runtime_stats *out) {
  if (store == NULL || out == NULL) {
    return;
  }

  pthread_mutex_lock(&store->stats_lock);
  sync_runtime_stats_locked(store);
  *out = store->runtime_stats;
  pthread_mutex_unlock(&store->stats_lock);
}

enum store_item_type do_store_item(item *it, int comm, conn *c,
                                   const uint32_t hv) {
  char *key = ITEM_key(it);
  item *old_it = do_item_get(key, it->nkey, hv);
  enum store_item_type stored = NOT_STORED;
  item *new_it = NULL;
  int flags;

  if (old_it != NULL && comm == NREAD_ADD) {
    do_item_update(old_it);
  } else if (old_it == NULL && (comm == NREAD_REPLACE || comm == NREAD_APPEND ||
                                comm == NREAD_PREPEND)) {
  } else if (comm == NREAD_CAS) {
    if (old_it == NULL) {
      stored = NOT_FOUND;
    } else if (ITEM_get_cas(it) == ITEM_get_cas(old_it)) {
      item_replace(old_it, it, hv);
      stored = STORED;
    } else {
      stored = EXISTS;
    }
  } else {
    if (comm == NREAD_APPEND || comm == NREAD_PREPEND) {
      flags = (int)strtol(ITEM_suffix(old_it), NULL, 10);
      new_it = do_item_alloc(key, it->nkey, flags, old_it->exptime,
                             it->nbytes + old_it->nbytes - 2);
      if (new_it == NULL) {
        if (old_it != NULL) {
          do_item_remove(old_it);
        }
        return NOT_STORED;
      }

      if (comm == NREAD_APPEND) {
        memcpy(ITEM_data(new_it), ITEM_data(old_it), old_it->nbytes);
        memcpy(ITEM_data(new_it) + old_it->nbytes - 2, ITEM_data(it),
               it->nbytes);
      } else {
        memcpy(ITEM_data(new_it), ITEM_data(it), it->nbytes);
        memcpy(ITEM_data(new_it) + it->nbytes - 2, ITEM_data(old_it),
               old_it->nbytes);
      }

      it = new_it;
    }

    if (old_it != NULL) {
      item_replace(old_it, it, hv);
    } else {
      do_item_link(it, hv);
    }

    stored = STORED;
  }

  if (old_it != NULL) {
    do_item_remove(old_it);
  }
  if (new_it != NULL) {
    do_item_remove(new_it);
  }
  if (stored == STORED && c != NULL) {
    c->cas = ITEM_get_cas(it);
  }

  return stored;
}

enum delta_result_type do_add_delta(conn *c, const char *key, const size_t nkey,
                                    const bool incr, const int64_t delta,
                                    char *buf, uint64_t *cas,
                                    const uint32_t hv) {
  char *ptr;
  uint64_t value;
  int res;
  item *it = do_item_get(key, nkey, hv);

  if (it == NULL) {
    return DELTA_ITEM_NOT_FOUND;
  }

  if (cas != NULL && *cas != 0 && ITEM_get_cas(it) != *cas) {
    do_item_remove(it);
    return DELTA_ITEM_CAS_MISMATCH;
  }

  ptr = ITEM_data(it);
  if (!safe_strtoull(ptr, &value)) {
    do_item_remove(it);
    return NON_NUMERIC;
  }

  if (incr) {
    value += delta;
  } else {
    value = delta > value ? 0 : value - delta;
  }

  snprintf(buf, INCR_MAX_STORAGE_LEN, "%llu", (unsigned long long)value);
  res = (int)strlen(buf);
  if (res + 2 > it->nbytes || it->refcount != 1) {
    item *new_it =
        do_item_alloc(ITEM_key(it), it->nkey, atoi(ITEM_suffix(it) + 1),
                      it->exptime, res + 2);
    if (new_it == NULL) {
      do_item_remove(it);
      return EOM;
    }

    memcpy(ITEM_data(new_it), buf, (size_t)res);
    memcpy(ITEM_data(new_it) + res, "\r\n", 2);
    item_replace(it, new_it, hv);
    ITEM_set_cas(it, settings.use_cas ? ITEM_get_cas(new_it) : 0);
    do_item_remove(new_it);
  } else {
    mutex_lock(&cache_lock);
    ITEM_set_cas(it, settings.use_cas ? get_cas_id() : 0);
    pthread_mutex_unlock(&cache_lock);
    memcpy(ITEM_data(it), buf, (size_t)res);
    memset(ITEM_data(it) + res, ' ', (size_t)(it->nbytes - res - 2));
    do_item_update(it);
  }

  if (cas != NULL) {
    *cas = ITEM_get_cas(it);
  }
  if (c != NULL) {
    c->cas = ITEM_get_cas(it);
  }
  do_item_remove(it);

  return OK;
}

int main(void) {
  memc_config config = {0};
  MemcStore *store;
  pthread_t threads[NUM_WORKERS];
  worker_args workers[NUM_WORKERS];
  memc_runtime_stats stats_snapshot;
  uint64_t start;
  uint64_t end;
  uint64_t total_ops = 0;
  double duration_ms;
  double throughput;
  size_t i;

  if (ensure_test_traces() != 0) {
    return 1;
  }

  config.expected_items = NUM_OPERATIONS;
  config.worker_count = NUM_WORKERS;
  config.memory_limit_bytes = (size_t)MEMORY_MB * 1024 * 1024;
  config.value_size = VALUE_SIZE;
  config.default_ttl = DEFAULT_TTL;
  config.use_cas = true;

  store = memc_store_new(&config);
  if (store == NULL) {
    fprintf(stderr, "failed to initialize memcached store\n");
    return 1;
  }

  if (populate_store(store) != 0) {
    memc_store_destroy(store);
    return 1;
  }

  start = mstime();
  for (i = 0; i < NUM_WORKERS; ++i) {
    workers[i].thread_id = i;
    workers[i].store = store;
    workers[i].operations_done = 0;
    workers[i].duration_ms = 0.0;
    pthread_create(&threads[i], NULL, benchmark_worker, &workers[i]);
  }

  for (i = 0; i < NUM_WORKERS; ++i) {
    pthread_join(threads[i], NULL);
    total_ops += workers[i].operations_done;
  }
  end = mstime();

  duration_ms = (double)(end - start);
  throughput = duration_ms > 0.0 ? total_ops * 1000.0 / duration_ms : 0.0;

  memc_get_stats(store, &stats_snapshot);
  printf("benchmark total: %" PRIu64 " ops in %.2f ms (%.2f ops/s)\n",
         total_ops, duration_ms, throughput);
  printf("cache stats: items=%" PRIu64 " bytes=%" PRIu64 " evictions=%" PRIu64
         " get_hits=%" PRIu64 " get_misses=%" PRIu64 "\n",
         stats_snapshot.curr_items, stats_snapshot.curr_bytes,
         stats_snapshot.evictions, stats_snapshot.get_hits,
         stats_snapshot.get_misses);

  memc_store_destroy(store);
  return 0;
}
