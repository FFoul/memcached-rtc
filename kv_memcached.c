#include "kv_memcached.h"

#ifndef RTC_BENCHMARK
#define RTC_BENCHMARK 1
#endif
#include "memcached.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
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

struct MemcStore {
    size_t value_size;
    uint32_t default_ttl;
};

volatile rel_time_t current_time;
struct stats stats;
struct settings settings;

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
    time_t now;

    now = time(NULL);
    current_time = (rel_time_t)(now - stats.started);
}

static rel_time_t realtime(const time_t exptime) {
    if (exptime == 0) {
        return 0;
    }

    if (exptime > REALTIME_MAXDELTA) {
        if (exptime <= stats.started) {
            return (rel_time_t)1;
        }
        return (rel_time_t)(exptime - stats.started);
    }

    return (rel_time_t)(exptime + current_time);
}

static void init_stats(void) {
    memset(&stats, 0, sizeof(stats));
    stats.accepting_conns = 1;
    stats.started = time(0) - 2;
    stats_prefix_init();
}

static void init_settings(void) {
    memset(&settings, 0, sizeof(settings));
    settings.access = 0700;
    settings.port = 11211;
    settings.udpport = 11211;
    settings.maxbytes = (size_t)MEMORY_MB * 1024 * 1024;
    settings.maxconns = 1024;
    settings.oldest_live = 0;
    settings.evict_to_free = 1;
    settings.factor = 1.25;
    settings.chunk_size = 48;
    settings.num_threads = NUM_WORKERS;
    settings.prefix_delimiter = ':';
    settings.detail_enabled = 0;
    settings.reqs_per_event = 20;
    settings.backlog = 1024;
}

static void fill_keybuf(uint64_t key, char *keybuf, size_t keybuf_size,
                        size_t *nkey_out) {
    int written;

    written = snprintf(keybuf, keybuf_size, "%" PRIu64, key);
    if (written < 0 || (size_t)written >= keybuf_size) {
        keybuf[0] = '\0';
        *nkey_out = 0;
        return;
    }

    *nkey_out = (size_t)written;
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

static char *make_value(char fill, size_t len) {
    char *value;

    value = malloc(len + 1);
    if (value == NULL) {
        return NULL;
    }

    memset(value, fill, len);
    value[len] = '\0';
    return value;
}

static void copy_stats(memc_runtime_stats *out) {
    STATS_LOCK();
    out->curr_items = stats.curr_items;
    out->total_items = stats.total_items;
    out->curr_bytes = stats.curr_bytes;
    out->get_cmds = stats.get_cmds;
    out->set_cmds = stats.set_cmds;
    out->get_hits = stats.get_hits;
    out->get_misses = stats.get_misses;
    out->flush_cmds = stats.flush_cmds;
    out->evictions = stats.evictions;
    STATS_UNLOCK();
}

static memc_result from_store_result(int result) {
    switch (result) {
    case 1:
        return MEMC_OK;
    case 2:
        return MEMC_EXISTS;
    case 3:
        return MEMC_NOT_FOUND;
    default:
        return MEMC_NOT_STORED;
    }
}

static memc_result store_value(MemcStore *store, uint64_t key,
                               const char *value, size_t value_len,
                               uint32_t ttl, int mode, uint64_t cas_in,
                               uint64_t *cas_out) {
    char keybuf[32];
    size_t nkey;
    item *it;
    int result;

    if (store == NULL || value == NULL || value_len == 0 ||
        value_len > INT_MAX - 2) {
        return MEMC_INVALID;
    }

    refresh_current_time();
    fill_keybuf(key, keybuf, sizeof(keybuf), &nkey);
    if (nkey == 0) {
        return MEMC_INVALID;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(keybuf);
    }

    it = item_alloc(keybuf, nkey, 0, realtime(ttl), (int)value_len + 2);
    if (it == NULL) {
        return MEMC_NO_MEMORY;
    }

    memcpy(ITEM_data(it), value, value_len);
    memcpy(ITEM_data(it) + value_len, "\r\n", 2);
    if (mode == NREAD_CAS) {
        it->cas_id = cas_in;
    }

    STATS_LOCK();
    stats.set_cmds++;
    STATS_UNLOCK();

    result = store_item(it, mode);
    if (result == 1 && cas_out != NULL) {
        *cas_out = it->cas_id;
    }
    item_remove(it);

    return from_store_result(result);
}

static memc_result delta_value(MemcStore *store, uint64_t key, uint64_t delta,
                               bool incr, uint64_t *value_out) {
    char keybuf[32];
    size_t nkey;
    char buf[32];
    char *resp;
    conn c;
    item *it;

    if (store == NULL) {
        return MEMC_INVALID;
    }

    refresh_current_time();
    fill_keybuf(key, keybuf, sizeof(keybuf), &nkey);
    if (nkey == 0) {
        return MEMC_INVALID;
    }

    memset(&c, 0, sizeof(c));
    it = item_get(keybuf, nkey);
    if (it == NULL) {
        return MEMC_NOT_FOUND;
    }

    resp = add_delta(&c, it, incr, (int64_t)delta, buf);
    item_remove(it);

    if (resp == buf) {
        if (value_out != NULL) {
            *value_out = strtoull(buf, NULL, 10);
        }
        return MEMC_OK;
    }

    if (strstr(resp, "non-numeric") != NULL) {
        return MEMC_NON_NUMERIC;
    }
    if (strstr(resp, "out of memory") != NULL) {
        return MEMC_NO_MEMORY;
    }

    return MEMC_NOT_STORED;
}

static int populate_store(MemcStore *store) {
    char path[PATH_MAX];
    char line[128];
    char *value;
    FILE *fp;
    size_t count;
    uint64_t start;
    uint64_t end;

    get_load_path(path, sizeof(path));
    fp = fopen(path, "r");
    if (fp == NULL) {
        perror("fopen load trace");
        return -1;
    }

    value = make_value('x', VALUE_SIZE);
    if (value == NULL) {
        fclose(fp);
        return -1;
    }

    count = 0;
    start = mstime();
    while (count < NUM_OPERATIONS && fgets(line, sizeof(line), fp) != NULL) {
        uint64_t key;

        key = strtoull(line, NULL, 10);
        if (memc_set(store, key, value, VALUE_SIZE, DEFAULT_TTL, NULL) !=
            MEMC_OK) {
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
    worker_args *worker;
    char path[PATH_MAX];
    char line[256];
    char *value;
    char *read_value;
    FILE *fp;
    uint64_t start;
    uint64_t end;
    double throughput;

    worker = arg;
    get_run_path(path, sizeof(path), worker->thread_id);
    fp = fopen(path, "r");
    if (fp == NULL) {
        perror("fopen benchmark trace");
        return NULL;
    }

    value = make_value('y', VALUE_SIZE);
    read_value = malloc(VALUE_SIZE + 1);
    if (value == NULL || read_value == NULL) {
        free(value);
        free(read_value);
        fclose(fp);
        return NULL;
    }

    start = mstime();
    while (fgets(line, sizeof(line), fp) != NULL) {
        char *comma;
        uint64_t key;

        comma = strchr(line, ',');
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

static int expect_result(const char *name, memc_result got, memc_result want) {
    if (got != want) {
        fprintf(stderr, "%s failed: got %d want %d\n", name, got, want);
        return -1;
    }
    return 0;
}

static int run_basic_checks(MemcStore *store) {
    char value[128];
    size_t value_len;
    uint64_t cas;
    uint64_t next_cas;
    uint64_t number;
    memc_result rc;

    rc = memc_set(store, 9000001, "alpha", 5, 0, &cas);
    if (expect_result("set", rc, MEMC_OK) != 0) {
        return -1;
    }

    rc = memc_get(store, 9000001, value, sizeof(value), &value_len, &next_cas);
    if (expect_result("get", rc, MEMC_OK) != 0) {
        return -1;
    }
    if (value_len != 5 || strcmp(value, "alpha") != 0 || next_cas != cas) {
        fprintf(stderr, "get verification failed\n");
        return -1;
    }

    rc = memc_add(store, 9000001, "beta", 4, 0, NULL);
    if (expect_result("add-existing", rc, MEMC_NOT_STORED) != 0) {
        return -1;
    }

    rc = memc_replace(store, 9000001, "beta", 4, 0, NULL);
    if (expect_result("replace", rc, MEMC_OK) != 0) {
        return -1;
    }

    rc = memc_append(store, 9000001, "42", 2, NULL);
    if (expect_result("append", rc, MEMC_OK) != 0) {
        return -1;
    }

    rc = memc_prepend(store, 9000001, "id-", 3, NULL);
    if (expect_result("prepend", rc, MEMC_OK) != 0) {
        return -1;
    }

    rc = memc_get(store, 9000001, value, sizeof(value), &value_len, &cas);
    if (expect_result("get-after-append", rc, MEMC_OK) != 0) {
        return -1;
    }
    if (strcmp(value, "id-beta42") != 0) {
        fprintf(stderr, "append/prepend verification failed: %s\n", value);
        return -1;
    }

    rc = memc_cas(store, 9000001, "gamma", 5, 0, cas, &next_cas);
    if (expect_result("cas", rc, MEMC_OK) != 0) {
        return -1;
    }

    rc = memc_set(store, 9000002, "10", 2, 0, NULL);
    if (expect_result("set-numeric", rc, MEMC_OK) != 0) {
        return -1;
    }

    rc = memc_incr(store, 9000002, 5, &number);
    if (expect_result("incr", rc, MEMC_OK) != 0 || number != 15) {
        fprintf(stderr, "incr verification failed\n");
        return -1;
    }

    rc = memc_decr(store, 9000002, 20, &number);
    if (expect_result("decr", rc, MEMC_OK) != 0 || number != 0) {
        fprintf(stderr, "decr verification failed\n");
        return -1;
    }

    rc = memc_delete(store, 9000001);
    if (expect_result("delete", rc, MEMC_OK) != 0) {
        return -1;
    }

    rc = memc_get(store, 9000001, value, sizeof(value), NULL, NULL);
    if (expect_result("get-after-delete", rc, MEMC_NOT_FOUND) != 0) {
        return -1;
    }

    printf("basic checks: passed\n");
    return 0;
}

static int run_trace_case(MemcStore *store, size_t worker_count) {
    pthread_t threads[NUM_WORKERS];
    worker_args workers[NUM_WORKERS];
    uint64_t start;
    uint64_t end;
    uint64_t total_ops;
    double duration_ms;
    double throughput;
    size_t i;

    if (populate_store(store) != 0) {
        return -1;
    }

    total_ops = 0;
    start = mstime();
    for (i = 0; i < worker_count; ++i) {
        workers[i].thread_id = i;
        workers[i].store = store;
        workers[i].operations_done = 0;
        workers[i].duration_ms = 0.0;
        if (pthread_create(&threads[i], NULL, benchmark_worker, &workers[i]) !=
            0) {
            perror("pthread_create");
            return -1;
        }
    }

    for (i = 0; i < worker_count; ++i) {
        pthread_join(threads[i], NULL);
        total_ops += workers[i].operations_done;
    }
    end = mstime();

    duration_ms = (double)(end - start);
    throughput = duration_ms > 0.0 ? total_ops * 1000.0 / duration_ms : 0.0;
    printf("trace workers=%zu: %" PRIu64 " ops in %.2f ms (%.2f ops/s)\n",
           worker_count, total_ops, duration_ms, throughput);

    return 0;
}

MemcStore *memc_store_new(void) {
    MemcStore *store;

    store = calloc(1, sizeof(*store));
    if (store == NULL) {
        return NULL;
    }

    store->value_size = VALUE_SIZE;
    store->default_ttl = DEFAULT_TTL;

    init_stats();
    init_settings();
    refresh_current_time();
    item_init();
    assoc_init();
    slabs_init(settings.maxbytes, settings.factor, false);
    thread_init(settings.num_threads, NULL);

    return store;
}

void memc_store_destroy(MemcStore *store) {
    if (store == NULL) {
        return;
    }

    free(store);
}

memc_result memc_get(MemcStore *store, uint64_t key, char *value_out,
                     size_t value_cap, size_t *value_len_out,
                     uint64_t *cas_out) {
    char keybuf[32];
    size_t nkey;
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

    STATS_LOCK();
    stats.get_cmds++;
    STATS_UNLOCK();

    it = item_get(keybuf, nkey);
    if (it == NULL) {
        if (settings.detail_enabled) {
            stats_prefix_record_get(keybuf, false);
        }

        STATS_LOCK();
        stats.get_misses++;
        STATS_UNLOCK();
        return MEMC_NOT_FOUND;
    }

    payload = (size_t)it->nbytes - 2;
    if (value_out != NULL && value_cap > 0) {
        size_t copied;

        copied = payload < value_cap - 1 ? payload : value_cap - 1;
        memcpy(value_out, ITEM_data(it), copied);
        value_out[copied] = '\0';
    }
    if (value_len_out != NULL) {
        *value_len_out = payload;
    }
    if (cas_out != NULL) {
        *cas_out = it->cas_id;
    }
    if (settings.detail_enabled) {
        stats_prefix_record_get(keybuf, true);
    }

    item_update(it);
    item_remove(it);

    STATS_LOCK();
    stats.get_hits++;
    STATS_UNLOCK();

    return MEMC_OK;
}

memc_result memc_set(MemcStore *store, uint64_t key, const char *value,
                     size_t value_len, uint32_t ttl, uint64_t *cas_out) {
    return store_value(store, key, value, value_len, ttl, NREAD_SET, 0,
                       cas_out);
}

memc_result memc_add(MemcStore *store, uint64_t key, const char *value,
                     size_t value_len, uint32_t ttl, uint64_t *cas_out) {
    return store_value(store, key, value, value_len, ttl, NREAD_ADD, 0,
                       cas_out);
}

memc_result memc_replace(MemcStore *store, uint64_t key, const char *value,
                         size_t value_len, uint32_t ttl, uint64_t *cas_out) {
    return store_value(store, key, value, value_len, ttl, NREAD_REPLACE, 0,
                       cas_out);
}

memc_result memc_append(MemcStore *store, uint64_t key, const char *value,
                        size_t value_len, uint64_t *cas_out) {
    return store_value(store, key, value, value_len, 0, NREAD_APPEND, 0,
                       cas_out);
}

memc_result memc_prepend(MemcStore *store, uint64_t key, const char *value,
                         size_t value_len, uint64_t *cas_out) {
    return store_value(store, key, value, value_len, 0, NREAD_PREPEND, 0,
                       cas_out);
}

memc_result memc_cas(MemcStore *store, uint64_t key, const char *value,
                     size_t value_len, uint32_t ttl, uint64_t expected_cas,
                     uint64_t *new_cas_out) {
    return store_value(store, key, value, value_len, ttl, NREAD_CAS,
                       expected_cas, new_cas_out);
}

memc_result memc_delete(MemcStore *store, uint64_t key) {
    char keybuf[32];
    size_t nkey;
    item *it;

    if (store == NULL) {
        return MEMC_INVALID;
    }

    refresh_current_time();
    fill_keybuf(key, keybuf, sizeof(keybuf), &nkey);
    if (nkey == 0) {
        return MEMC_INVALID;
    }

    it = item_get(keybuf, nkey);
    if (it == NULL) {
        return MEMC_NOT_FOUND;
    }

    item_unlink(it);
    item_remove(it);
    if (settings.detail_enabled) {
        stats_prefix_record_delete(keybuf);
    }

    return MEMC_OK;
}

memc_result memc_incr(MemcStore *store, uint64_t key, uint64_t delta,
                      uint64_t *value_out) {
    return delta_value(store, key, delta, true, value_out);
}

memc_result memc_decr(MemcStore *store, uint64_t key, uint64_t delta,
                      uint64_t *value_out) {
    return delta_value(store, key, delta, false, value_out);
}

void memc_flush_all(MemcStore *store, uint32_t ttl) {
    if (store == NULL) {
        return;
    }

    refresh_current_time();
    STATS_LOCK();
    stats.flush_cmds++;
    STATS_UNLOCK();

    if (ttl > 0) {
        settings.oldest_live = realtime(ttl) - 1;
    } else {
        settings.oldest_live = current_time - 1;
    }
    item_flush_expired();
}

void memc_get_stats(MemcStore *store, memc_runtime_stats *out) {
    if (store == NULL || out == NULL) {
        return;
    }

    copy_stats(out);
}

/*
 * Stores an item in the cache according to the semantics of one of the set
 * commands. In threaded mode, this is protected by the cache lock.
 *
 * Returns true if the item was stored.
 */
int do_store_item(item *it, int comm) {
    char *key = ITEM_key(it);
    bool delete_locked = false;
    item *old_it = do_item_get_notedeleted(key, it->nkey, &delete_locked);
    int stored = 0;

    item *new_it = NULL;
    int flags;

    if (old_it != NULL && comm == NREAD_ADD) {
        /* add only adds a nonexistent item, but promote to head of LRU */
        do_item_update(old_it);
    } else if (!old_it && (comm == NREAD_REPLACE
        || comm == NREAD_APPEND || comm == NREAD_PREPEND))
    {
        /* replace only replaces an existing value; don't store */
    } else if (delete_locked && (comm == NREAD_REPLACE || comm == NREAD_ADD
        || comm == NREAD_APPEND || comm == NREAD_PREPEND))
    {
        /* replace and add can't override delete locks; don't store */
    } else if (comm == NREAD_CAS) {
        /* validate cas operation */
        if (delete_locked)
            old_it = do_item_get_nocheck(key, it->nkey);

        if(old_it == NULL) {
          stored = 3;
        }
        else if(it->cas_id == old_it->cas_id) {
          do_item_replace(old_it, it);
          stored = 1;
        }
        else
        {
          stored = 2;
        }
    } else {
        if (comm == NREAD_APPEND || comm == NREAD_PREPEND) {
            flags = (int) strtol(ITEM_suffix(old_it), (char **) NULL, 10);

            new_it = do_item_alloc(key, it->nkey, flags, old_it->exptime,
                                   it->nbytes + old_it->nbytes - 2);

            if (new_it == NULL) {
                if (old_it != NULL)
                    do_item_remove(old_it);

                return 0;
            }

            if (comm == NREAD_APPEND) {
                memcpy(ITEM_data(new_it), ITEM_data(old_it), old_it->nbytes);
                memcpy(ITEM_data(new_it) + old_it->nbytes - 2,
                       ITEM_data(it), it->nbytes);
            } else {
                memcpy(ITEM_data(new_it), ITEM_data(it), it->nbytes);
                memcpy(ITEM_data(new_it) + it->nbytes - 2,
                       ITEM_data(old_it), old_it->nbytes);
            }

            it = new_it;
        }

        if (delete_locked)
            old_it = do_item_get_nocheck(key, it->nkey);

        if (old_it != NULL)
            do_item_replace(old_it, it);
        else
            do_item_link(it);

        stored = 1;
    }

    if (old_it != NULL)
        do_item_remove(old_it);
    if (new_it != NULL)
        do_item_remove(new_it);

    return stored;
}

/*
 * adds a delta value to a numeric item.
 *
 * c     connection requesting the operation
 * it    item to adjust
 * incr  true to increment value, false to decrement
 * delta amount to adjust value by
 * buf   buffer for response string
 *
 * returns a response string to send back to the client.
 */
char *do_add_delta(conn *c, item *it, const bool incr, const int64_t delta,
                   char *buf) {
    char *ptr;
    uint64_t value;
    int res;

    ptr = ITEM_data(it);
    while ((*ptr != '\0') && (*ptr < '0' && *ptr > '9')) ptr++;

    value = strtoull(ptr, NULL, 10);

    if(errno == ERANGE) {
        return "CLIENT_ERROR cannot increment or decrement non-numeric value";
    }

    if (incr) {
        value += delta;
        MEMCACHED_COMMAND_INCR(c->sfd, ITEM_key(it), value);
    } else {
        if (delta >= value) value = 0;
        else value -= delta;
        MEMCACHED_COMMAND_DECR(c->sfd, ITEM_key(it), value);
    }
    sprintf(buf, "%llu", value);
    res = strlen(buf);
    if (res + 2 > it->nbytes) {
        item *new_it;
        new_it = do_item_alloc(ITEM_key(it), it->nkey,
                               atoi(ITEM_suffix(it) + 1), it->exptime,
                               res + 2 );
        if (new_it == 0) {
            return "SERVER_ERROR out of memory in incr/decr";
        }
        memcpy(ITEM_data(new_it), buf, res);
        memcpy(ITEM_data(new_it) + res, "\r\n", 2);
        do_item_replace(it, new_it);
        do_item_remove(new_it);
    } else {
        it->cas_id = get_cas_id();

        memcpy(ITEM_data(it), buf, res);
        memset(ITEM_data(it) + res, ' ', it->nbytes - res - 2);
    }

    return buf;
}

int kv_memcached_run(void) {
    MemcStore *store;
    memc_runtime_stats stats_snapshot;

    if (ensure_test_traces() != 0) {
        return 1;
    }

    store = memc_store_new();
    if (store == NULL) {
        fprintf(stderr, "failed to initialize memcached store\n");
        return 1;
    }

    if (run_basic_checks(store) != 0) {
        memc_store_destroy(store);
        return 1;
    }

    if (run_trace_case(store, 1) != 0 ||
        run_trace_case(store, 2) != 0 ||
        run_trace_case(store, 4) != 0) {
        memc_store_destroy(store);
        return 1;
    }

    memc_get_stats(store, &stats_snapshot);
    printf("cache stats: items=%" PRIu64 " bytes=%" PRIu64
           " evictions=%" PRIu64 " get_hits=%" PRIu64
           " get_misses=%" PRIu64 "\n",
           stats_snapshot.curr_items, stats_snapshot.curr_bytes,
           stats_snapshot.evictions, stats_snapshot.get_hits,
           stats_snapshot.get_misses);

    memc_store_destroy(store);
    return 0;
}

int main(void) {
    return kv_memcached_run();
}
