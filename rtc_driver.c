/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "memcached.h"
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define DEFAULT_DATA_DIR "/home/hjx/data/kv"
#define NUM_SERVERS 32
#define READ_RATIO 50
#define NUM_OPERATIONS 10000000
#define VALUE_SIZE 32

typedef struct {
    size_t ops;
    size_t reads;
    size_t read_hits;
    size_t read_misses;
    size_t writes;
    double duration_ms;
} replay_result;

typedef struct {
    int thread_id;
    int num_operations;
    int read_ratio;
    const char *data_dir;
    replay_result result;
    int rc;
} run_worker;

static uint64_t mstime(void) {
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static void print_peak_rss(void) {
    FILE *file = fopen("/proc/self/status", "r");
    char line[256];

    if (file == NULL) {
        return;
    }

    while (fgets(line, sizeof(line), file) != NULL) {
        if (strncmp(line, "VmHWM:", 6) == 0) {
            printf("Peak memory: %s\n", line);
            break;
        }
    }

    fclose(file);
}

static void fill_default_value(char value[VALUE_SIZE]) {
    int i;

    for (i = 0; i < VALUE_SIZE; i++) {
        value[i] = 'x';
    }
    value[VALUE_SIZE - 1] = '\0';
}

static int format_load_trace_path(char *buffer, size_t size, const char *data_dir,
                                  int num_operations) {
    return snprintf(buffer, size, "%s/load_%d.csv", data_dir, num_operations);
}

static int format_run_trace_path(char *buffer, size_t size, const char *data_dir,
                                 int num_operations, int read_ratio,
                                 int worker_id) {
    return snprintf(buffer, size, "%s/run_%d_%d_%d.csv", data_dir,
                    num_operations, read_ratio, worker_id);
}

static bool rtc_conn_init(conn *connection, int thread_id) {
    memset(connection, 0, sizeof(*connection));
    connection->thread = rtc_thread_get(thread_id);
    connection->protocol = ascii_prot;
    connection->transport = tcp_transport;
    connection->state = conn_new_cmd;
    connection->write_and_go = conn_new_cmd;
    connection->sfd = 0;
    return connection->thread != NULL;
}

static void rtc_note_set_cmd(conn *connection, item *it) {
    pthread_mutex_lock(&connection->thread->stats.mutex);
    connection->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
    pthread_mutex_unlock(&connection->thread->stats.mutex);
}

static bool rtc_store_bytes(conn *connection, int command, const char *key,
                            const void *value, size_t value_len) {
    const size_t key_len = strlen(key);
    item *it;
    enum store_item_type result;

    if (key_len == 0 || key_len > KEY_MAX_LENGTH) {
        return false;
    }

    it = item_alloc((char *)key, key_len, 0, realtime(0), (int)value_len + 2);
    if (it == NULL) {
        return false;
    }

    memcpy(ITEM_data(it), value, value_len);
    memcpy(ITEM_data(it) + value_len, "\r\n", 2);

    rtc_note_set_cmd(connection, it);
    result = store_item(it, command, connection);
    item_remove(it);
    return result == STORED;
}

static bool rtc_read_exists(conn *connection, const char *key) {
    const size_t key_len = strlen(key);
    item *it;
    bool hit;

    if (key_len == 0 || key_len > KEY_MAX_LENGTH) {
        return false;
    }

    it = item_get(key, key_len);
    hit = it != NULL;

    pthread_mutex_lock(&connection->thread->stats.mutex);
    connection->thread->stats.get_cmds++;
    if (hit) {
        connection->thread->stats.slab_stats[it->slabs_clsid].get_hits++;
    } else {
        connection->thread->stats.get_misses++;
    }
    pthread_mutex_unlock(&connection->thread->stats.mutex);

    if (it != NULL) {
        item_remove(it);
    }

    return hit;
}

static bool format_numeric_key(char *buffer, size_t size, const char *text) {
    unsigned long long key = strtoull(text, NULL, 10);
    int written;

    written = snprintf(buffer, size, "%llu", key);
    return written > 0 && (size_t)written < size;
}

static int populate(conn *connection, const char *data_dir, int num_operations,
                    replay_result *result) {
    char file_name[PATH_MAX];
    char line[128];
    char key_buffer[32];
    char value[VALUE_SIZE];
    FILE *populate_fp;
    uint64_t start;
    int cnt = 0;

    if (format_load_trace_path(file_name, sizeof(file_name), data_dir,
                               num_operations) <= 0) {
        fprintf(stderr, "load trace path too long\n");
        return 1;
    }

    populate_fp = fopen(file_name, "r");
    if (populate_fp == NULL) {
        perror("fopen populate file");
        return 1;
    }

    fill_default_value(value);
    start = mstime();

    while (cnt++ < num_operations && fgets(line, sizeof(line), populate_fp)) {
        if (!format_numeric_key(key_buffer, sizeof(key_buffer), line)) {
            fprintf(stderr, "invalid populate key: %s\n", line);
            fclose(populate_fp);
            return 1;
        }

        result->ops++;
        result->writes++;
        if (!rtc_store_bytes(connection, NREAD_ADD, key_buffer, value,
                             VALUE_SIZE)) {
            fprintf(stderr, "failed to populate key %s\n", key_buffer);
            fclose(populate_fp);
            return 1;
        }
    }

    result->duration_ms = (double)(mstime() - start);
    printf("populate time: %.2f ms\n", result->duration_ms);

    fclose(populate_fp);
    return 0;
}

static int replay_run_trace(conn *connection, const char *path,
                            int num_operations, replay_result *result) {
    char line[128];
    char key_buffer[32];
    char value[VALUE_SIZE];
    FILE *benchmark_fp = fopen(path, "r");
    uint64_t start;
    int cnt = 0;

    if (benchmark_fp == NULL) {
        perror("fopen benchmark file");
        return 1;
    }

    fill_default_value(value);
    start = mstime();

    while (cnt++ < num_operations && fgets(line, sizeof(line), benchmark_fp)) {
        char *comma = strchr(line, ',');
        char *operation;
        char *key_text;

        if (comma == NULL) {
            fprintf(stderr, "invalid benchmark line in %s: %s\n", path, line);
            fclose(benchmark_fp);
            return 1;
        }

        *comma = '\0';
        operation = line;
        key_text = comma + 1;

        if (!format_numeric_key(key_buffer, sizeof(key_buffer), key_text)) {
            fprintf(stderr, "invalid benchmark key in %s: %s\n", path, key_text);
            fclose(benchmark_fp);
            return 1;
        }

        if (strcmp(operation, "READ") == 0) {
            result->ops++;
            result->reads++;
            if (rtc_read_exists(connection, key_buffer)) {
                result->read_hits++;
            } else {
                result->read_misses++;
            }
        } else if (strcmp(operation, "UPDATE") == 0) {
            result->ops++;
            result->writes++;
            if (!rtc_store_bytes(connection, NREAD_SET, key_buffer, value,
                                 VALUE_SIZE)) {
                fprintf(stderr, "failed to update key %s\n", key_buffer);
                fclose(benchmark_fp);
                return 1;
            }
        } else {
            fprintf(stderr, "unsupported run op in %s: %s\n", path, operation);
            fclose(benchmark_fp);
            return 1;
        }
    }

    result->duration_ms = (double)(mstime() - start);
    fclose(benchmark_fp);
    return 0;
}

static void accumulate_result(replay_result *dst, const replay_result *src) {
    dst->ops += src->ops;
    dst->reads += src->reads;
    dst->read_hits += src->read_hits;
    dst->read_misses += src->read_misses;
    dst->writes += src->writes;
}

static void *run_worker_main(void *arg) {
    run_worker *worker = arg;
    conn connection;
    char benchmark_fname[PATH_MAX];

    if (!rtc_conn_init(&connection, worker->thread_id)) {
        worker->rc = 1;
        return NULL;
    }

    if (format_run_trace_path(benchmark_fname, sizeof(benchmark_fname),
                              worker->data_dir, worker->num_operations,
                              worker->read_ratio, worker->thread_id) <= 0) {
        fprintf(stderr, "run trace path too long for worker %d\n",
                worker->thread_id);
        worker->rc = 1;
        return NULL;
    }

    worker->rc = replay_run_trace(&connection, benchmark_fname,
                                  worker->num_operations, &worker->result);
    if (worker->rc != 0) {
        worker->rc = 1;
        return NULL;
    }

    printf("Thread %d: benchmark time: %.2f ms\n", worker->thread_id,
           worker->result.duration_ms);
    if (worker->result.duration_ms > 0.0) {
        printf("Thread %d: operation num: %zu, throughput: %.2f ops/s\n",
               worker->thread_id, worker->result.ops,
               worker->result.ops * 1000.0 / worker->result.duration_ms);
    } else {
        printf("Thread %d: operation num: %zu, throughput: inf ops/s\n",
               worker->thread_id, worker->result.ops);
    }

    return NULL;
}

static int run_benchmark(const char *data_dir, int num_operations,
                         int read_ratio, int num_servers) {
    conn load_connection;
    replay_result load_stats = {0};
    replay_result run_stats = {0};
    run_worker *workers;
    pthread_t *threads;
    uint64_t start_bench;
    uint64_t end_bench;
    int created_threads = 0;
    int i;
    int rc = 0;

    if (!rtc_conn_init(&load_connection, 0)) {
        fprintf(stderr, "failed to bind synthetic load connection\n");
        return 1;
    }

    printf("\n=== Population Phase ===\n");
    if (populate(&load_connection, data_dir, num_operations, &load_stats) != 0) {
        return 1;
    }

    workers = calloc((size_t)num_servers, sizeof(*workers));
    threads = calloc((size_t)num_servers, sizeof(*threads));
    if (workers == NULL || threads == NULL) {
        fprintf(stderr, "failed to allocate worker state\n");
        free(workers);
        free(threads);
        return 1;
    }

    printf("\n=== Benchmark Phase ===\n");
    start_bench = mstime();

    for (i = 0; i < num_servers; i++) {
        workers[i].thread_id = i;
        workers[i].num_operations = num_operations;
        workers[i].read_ratio = read_ratio;
        workers[i].data_dir = data_dir;
        if (pthread_create(&threads[i], NULL, run_worker_main, &workers[i]) !=
            0) {
            fprintf(stderr, "failed to create worker thread %d\n", i);
            rc = 1;
            break;
        }
        created_threads++;
    }

    for (i = 0; i < created_threads; i++) {
        pthread_join(threads[i], NULL);
        if (workers[i].rc != 0) {
            rc = 1;
        }
        accumulate_result(&run_stats, &workers[i].result);
    }

    free(workers);
    free(threads);

    if (rc != 0) {
        return 1;
    }

    end_bench = mstime();
    printf("Benchmark phase complete, duration: %.2f ms\n",
           (double)(end_bench - start_bench));
    printf("load: ops=%zu writes=%zu\n", load_stats.ops, load_stats.writes);
    printf("run: ops=%zu reads=%zu hits=%zu misses=%zu writes=%zu\n",
           run_stats.ops, run_stats.reads, run_stats.read_hits,
           run_stats.read_misses, run_stats.writes);
    return 0;
}

static void print_usage(const char *argv0) {
    fprintf(stderr,
            "usage: %s [--data-dir path] [--num-operations n] "
            "[--read-ratio n] [--num-servers n]\n",
            argv0);
}

int main(int argc, char **argv) {
    const char *data_dir = DEFAULT_DATA_DIR;
    int num_operations = NUM_OPERATIONS;
    int read_ratio = READ_RATIO;
    int num_servers = NUM_SERVERS;
    int i;
    int rc;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (strcmp(argv[i], "--num-operations") == 0 && i + 1 < argc) {
            num_operations = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--read-ratio") == 0 && i + 1 < argc) {
            read_ratio = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--num-servers") == 0 && i + 1 < argc) {
            num_servers = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            print_usage(argv[0]);
            return 1;
        }
    }

    if (num_operations <= 0 || read_ratio < 0 || num_servers <= 0) {
        fprintf(stderr, "invalid benchmark configuration\n");
        return 1;
    }

    rtc_settings_init();
    rtc_stats_init();
    rtc_update_time();

    assoc_init(settings.hashpower_init);
    slabs_init(settings.maxbytes, settings.factor, false);
    thread_init(num_servers, NULL);

    if (start_assoc_maintenance_thread() != 0) {
        fprintf(stderr, "failed to start assoc maintenance thread\n");
        return 1;
    }

    rc = run_benchmark(data_dir, num_operations, read_ratio, num_servers);
    stop_assoc_maintenance_thread();

    printf("\nCleaning up...\n");
    printf("Done!\n");
    print_peak_rss();

    return rc;
}
