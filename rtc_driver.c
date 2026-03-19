/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "memcached.h"
#include <errno.h>
#include <glob.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/time.h>

#ifndef PATH_MAX
#define PATH_MAX 1024
#endif

#define DEFAULT_LOAD_TRACE "testdata/load_8.csv"
#define DEFAULT_RUN_PREFIX "testdata/run_8_50"
#define VALUE_BUFFER_SIZE 4096

typedef struct {
    size_t ops;
    size_t reads;
    size_t read_hits;
    size_t read_misses;
    size_t writes;
    size_t deletes;
    double seconds;
    int status;
} replay_result;

typedef struct {
    int thread_id;
    char trace_path[PATH_MAX];
    replay_result result;
} worker_arg;

static double rtc_now_seconds(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

static char *trim(char *text) {
    char *end;

    while (*text == ' ' || *text == '\t' || *text == '\r' || *text == '\n') {
        text++;
    }

    end = text + strlen(text);
    while (end > text &&
           (end[-1] == ' ' || end[-1] == '\t' ||
            end[-1] == '\r' || end[-1] == '\n')) {
        end--;
    }
    *end = '\0';
    return text;
}

static bool rtc_conn_init(conn *connection, int thread_id) {
    memset(connection, 0, sizeof(*connection));
    connection->thread = rtc_thread_get(thread_id);
    connection->protocol = ascii_prot;
    connection->transport = tcp_transport;
    connection->state = conn_new_cmd;
    connection->write_and_go = conn_new_cmd;
    connection->sfd = thread_id;
    return connection->thread != NULL;
}

static void rtc_note_set_cmd(conn *connection, item *it) {
    pthread_mutex_lock(&connection->thread->stats.mutex);
    connection->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
    pthread_mutex_unlock(&connection->thread->stats.mutex);
}

static enum store_item_type rtc_store_op(conn *connection, int command,
                                         const char *key, const char *value,
                                         uint32_t flags, time_t exptime,
                                         uint64_t cas) {
    size_t key_len = strlen(key);
    size_t value_len = strlen(value);
    item *it;
    enum store_item_type result;

    rtc_update_time();

    if (key_len > KEY_MAX_LENGTH) {
        return NOT_STORED;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, key_len);
    }

    it = item_alloc((char *)key, key_len, (int)flags, realtime(exptime),
                    (int)value_len + 2);
    if (it == NULL) {
        if (command == NREAD_SET) {
            item *old_it = item_get(key, key_len);
            if (old_it != NULL) {
                item_unlink(old_it);
                item_remove(old_it);
            }
        }
        return NOT_STORED;
    }

    memcpy(ITEM_data(it), value, value_len);
    memcpy(ITEM_data(it) + value_len, "\r\n", 2);
    ITEM_set_cas(it, cas);

    rtc_note_set_cmd(connection, it);
    result = store_item(it, command, connection);
    item_remove(it);
    return result;
}

static bool rtc_get_value(conn *connection, const char *key,
                          char *value_out, size_t value_out_size,
                          uint64_t *cas_out) {
    size_t key_len = strlen(key);
    item *it;
    size_t stored_len;

    rtc_update_time();

    it = item_get(key, key_len);
    if (settings.detail_enabled) {
        stats_prefix_record_get(key, key_len, it != NULL);
    }

    pthread_mutex_lock(&connection->thread->stats.mutex);
    connection->thread->stats.get_cmds++;
    if (it != NULL) {
        connection->thread->stats.slab_stats[it->slabs_clsid].get_hits++;
    } else {
        connection->thread->stats.get_misses++;
    }
    pthread_mutex_unlock(&connection->thread->stats.mutex);

    if (it == NULL) {
        return false;
    }

    stored_len = it->nbytes >= 2 ? (size_t)it->nbytes - 2 : 0;
    if (stored_len + 1 > value_out_size) {
        item_remove(it);
        return false;
    }

    memcpy(value_out, ITEM_data(it), stored_len);
    value_out[stored_len] = '\0';
    if (cas_out != NULL) {
        *cas_out = ITEM_get_cas(it);
    }

    item_update(it);
    item_remove(it);
    return true;
}

static bool rtc_delete_op(conn *connection, const char *key) {
    size_t key_len = strlen(key);
    item *it;

    rtc_update_time();

    if (settings.detail_enabled) {
        stats_prefix_record_delete(key, key_len);
    }

    it = item_get(key, key_len);
    if (it != NULL) {
        pthread_mutex_lock(&connection->thread->stats.mutex);
        connection->thread->stats.slab_stats[it->slabs_clsid].delete_hits++;
        pthread_mutex_unlock(&connection->thread->stats.mutex);

        item_unlink(it);
        item_remove(it);
        return true;
    }

    pthread_mutex_lock(&connection->thread->stats.mutex);
    connection->thread->stats.delete_misses++;
    pthread_mutex_unlock(&connection->thread->stats.mutex);
    return false;
}

static enum delta_result_type rtc_arithmetic_op(conn *connection, const char *key,
                                                bool incr, uint64_t delta,
                                                uint64_t *value_out) {
    char buffer[INCR_MAX_STORAGE_LEN];
    enum delta_result_type result;

    rtc_update_time();
    result = add_delta(connection, key, strlen(key), incr, (int64_t)delta,
                       buffer, NULL);
    if (result == OK && value_out != NULL) {
        *value_out = strtoull(buffer, NULL, 10);
    } else if (result == DELTA_ITEM_NOT_FOUND) {
        pthread_mutex_lock(&connection->thread->stats.mutex);
        if (incr) {
            connection->thread->stats.incr_misses++;
        } else {
            connection->thread->stats.decr_misses++;
        }
        pthread_mutex_unlock(&connection->thread->stats.mutex);
    }

    return result;
}

static void rtc_flush_all(conn *connection, time_t exptime) {
    rtc_update_time();

    pthread_mutex_lock(&connection->thread->stats.mutex);
    connection->thread->stats.flush_cmds++;
    pthread_mutex_unlock(&connection->thread->stats.mutex);

    if (exptime > 0) {
        settings.oldest_live = realtime(exptime) - 1;
    } else {
        settings.oldest_live = current_time - 1;
    }
    item_flush_expired();
}

static bool rtc_expect_string(const char *label, const char *actual,
                              const char *expected) {
    if (strcmp(actual, expected) != 0) {
        fprintf(stderr, "self-check failed: %s expected '%s' got '%s'\n",
                label, expected, actual);
        return false;
    }
    return true;
}

static bool rtc_self_check(void) {
    conn connection;
    char value[VALUE_BUFFER_SIZE];
    uint64_t cas = 0;
    uint64_t numeric_value = 0;

    if (!rtc_conn_init(&connection, 0)) {
        fprintf(stderr, "self-check failed: could not bind synthetic conn\n");
        return false;
    }

    if (rtc_store_op(&connection, NREAD_SET, "alpha", "1", 0, 0, 0) != STORED) {
        fprintf(stderr, "self-check failed: set alpha\n");
        return false;
    }
    if (!rtc_get_value(&connection, "alpha", value, sizeof(value), &cas) ||
        !rtc_expect_string("get alpha", value, "1")) {
        return false;
    }
    if (rtc_store_op(&connection, NREAD_ADD, "alpha", "2", 0, 0, 0) != NOT_STORED) {
        fprintf(stderr, "self-check failed: add existing alpha\n");
        return false;
    }
    if (rtc_store_op(&connection, NREAD_REPLACE, "alpha", "2", 0, 0, 0) != STORED ||
        !rtc_get_value(&connection, "alpha", value, sizeof(value), NULL) ||
        !rtc_expect_string("replace alpha", value, "2")) {
        return false;
    }
    if (rtc_store_op(&connection, NREAD_APPEND, "alpha", "3", 0, 0, 0) != STORED ||
        !rtc_get_value(&connection, "alpha", value, sizeof(value), NULL) ||
        !rtc_expect_string("append alpha", value, "23")) {
        return false;
    }
    if (rtc_store_op(&connection, NREAD_PREPEND, "alpha", "1", 0, 0, 0) != STORED ||
        !rtc_get_value(&connection, "alpha", value, sizeof(value), &cas) ||
        !rtc_expect_string("prepend alpha", value, "123")) {
        return false;
    }
    if (rtc_store_op(&connection, NREAD_CAS, "alpha", "777", 0, 0, cas) != STORED ||
        !rtc_get_value(&connection, "alpha", value, sizeof(value), NULL) ||
        !rtc_expect_string("cas alpha", value, "777")) {
        return false;
    }
    if (rtc_store_op(&connection, NREAD_CAS, "alpha", "999", 0, 0, cas) != EXISTS) {
        fprintf(stderr, "self-check failed: cas mismatch should return EXISTS\n");
        return false;
    }
    if (rtc_store_op(&connection, NREAD_SET, "counter", "10", 0, 0, 0) != STORED) {
        fprintf(stderr, "self-check failed: set counter\n");
        return false;
    }
    if (rtc_arithmetic_op(&connection, "counter", true, 5, &numeric_value) != OK ||
        numeric_value != 15) {
        fprintf(stderr, "self-check failed: incr counter\n");
        return false;
    }
    if (rtc_arithmetic_op(&connection, "counter", false, 20, &numeric_value) != OK ||
        numeric_value != 0) {
        fprintf(stderr, "self-check failed: decr counter\n");
        return false;
    }
    if (!rtc_delete_op(&connection, "alpha")) {
        fprintf(stderr, "self-check failed: delete alpha\n");
        return false;
    }
    if (rtc_get_value(&connection, "alpha", value, sizeof(value), NULL)) {
        fprintf(stderr, "self-check failed: alpha should be missing after delete\n");
        return false;
    }
    if (rtc_store_op(&connection, NREAD_SET, "flushme", "gone", 0, 0, 0) != STORED) {
        fprintf(stderr, "self-check failed: set flushme\n");
        return false;
    }
    rtc_flush_all(&connection, 0);
    rtc_advance_time(1);
    item_flush_expired();
    if (rtc_get_value(&connection, "flushme", value, sizeof(value), NULL)) {
        fprintf(stderr, "self-check failed: flush_all did not expire flushme\n");
        return false;
    }

    return true;
}

static int parse_csv_fields(char *line, char **op, char **key, char **value) {
    char *saveptr = NULL;
    char *fields[3];
    int count = 0;
    char *token;

    token = strtok_r(line, ",", &saveptr);
    while (token != NULL && count < 3) {
        fields[count++] = trim(token);
        token = strtok_r(NULL, ",", &saveptr);
    }

    if (count == 0) {
        return 0;
    }

    *op = fields[0];
    *key = count > 1 ? fields[1] : (char *)"";
    *value = count > 2 ? fields[2] : (char *)"";
    return count;
}

static int replay_trace_file(conn *connection, const char *path, replay_result *result) {
    FILE *file = fopen(path, "r");
    char *line = NULL;
    size_t line_cap = 0;
    ssize_t line_len;

    if (file == NULL) {
        perror(path);
        return -1;
    }

    while ((line_len = getline(&line, &line_cap, file)) != -1) {
        char *cursor = trim(line);
        char *op = NULL;
        char *key = NULL;
        char *value = NULL;

        if (line_len == 0 || cursor[0] == '\0' || cursor[0] == '#') {
            continue;
        }

        if (parse_csv_fields(cursor, &op, &key, &value) == 0) {
            continue;
        }

        if (strcasecmp(op, "READ") == 0 || strcasecmp(op, "GET") == 0) {
            char read_buffer[VALUE_BUFFER_SIZE];
            bool hit = rtc_get_value(connection, key, read_buffer,
                                     sizeof(read_buffer), NULL);
            result->ops++;
            result->reads++;
            if (hit) {
                result->read_hits++;
            } else {
                result->read_misses++;
            }
        } else if (strcasecmp(op, "UPDATE") == 0 || strcasecmp(op, "SET") == 0) {
            result->ops++;
            result->writes++;
            (void)rtc_store_op(connection, NREAD_SET, key, value, 0, 0, 0);
        } else if (strcasecmp(op, "INSERT") == 0 || strcasecmp(op, "ADD") == 0) {
            result->ops++;
            result->writes++;
            (void)rtc_store_op(connection, NREAD_ADD, key, value, 0, 0, 0);
        } else if (strcasecmp(op, "DELETE") == 0) {
            result->ops++;
            result->deletes++;
            (void)rtc_delete_op(connection, key);
        } else {
            fprintf(stderr, "unsupported trace op in %s: %s\n", path, op);
            free(line);
            fclose(file);
            return -1;
        }
    }

    free(line);
    fclose(file);
    return 0;
}

static void *run_worker(void *arg) {
    worker_arg *worker = (worker_arg *)arg;
    conn connection;
    double start;
    double end;

    if (!rtc_conn_init(&connection, worker->thread_id)) {
        worker->result.status = -1;
        return NULL;
    }

    start = rtc_now_seconds();
    worker->result.status = replay_trace_file(&connection, worker->trace_path,
                                              &worker->result);
    end = rtc_now_seconds();
    worker->result.seconds = end - start;
    return NULL;
}

static int detect_thread_count(const char *run_prefix) {
    char pattern[PATH_MAX];
    glob_t matches;
    int count;

    snprintf(pattern, sizeof(pattern), "%s_*.csv", run_prefix);
    memset(&matches, 0, sizeof(matches));
    if (glob(pattern, 0, NULL, &matches) != 0) {
        globfree(&matches);
        return -1;
    }

    count = (int)matches.gl_pathc;
    globfree(&matches);
    return count;
}

static int run_replay(const char *load_trace, const char *run_prefix, int thread_count) {
    conn load_connection;
    pthread_t *threads = NULL;
    worker_arg *workers = NULL;
    replay_result load_stats = {0};
    replay_result run_total = {0};
    double wall_start;
    double wall_end;
    int rc = 0;
    int i;

    if (!rtc_conn_init(&load_connection, 0)) {
        fprintf(stderr, "failed to bind load connection\n");
        return 1;
    }

    printf("self-check: running\n");
    if (!rtc_self_check()) {
        return 1;
    }
    printf("self-check: ok\n");

    rtc_flush_all(&load_connection, 0);
    rtc_advance_time(1);
    item_flush_expired();

    printf("load trace: %s\n", load_trace);
    if (replay_trace_file(&load_connection, load_trace, &load_stats) != 0) {
        return 1;
    }

    threads = calloc((size_t)thread_count, sizeof(pthread_t));
    workers = calloc((size_t)thread_count, sizeof(worker_arg));
    if (threads == NULL || workers == NULL) {
        perror("allocating worker state");
        free(threads);
        free(workers);
        return 1;
    }

    wall_start = rtc_now_seconds();
    for (i = 0; i < thread_count; i++) {
        workers[i].thread_id = i;
        snprintf(workers[i].trace_path, sizeof(workers[i].trace_path),
                 "%s_%d.csv", run_prefix, i);
        if (pthread_create(&threads[i], NULL, run_worker, &workers[i]) != 0) {
            perror("pthread_create");
            rc = 1;
            thread_count = i;
            break;
        }
    }

    for (i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
        if (workers[i].result.status != 0) {
            rc = 1;
        }
        run_total.ops += workers[i].result.ops;
        run_total.reads += workers[i].result.reads;
        run_total.read_hits += workers[i].result.read_hits;
        run_total.read_misses += workers[i].result.read_misses;
        run_total.writes += workers[i].result.writes;
        run_total.deletes += workers[i].result.deletes;
    }
    wall_end = rtc_now_seconds();

    printf("load ops=%zu writes=%zu reads=%zu deletes=%zu\n",
           load_stats.ops, load_stats.writes, load_stats.reads, load_stats.deletes);
    printf("run threads: %d\n", thread_count);
    printf("run prefix: %s\n", run_prefix);
    printf("ops=%zu reads=%zu hits=%zu misses=%zu writes=%zu deletes=%zu elapsed=%.6f throughput=%.2f ops/s\n",
           run_total.ops, run_total.reads, run_total.read_hits, run_total.read_misses,
           run_total.writes, run_total.deletes, wall_end - wall_start,
           (wall_end - wall_start) > 0.0 ? run_total.ops / (wall_end - wall_start) : 0.0);

    size_t stats_len = 0;
    char *stats_snapshot = rtc_stats_snapshot(&stats_len);
    if (stats_snapshot != NULL) {
        printf("%s", stats_snapshot);
        free(stats_snapshot);
    }

    free(threads);
    free(workers);
    return rc;
}

static void print_usage(const char *argv0) {
    fprintf(stderr,
            "usage: %s [--load path] [--run-prefix prefix] [--threads N]\n",
            argv0);
}

int main(int argc, char **argv) {
    const char *load_trace = DEFAULT_LOAD_TRACE;
    const char *run_prefix = DEFAULT_RUN_PREFIX;
    int thread_count = 0;
    int i;
    int rc;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--load") == 0 && i + 1 < argc) {
            load_trace = argv[++i];
        } else if (strcmp(argv[i], "--run-prefix") == 0 && i + 1 < argc) {
            run_prefix = argv[++i];
        } else if (strcmp(argv[i], "--threads") == 0 && i + 1 < argc) {
            thread_count = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            print_usage(argv[0]);
            return 1;
        }
    }

    if (thread_count <= 0) {
        thread_count = detect_thread_count(run_prefix);
        if (thread_count <= 0) {
            fprintf(stderr, "failed to detect trace threads for prefix %s\n", run_prefix);
            return 1;
        }
    }

    rtc_settings_init();
    settings.num_threads = thread_count;
    settings.binding_protocol = ascii_prot;
    rtc_stats_init();
    rtc_update_time();

    assoc_init(settings.hashpower_init);
    slabs_init(settings.maxbytes, settings.factor, false);
    thread_init(settings.num_threads, NULL);

    if (start_assoc_maintenance_thread() != 0) {
        fprintf(stderr, "failed to start assoc maintenance thread\n");
        return 1;
    }

    rc = run_replay(load_trace, run_prefix, thread_count);
    stop_assoc_maintenance_thread();
    return rc;
}
