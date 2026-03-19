/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "memcached.h"
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEFAULT_LOAD_TRACE "testdata/load_8.csv"
#define DEFAULT_RUN_PREFIX "testdata/run_8_50"
#define DEFAULT_VALUE "1"

typedef struct {
    size_t ops;
    size_t reads;
    size_t read_hits;
    size_t read_misses;
    size_t writes;
} replay_result;

typedef enum { TRACE_LOAD, TRACE_RUN } trace_mode;

typedef struct {
    int thread_id;
    char *trace_path;
    replay_result result;
    int rc;
} run_worker;

static char *trim(char *text) {
    char *end;

    while (*text == ' ' || *text == '\t' || *text == '\r' || *text == '\n') {
        text++;
    }

    end = text + strlen(text);
    while (end > text && (end[-1] == ' ' || end[-1] == '\t' ||
                          end[-1] == '\r' || end[-1] == '\n')) {
        end--;
    }

    *end = '\0';
    return text;
}

static int parse_csv_fields(char *line, char **op, char **key, char **value) {
    char *saveptr = NULL;
    char *fields[3];
    int count = 0;
    char *token = strtok_r(line, ",", &saveptr);

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

static int format_run_trace_path(char *buffer, size_t size, const char *prefix,
                                 int worker_id) {
    return snprintf(buffer, size, "%s_%d.csv", prefix, worker_id);
}

static int detect_worker_count(const char *run_prefix) {
    char path[PATH_MAX];
    int worker_count = 0;

    while (format_run_trace_path(path, sizeof(path), run_prefix, worker_count) >
               0 &&
           access(path, F_OK) == 0) {
        worker_count++;
    }

    return worker_count;
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

static bool rtc_store_value(conn *connection, int command, const char *key,
                            const char *value) {
    const size_t key_len = strlen(key);
    const char *payload = value[0] == '\0' ? DEFAULT_VALUE : value;
    const size_t value_len = strlen(payload);
    item *it;
    enum store_item_type result;

    if (key_len == 0 || key_len > KEY_MAX_LENGTH) {
        return false;
    }

    it = item_alloc((char *)key, key_len, 0, realtime(0), (int)value_len + 2);
    if (it == NULL) {
        return false;
    }

    memcpy(ITEM_data(it), payload, value_len);
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

static int replay_trace_file(conn *connection, const char *path,
                             trace_mode mode, replay_result *result) {
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

        if (mode == TRACE_LOAD) {
            if (strcmp(op, "INSERT") != 0) {
                fprintf(stderr, "unsupported load op in %s: %s\n", path, op);
                free(line);
                fclose(file);
                return -1;
            }

            result->ops++;
            result->writes++;
            (void)rtc_store_value(connection, NREAD_ADD, key, value);
            continue;
        }

        if (strcmp(op, "READ") == 0) {
            result->ops++;
            result->reads++;
            if (rtc_read_exists(connection, key)) {
                result->read_hits++;
            } else {
                result->read_misses++;
            }
        } else if (strcmp(op, "UPDATE") == 0) {
            result->ops++;
            result->writes++;
            (void)rtc_store_value(connection, NREAD_SET, key, value);
        } else {
            fprintf(stderr, "unsupported run op in %s: %s\n", path, op);
            free(line);
            fclose(file);
            return -1;
        }
    }

    free(line);
    fclose(file);
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

    if (!rtc_conn_init(&connection, worker->thread_id)) {
        worker->rc = 1;
        return NULL;
    }

    worker->rc = replay_trace_file(&connection, worker->trace_path, TRACE_RUN,
                                   &worker->result);
    if (worker->rc != 0) {
        worker->rc = 1;
    }
    return NULL;
}

static int run_replay(const char *load_trace, const char *run_prefix) {
    char path[PATH_MAX];
    conn load_connection;
    replay_result load_stats = {0};
    replay_result run_stats = {0};
    run_worker *workers = NULL;
    pthread_t *threads = NULL;
    int worker_count;
    int created_threads = 0;
    int i;
    int rc = 0;

    worker_count = detect_worker_count(run_prefix);
    if (worker_count <= 0) {
        fprintf(stderr, "no run traces found for prefix %s\n", run_prefix);
        return 1;
    }

    if (!rtc_conn_init(&load_connection, 0)) {
        fprintf(stderr, "failed to bind synthetic load connection\n");
        return 1;
    }

    if (replay_trace_file(&load_connection, load_trace, TRACE_LOAD,
                          &load_stats) != 0) {
        return 1;
    }

    workers = calloc((size_t)worker_count, sizeof(*workers));
    threads = calloc((size_t)worker_count, sizeof(*threads));
    if (workers == NULL || threads == NULL) {
        fprintf(stderr, "failed to allocate worker state\n");
        free(workers);
        free(threads);
        return 1;
    }

    for (i = 0; i < worker_count; i++) {
        if (format_run_trace_path(path, sizeof(path), run_prefix, i) <= 0) {
            fprintf(stderr, "run trace path too long for worker %d\n", i);
            rc = 1;
            break;
        }

        workers[i].thread_id = i;
        workers[i].trace_path = strdup(path);
        if (workers[i].trace_path == NULL) {
            fprintf(stderr, "failed to allocate trace path for worker %d\n", i);
            rc = 1;
            break;
        }

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
        if (workers[i].trace_path != NULL) {
            free(workers[i].trace_path);
        }
        if (workers[i].rc != 0) {
            rc = 1;
        }
        accumulate_result(&run_stats, &workers[i].result);
    }

    for (; i < worker_count; i++) {
        if (workers[i].trace_path != NULL) {
            free(workers[i].trace_path);
        }
    }

    free(workers);
    free(threads);

    if (rc != 0) {
        return 1;
    }

    printf("workers=%d\n", worker_count);
    printf("load: ops=%zu writes=%zu\n", load_stats.ops, load_stats.writes);
    printf("run: ops=%zu reads=%zu hits=%zu misses=%zu writes=%zu\n",
           run_stats.ops, run_stats.reads, run_stats.read_hits,
           run_stats.read_misses, run_stats.writes);
    return 0;
}

static void print_usage(const char *argv0) {
    fprintf(stderr, "usage: %s [--load path] [--run-prefix path]\n", argv0);
}

int main(int argc, char **argv) {
    const char *load_trace = DEFAULT_LOAD_TRACE;
    const char *run_prefix = DEFAULT_RUN_PREFIX;
    int i;
    int worker_count;
    int rc;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--load") == 0 && i + 1 < argc) {
            load_trace = argv[++i];
        } else if (strcmp(argv[i], "--run-prefix") == 0 && i + 1 < argc) {
            run_prefix = argv[++i];
        } else if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            print_usage(argv[0]);
            return 1;
        }
    }

    worker_count = detect_worker_count(run_prefix);
    if (worker_count <= 0) {
        fprintf(stderr, "no run traces found for prefix %s\n", run_prefix);
        return 1;
    }

    rtc_settings_init();
    rtc_stats_init();
    rtc_update_time();

    assoc_init(settings.hashpower_init);
    slabs_init(settings.maxbytes, settings.factor, false);
    thread_init(worker_count, NULL);

    if (start_assoc_maintenance_thread() != 0) {
        fprintf(stderr, "failed to start assoc maintenance thread\n");
        return 1;
    }

    rc = run_replay(load_trace, run_prefix);
    stop_assoc_maintenance_thread();
    return rc;
}
