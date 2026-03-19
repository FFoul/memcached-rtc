/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "memcached.h"
#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>

#ifndef VERSION
#define VERSION "1.4.10-rtc"
#endif

#define REALTIME_MAXDELTA (60 * 60 * 24 * 30)

time_t process_started;

typedef struct {
    char *data;
    size_t length;
    size_t capacity;
} rtc_stats_buffer;

static const char *rtc_prot_text(enum protocol prot) {
    switch (prot) {
    case ascii_prot:
        return "ascii";
    case binary_prot:
        return "binary";
    case negotiating_prot:
    default:
        return "auto-negotiate";
    }
}

static bool rtc_buffer_reserve(rtc_stats_buffer *buffer, size_t extra) {
    size_t needed = buffer->length + extra + 1;
    size_t capacity = buffer->capacity == 0 ? 256 : buffer->capacity;

    if (needed <= buffer->capacity) {
        return true;
    }

    while (capacity < needed) {
        capacity *= 2;
    }

    char *new_data = realloc(buffer->data, capacity);
    if (new_data == NULL) {
        return false;
    }

    buffer->data = new_data;
    buffer->capacity = capacity;
    return true;
}

static bool rtc_buffer_append(rtc_stats_buffer *buffer, const char *data, size_t len) {
    if (!rtc_buffer_reserve(buffer, len)) {
        return false;
    }

    memcpy(buffer->data + buffer->length, data, len);
    buffer->length += len;
    buffer->data[buffer->length] = '\0';
    return true;
}

static void rtc_add_stat_line(const char *key, const uint16_t klen,
                              const char *val, const uint32_t vlen,
                              const void *cookie) {
    rtc_stats_buffer *buffer = (rtc_stats_buffer *)cookie;

    if (key == NULL || val == NULL) {
        (void)rtc_buffer_append(buffer, "END\r\n", 5);
        return;
    }

    (void)rtc_buffer_append(buffer, "STAT ", 5);
    (void)rtc_buffer_append(buffer, key, klen);
    (void)rtc_buffer_append(buffer, " ", 1);
    (void)rtc_buffer_append(buffer, val, vlen);
    (void)rtc_buffer_append(buffer, "\r\n", 2);
}

static void rtc_server_stats(ADD_STAT add_stats, void *cookie) {
    conn *c = (conn *)cookie;
    rel_time_t now = current_time;
    struct thread_stats thread_stats;
    struct slab_stats slab_stats;
    struct rusage usage;

    threadlocal_stats_aggregate(&thread_stats);
    slab_stats_aggregate(&thread_stats, &slab_stats);
    getrusage(RUSAGE_SELF, &usage);

    STATS_LOCK();
    append_stat("pid", add_stats, c, "%lu", (unsigned long)getpid());
    append_stat("uptime", add_stats, c, "%u", now);
    append_stat("time", add_stats, c, "%ld", now + (long)process_started);
    append_stat("version", add_stats, c, "%s", VERSION);
    append_stat("libevent", add_stats, c, "%s", "none");
    append_stat("pointer_size", add_stats, c, "%d", (int)(8 * sizeof(void *)));
    append_stat("rusage_user", add_stats, c, "%ld.%06ld",
                (long)usage.ru_utime.tv_sec, (long)usage.ru_utime.tv_usec);
    append_stat("rusage_system", add_stats, c, "%ld.%06ld",
                (long)usage.ru_stime.tv_sec, (long)usage.ru_stime.tv_usec);
    append_stat("curr_connections", add_stats, c, "%u", stats.curr_conns);
    append_stat("total_connections", add_stats, c, "%u", stats.total_conns);
    append_stat("connection_structures", add_stats, c, "%u", stats.conn_structs);
    append_stat("reserved_fds", add_stats, c, "%u", stats.reserved_fds);
    append_stat("cmd_get", add_stats, c, "%llu",
                (unsigned long long)thread_stats.get_cmds);
    append_stat("cmd_set", add_stats, c, "%llu",
                (unsigned long long)slab_stats.set_cmds);
    append_stat("cmd_flush", add_stats, c, "%llu",
                (unsigned long long)thread_stats.flush_cmds);
    append_stat("cmd_touch", add_stats, c, "%llu",
                (unsigned long long)thread_stats.touch_cmds);
    append_stat("get_hits", add_stats, c, "%llu",
                (unsigned long long)slab_stats.get_hits);
    append_stat("get_misses", add_stats, c, "%llu",
                (unsigned long long)thread_stats.get_misses);
    append_stat("delete_misses", add_stats, c, "%llu",
                (unsigned long long)thread_stats.delete_misses);
    append_stat("delete_hits", add_stats, c, "%llu",
                (unsigned long long)slab_stats.delete_hits);
    append_stat("incr_misses", add_stats, c, "%llu",
                (unsigned long long)thread_stats.incr_misses);
    append_stat("incr_hits", add_stats, c, "%llu",
                (unsigned long long)slab_stats.incr_hits);
    append_stat("decr_misses", add_stats, c, "%llu",
                (unsigned long long)thread_stats.decr_misses);
    append_stat("decr_hits", add_stats, c, "%llu",
                (unsigned long long)slab_stats.decr_hits);
    append_stat("cas_misses", add_stats, c, "%llu",
                (unsigned long long)thread_stats.cas_misses);
    append_stat("cas_hits", add_stats, c, "%llu",
                (unsigned long long)slab_stats.cas_hits);
    append_stat("cas_badval", add_stats, c, "%llu",
                (unsigned long long)slab_stats.cas_badval);
    append_stat("touch_hits", add_stats, c, "%llu",
                (unsigned long long)slab_stats.touch_hits);
    append_stat("touch_misses", add_stats, c, "%llu",
                (unsigned long long)thread_stats.touch_misses);
    append_stat("bytes_read", add_stats, c, "%llu",
                (unsigned long long)thread_stats.bytes_read);
    append_stat("bytes_written", add_stats, c, "%llu",
                (unsigned long long)thread_stats.bytes_written);
    append_stat("limit_maxbytes", add_stats, c, "%llu",
                (unsigned long long)settings.maxbytes);
    append_stat("accepting_conns", add_stats, c, "%u", stats.accepting_conns);
    append_stat("listen_disabled_num", add_stats, c, "%llu",
                (unsigned long long)stats.listen_disabled_num);
    append_stat("threads", add_stats, c, "%d", settings.num_threads);
    append_stat("conn_yields", add_stats, c, "%llu",
                (unsigned long long)thread_stats.conn_yields);
    append_stat("hash_power_level", add_stats, c, "%u", stats.hash_power_level);
    append_stat("hash_bytes", add_stats, c, "%llu",
                (unsigned long long)stats.hash_bytes);
    append_stat("hash_is_expanding", add_stats, c, "%u", stats.hash_is_expanding);
    append_stat("expired_unfetched", add_stats, c, "%llu",
                (unsigned long long)stats.expired_unfetched);
    append_stat("evicted_unfetched", add_stats, c, "%llu",
                (unsigned long long)stats.evicted_unfetched);
    append_stat("binding_protocol", add_stats, c, "%s",
                rtc_prot_text(settings.binding_protocol));
    STATS_UNLOCK();
}

rel_time_t realtime(const time_t exptime) {
    if (exptime == 0) {
        return 0;
    }

    if (exptime > REALTIME_MAXDELTA) {
        if (exptime <= process_started) {
            return (rel_time_t)1;
        }
        return (rel_time_t)(exptime - process_started);
    }

    return (rel_time_t)(exptime + current_time);
}

void rtc_update_time(void) {
    time_t now = time(NULL);

    if (process_started == 0) {
        process_started = now - 2;
    }

    current_time = (rel_time_t)(now - process_started);
}

void rtc_advance_time(rel_time_t delta) {
    current_time += delta;
}

void rtc_stats_init(void) {
    stats.curr_items = 0;
    stats.total_items = 0;
    stats.curr_bytes = 0;
    stats.curr_conns = 0;
    stats.total_conns = 0;
    stats.rejected_conns = 0;
    stats.reserved_fds = 0;
    stats.conn_structs = 0;
    stats.get_cmds = 0;
    stats.set_cmds = 0;
    stats.touch_cmds = 0;
    stats.get_hits = 0;
    stats.get_misses = 0;
    stats.touch_hits = 0;
    stats.touch_misses = 0;
    stats.evictions = 0;
    stats.reclaimed = 0;
    stats.started = 0;
    stats.accepting_conns = true;
    stats.listen_disabled_num = 0;
    stats.hash_power_level = 0;
    stats.hash_bytes = 0;
    stats.hash_is_expanding = false;
    stats.expired_unfetched = 0;
    stats.evicted_unfetched = 0;

    process_started = time(NULL) - 2;
    current_time = (rel_time_t)(time(NULL) - process_started);
    stats_prefix_init();
}

void rtc_settings_init(void) {
    settings.use_cas = true;
    settings.access = 0700;
    settings.port = 11211;
    settings.udpport = 11211;
    settings.inter = NULL;
    settings.maxbytes = 64 * 1024 * 1024;
    settings.maxconns = 1024;
    settings.verbose = 0;
    settings.oldest_live = 0;
    settings.evict_to_free = 1;
    settings.socketpath = NULL;
    settings.factor = 1.25;
    settings.chunk_size = 48;
    settings.num_threads = 1;
    settings.num_threads_per_udp = 0;
    settings.prefix_delimiter = ':';
    settings.detail_enabled = 0;
    settings.reqs_per_event = 20;
    settings.backlog = 1024;
    settings.binding_protocol = negotiating_prot;
    settings.item_size_max = 1024 * 1024;
    settings.sasl = false;
    settings.maxconns_fast = false;
    settings.hashpower_init = 0;
}

void append_stat(const char *name, ADD_STAT add_stats, conn *c,
                 const char *fmt, ...) {
    char val_str[STAT_VAL_LEN];
    int vlen;
    va_list ap;

    assert(name != NULL);
    assert(add_stats != NULL);
    assert(fmt != NULL);

    va_start(ap, fmt);
    vlen = vsnprintf(val_str, sizeof(val_str) - 1, fmt, ap);
    va_end(ap);

    add_stats(name, strlen(name), val_str, vlen, c);
}

char *rtc_stats_snapshot(size_t *length) {
    rtc_stats_buffer buffer = {0};

    rtc_server_stats(rtc_add_stat_line, &buffer);
    (void)get_stats(NULL, 0, rtc_add_stat_line, &buffer);
    rtc_add_stat_line(NULL, 0, NULL, 0, &buffer);

    if (buffer.data == NULL) {
        buffer.data = strdup("END\r\n");
        buffer.length = buffer.data == NULL ? 0 : 5;
    }

    if (length != NULL) {
        *length = buffer.length;
    }

    return buffer.data;
}

enum store_item_type do_store_item(item *it, int comm, conn *c, const uint32_t hv) {
    char *key = ITEM_key(it);
    item *old_it = do_item_get(key, it->nkey, hv);
    enum store_item_type stored = NOT_STORED;
    item *new_it = NULL;
    int flags;

    if (old_it != NULL && comm == NREAD_ADD) {
        do_item_update(old_it);
    } else if (!old_it && (comm == NREAD_REPLACE ||
                           comm == NREAD_APPEND ||
                           comm == NREAD_PREPEND)) {
        /* replace/append/prepend require an existing item */
    } else if (comm == NREAD_CAS) {
        if (old_it == NULL) {
            stored = NOT_FOUND;
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.cas_misses++;
            pthread_mutex_unlock(&c->thread->stats.mutex);
        } else if (ITEM_get_cas(it) == ITEM_get_cas(old_it)) {
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.slab_stats[old_it->slabs_clsid].cas_hits++;
            pthread_mutex_unlock(&c->thread->stats.mutex);

            item_replace(old_it, it, hv);
            stored = STORED;
        } else {
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.slab_stats[old_it->slabs_clsid].cas_badval++;
            pthread_mutex_unlock(&c->thread->stats.mutex);

            if (settings.verbose > 1) {
                fprintf(stderr, "CAS: failure: expected %llu, got %llu\n",
                        (unsigned long long)ITEM_get_cas(old_it),
                        (unsigned long long)ITEM_get_cas(it));
            }
            stored = EXISTS;
        }
    } else {
        if (comm == NREAD_APPEND || comm == NREAD_PREPEND) {
            if (ITEM_get_cas(it) != 0 && ITEM_get_cas(it) != ITEM_get_cas(old_it)) {
                stored = EXISTS;
            }

            if (stored == NOT_STORED) {
                flags = (int)strtol(ITEM_suffix(old_it), (char **)NULL, 10);
                new_it = item_alloc(key, it->nkey, flags, old_it->exptime,
                                    it->nbytes + old_it->nbytes - 2);
                if (new_it == NULL) {
                    if (old_it != NULL) {
                        do_item_remove(old_it);
                    }
                    return NOT_STORED;
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
        }

        if (stored == NOT_STORED) {
            if (old_it != NULL) {
                item_replace(old_it, it, hv);
            } else {
                do_item_link(it, hv);
            }

            c->cas = ITEM_get_cas(it);
            stored = STORED;
        }
    }

    if (old_it != NULL) {
        do_item_remove(old_it);
    }
    if (new_it != NULL) {
        do_item_remove(new_it);
    }

    if (stored == STORED) {
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
    item *it;

    it = do_item_get(key, nkey, hv);
    if (!it) {
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
        MEMCACHED_COMMAND_INCR(c->sfd, ITEM_key(it), it->nkey, value);
    } else {
        value = delta > value ? 0 : value - delta;
        MEMCACHED_COMMAND_DECR(c->sfd, ITEM_key(it), it->nkey, value);
    }

    pthread_mutex_lock(&c->thread->stats.mutex);
    if (incr) {
        c->thread->stats.slab_stats[it->slabs_clsid].incr_hits++;
    } else {
        c->thread->stats.slab_stats[it->slabs_clsid].decr_hits++;
    }
    pthread_mutex_unlock(&c->thread->stats.mutex);

    snprintf(buf, INCR_MAX_STORAGE_LEN, "%llu", (unsigned long long)value);
    res = strlen(buf);
    if (res + 2 > it->nbytes || it->refcount != 1) {
        item *new_it = item_alloc(ITEM_key(it), it->nkey,
                                  atoi(ITEM_suffix(it) + 1), it->exptime,
                                  res + 2);
        if (new_it == NULL) {
            do_item_remove(it);
            return EOM;
        }

        memcpy(ITEM_data(new_it), buf, res);
        memcpy(ITEM_data(new_it) + res, "\r\n", 2);
        item_replace(it, new_it, hv);
        ITEM_set_cas(it, settings.use_cas ? ITEM_get_cas(new_it) : 0);
        do_item_remove(new_it);
    } else {
        mutex_lock(&cache_lock);
        ITEM_set_cas(it, settings.use_cas ? get_cas_id() : 0);
        pthread_mutex_unlock(&cache_lock);

        memcpy(ITEM_data(it), buf, res);
        memset(ITEM_data(it) + res, ' ', it->nbytes - res - 2);
        do_item_update(it);
    }

    if (cas != NULL) {
        *cas = ITEM_get_cas(it);
    }

    do_item_remove(it);
    return OK;
}
