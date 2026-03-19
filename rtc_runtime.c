/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "memcached.h"
#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#ifndef VERSION
#define VERSION "1.4.10-rtc"
#endif

#define REALTIME_MAXDELTA (60 * 60 * 24 * 30)

time_t process_started;

rel_time_t realtime(const time_t exptime) {
    if (exptime == 0) {
        return 0;
    }

    if (process_started == 0) {
        process_started = 1;
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
    if (process_started == 0) {
        process_started = 1;
    }
    if (current_time == 0) {
        current_time = 1;
    }
}

void rtc_stats_init(void) {
    memset(&stats, 0, sizeof(stats));
    stats.accepting_conns = true;
    process_started = 1;
    current_time = 1;
    stats_prefix_init();
}

void rtc_settings_init(void) {
    memset(&settings, 0, sizeof(settings));
    settings.access = 0700;
    settings.port = 11211;
    settings.udpport = 11211;
    settings.maxbytes = 64 * 1024 * 1024;
    settings.maxconns = 1024;
    settings.evict_to_free = 1;
    settings.factor = 1.25;
    settings.chunk_size = 48;
    settings.num_threads = 1;
    settings.prefix_delimiter = ':';
    settings.reqs_per_event = 20;
    settings.backlog = 1024;
    settings.binding_protocol = ascii_prot;
    settings.item_size_max = 1024 * 1024;
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

enum store_item_type do_store_item(item *it, int comm, conn *c,
                                   const uint32_t hv) {
    item *old_it = do_item_get(ITEM_key(it), it->nkey, hv);

    if (comm == NREAD_ADD && old_it != NULL) {
        do_item_remove(old_it);
        return NOT_STORED;
    }

    if (comm != NREAD_ADD && comm != NREAD_SET) {
        if (old_it != NULL) {
            do_item_remove(old_it);
        }
        return NOT_STORED;
    }

    if (old_it != NULL) {
        item_replace(old_it, it, hv);
        do_item_remove(old_it);
    } else {
        do_item_link(it, hv);
    }

    c->cas = ITEM_get_cas(it);
    return STORED;
}

enum delta_result_type do_add_delta(conn *c, const char *key, const size_t nkey,
                                    const bool incr, const int64_t delta,
                                    char *buf, uint64_t *cas,
                                    const uint32_t hv) {
    (void)c;
    (void)key;
    (void)nkey;
    (void)incr;
    (void)delta;
    (void)buf;
    (void)cas;
    (void)hv;
    return DELTA_ITEM_NOT_FOUND;
}
