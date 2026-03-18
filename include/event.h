#ifndef MEMCACHED_ANALYSIS_EVENT_H
#define MEMCACHED_ANALYSIS_EVENT_H

#include <sys/time.h>

struct event_base;
struct event;

typedef void (*event_callback_fn)(int, short, void*);

#define EV_TIMEOUT 0x01
#define EV_READ 0x02
#define EV_WRITE 0x04
#define EV_PERSIST 0x10

struct event {
    int fd;
    short events;
    short active_events;
    int registered;
    int has_timeout;
    struct timeval timeout_at;
    event_callback_fn callback;
    void* arg;
    struct event_base* ev_base;
    struct event* next;
};

struct event_base* event_init(void);
void event_set(struct event* ev, int fd, short events, event_callback_fn cb,
               void* arg);
int event_base_set(struct event_base* base, struct event* ev);
int event_add(struct event* ev, const struct timeval* timeout);
int event_del(struct event* ev);
int event_base_loop(struct event_base* base, int flags);
int event_base_loopexit(struct event_base* base, const struct timeval* timeout);

#define evtimer_set(ev, cb, arg) event_set((ev), -1, 0, (cb), (arg))
#define evtimer_add(ev, tv) event_add((ev), (tv))
#define evtimer_del(ev) event_del((ev))

#endif
