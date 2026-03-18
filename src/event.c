#include "event.h"

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/time.h>
#include <unistd.h>

struct event_base {
    struct event* head;
    pthread_mutex_t lock;
    int should_exit;
};

static struct timeval timeval_now(void) {
    struct timeval now;
    gettimeofday(&now, NULL);
    return now;
}

static struct timeval timeval_add(const struct timeval a,
                                  const struct timeval b) {
    struct timeval out = a;
    out.tv_sec += b.tv_sec;
    out.tv_usec += b.tv_usec;
    if (out.tv_usec >= 1000000) {
        out.tv_sec += out.tv_usec / 1000000;
        out.tv_usec %= 1000000;
    }
    return out;
}

static int timeval_cmp(const struct timeval a, const struct timeval b) {
    if (a.tv_sec != b.tv_sec) {
        return (a.tv_sec > b.tv_sec) - (a.tv_sec < b.tv_sec);
    }
    return (a.tv_usec > b.tv_usec) - (a.tv_usec < b.tv_usec);
}

static struct timeval timeval_sub(const struct timeval end,
                                  const struct timeval start) {
    struct timeval out;
    out.tv_sec = end.tv_sec - start.tv_sec;
    out.tv_usec = end.tv_usec - start.tv_usec;
    if (out.tv_usec < 0) {
        out.tv_usec += 1000000;
        out.tv_sec -= 1;
    }
    if (out.tv_sec < 0) {
        out.tv_sec = 0;
        out.tv_usec = 0;
    }
    return out;
}

struct event_base* event_init(void) {
    struct event_base* base = calloc(1, sizeof(*base));
    if (base == NULL) {
        return NULL;
    }
    pthread_mutex_init(&base->lock, NULL);
    return base;
}

void event_set(struct event* ev, int fd, short events, event_callback_fn cb,
               void* arg) {
    memset(ev, 0, sizeof(*ev));
    ev->fd = fd;
    ev->events = events;
    ev->callback = cb;
    ev->arg = arg;
}

int event_base_set(struct event_base* base, struct event* ev) {
    ev->ev_base = base;
    return 0;
}

int event_add(struct event* ev, const struct timeval* timeout) {
    struct event_base* base = ev->ev_base;
    if (base == NULL) {
        errno = EINVAL;
        return -1;
    }

    pthread_mutex_lock(&base->lock);
    if (timeout != NULL) {
        ev->timeout_at = timeval_add(timeval_now(), *timeout);
        ev->has_timeout = 1;
    }
    if (!ev->registered) {
        ev->next = base->head;
        base->head = ev;
        ev->registered = 1;
    }
    pthread_mutex_unlock(&base->lock);
    return 0;
}

int event_del(struct event* ev) {
    struct event_base* base = ev->ev_base;
    struct event** cursor;

    if (base == NULL) {
        return 0;
    }

    pthread_mutex_lock(&base->lock);
    cursor = &base->head;
    while (*cursor != NULL) {
        if (*cursor == ev) {
            *cursor = ev->next;
            ev->next = NULL;
            ev->registered = 0;
            ev->has_timeout = 0;
            break;
        }
        cursor = &(*cursor)->next;
    }
    pthread_mutex_unlock(&base->lock);
    return 0;
}

int event_base_loopexit(struct event_base* base, const struct timeval* timeout) {
    (void)timeout;
    pthread_mutex_lock(&base->lock);
    base->should_exit = 1;
    pthread_mutex_unlock(&base->lock);
    return 0;
}

int event_base_loop(struct event_base* base, int flags) {
    (void)flags;

    while (1) {
        fd_set readfds;
        fd_set writefds;
        struct event** snapshot = NULL;
        struct timeval now;
        struct timeval timeout;
        struct timeval* timeout_ptr = NULL;
        struct timeval next_timeout;
        int snapshot_count = 0;
        int maxfd = -1;
        int rc;
        int have_timeout = 0;

        FD_ZERO(&readfds);
        FD_ZERO(&writefds);

        pthread_mutex_lock(&base->lock);
        if (base->should_exit) {
            pthread_mutex_unlock(&base->lock);
            break;
        }
        for (struct event* it = base->head; it != NULL; it = it->next) {
            snapshot_count++;
        }
        snapshot = calloc((size_t)(snapshot_count == 0 ? 1 : snapshot_count),
                          sizeof(*snapshot));
        snapshot_count = 0;
        for (struct event* it = base->head; it != NULL; it = it->next) {
            snapshot[snapshot_count++] = it;
            if ((it->events & EV_READ) && it->fd >= 0) {
                FD_SET(it->fd, &readfds);
                if (it->fd > maxfd) {
                    maxfd = it->fd;
                }
            }
            if ((it->events & EV_WRITE) && it->fd >= 0) {
                FD_SET(it->fd, &writefds);
                if (it->fd > maxfd) {
                    maxfd = it->fd;
                }
            }
            if (it->has_timeout) {
                if (!have_timeout || timeval_cmp(it->timeout_at, next_timeout) < 0) {
                    next_timeout = it->timeout_at;
                    have_timeout = 1;
                }
            }
        }
        pthread_mutex_unlock(&base->lock);

        now = timeval_now();
        if (have_timeout) {
            timeout = timeval_sub(next_timeout, now);
            timeout_ptr = &timeout;
        } else if (maxfd < 0) {
            timeout.tv_sec = 0;
            timeout.tv_usec = 10000;
            timeout_ptr = &timeout;
        }

        rc = select(maxfd + 1, &readfds, &writefds, NULL, timeout_ptr);
        now = timeval_now();
        if (rc < 0 && errno != EINTR) {
            free(snapshot);
            return -1;
        }

        for (int i = 0; i < snapshot_count; ++i) {
            struct event* ev = snapshot[i];
            short fired = 0;

            if (!ev->registered) {
                continue;
            }
            if ((ev->events & EV_READ) && ev->fd >= 0 && FD_ISSET(ev->fd, &readfds)) {
                fired |= EV_READ;
            }
            if ((ev->events & EV_WRITE) && ev->fd >= 0 &&
                FD_ISSET(ev->fd, &writefds)) {
                fired |= EV_WRITE;
            }
            if (ev->has_timeout && timeval_cmp(now, ev->timeout_at) >= 0) {
                fired |= EV_TIMEOUT;
            }

            if (fired == 0) {
                continue;
            }

            if (!(ev->events & EV_PERSIST)) {
                event_del(ev);
            } else if (fired & EV_TIMEOUT) {
                pthread_mutex_lock(&base->lock);
                ev->has_timeout = 0;
                pthread_mutex_unlock(&base->lock);
            }

            ev->active_events = fired;
            ev->callback(ev->fd, fired, ev->arg);
        }

        free(snapshot);
    }

    return 0;
}
