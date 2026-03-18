#include "memcached.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

typedef struct {
    int fd;
    int id;
    pthread_t thread;
    const char* script;
    char response[4096];
    size_t response_len;
} analysis_client;

typedef struct {
    analysis_client* clients;
    size_t count;
} analysis_joiner;

void memcached_request_shutdown(void);

static const char* kScripts[] = {
    "set counter 0 0 1\r\n0\r\n"
    "set shared 0 0 3\r\none\r\n"
    "get shared\r\n"
    "quit\r\n",
    "set shared 0 0 3\r\ntwo\r\n"
    "incr counter 1\r\n"
    "get counter\r\n"
    "quit\r\n",
    "set shared 0 0 5\r\nthree\r\n"
    "delete shared\r\n"
    "set shared 0 0 4\r\nfour\r\n"
    "get shared\r\n"
    "quit\r\n",
};

static void* run_client(void* arg) {
    analysis_client* client = arg;
    size_t script_len = strlen(client->script);
    size_t offset = 0;

    while (offset < script_len) {
        ssize_t written = write(client->fd, client->script + offset,
                                script_len - offset);
        if (written <= 0) {
            break;
        }
        offset += (size_t)written;
    }
    shutdown(client->fd, SHUT_WR);

    while (client->response_len < sizeof(client->response) - 1) {
        ssize_t read_bytes =
            read(client->fd, client->response + client->response_len,
                 sizeof(client->response) - client->response_len - 1);
        if (read_bytes <= 0) {
            break;
        }
        client->response_len += (size_t)read_bytes;
    }
    client->response[client->response_len] = '\0';
    close(client->fd);
    return NULL;
}

static void* join_clients(void* arg) {
    analysis_joiner* joiner = arg;

    for (size_t i = 0; i < joiner->count; ++i) {
        pthread_join(joiner->clients[i].thread, NULL);
        fprintf(stdout, "client-%d response:\n%s\n",
                joiner->clients[i].id, joiner->clients[i].response);
    }
    memcached_request_shutdown();
    free(joiner->clients);
    free(joiner);
    return NULL;
}

int analysis_harness_start(void) {
    const size_t client_count = sizeof(kScripts) / sizeof(kScripts[0]);
    analysis_client* clients = calloc(client_count, sizeof(*clients));
    analysis_joiner* joiner = calloc(1, sizeof(*joiner));
    pthread_t joiner_thread;

    if (clients == NULL || joiner == NULL) {
        free(clients);
        free(joiner);
        return -1;
    }

    for (size_t i = 0; i < client_count; ++i) {
        int pair[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, pair) != 0) {
            free(clients);
            free(joiner);
            return -1;
        }

        clients[i].fd = pair[0];
        clients[i].id = (int)i;
        clients[i].script = kScripts[i];

        dispatch_conn_new(pair[1], conn_read, EV_READ | EV_PERSIST, 1, 0);
        pthread_create(&clients[i].thread, NULL, run_client, &clients[i]);
    }

    joiner->clients = clients;
    joiner->count = client_count;
    pthread_create(&joiner_thread, NULL, join_clients, joiner);
    pthread_detach(joiner_thread);

    return 0;
}
