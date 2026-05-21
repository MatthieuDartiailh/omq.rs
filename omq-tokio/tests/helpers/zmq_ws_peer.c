// Helper for libzmq WS interop tests.
// Usage: zmq_ws_peer push ws://host:port COUNT SIZE
//        zmq_ws_peer pull ws://host:port COUNT SIZE
#include <zmq.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv) {
    if (argc < 5) {
        fprintf(stderr, "usage: %s push|pull ENDPOINT COUNT SIZE\n", argv[0]);
        return 1;
    }
    const char *role = argv[1];
    const char *endpoint = argv[2];
    int count = atoi(argv[3]);
    int size = atoi(argv[4]);

    void *ctx = zmq_ctx_new();
    if (!ctx) { perror("zmq_ctx_new"); return 1; }

    int is_push = strcmp(role, "push") == 0;
    void *sock = zmq_socket(ctx, is_push ? ZMQ_PUSH : ZMQ_PULL);
    if (!sock) { perror("zmq_socket"); return 1; }

    int linger = 1000;
    zmq_setsockopt(sock, ZMQ_LINGER, &linger, sizeof(linger));

    int rc;
    if (is_push) {
        rc = zmq_connect(sock, endpoint);
    } else {
        rc = zmq_bind(sock, endpoint);
    }
    if (rc != 0) {
        fprintf(stderr, "%s failed: %s\n", is_push ? "connect" : "bind", zmq_strerror(zmq_errno()));
        return 1;
    }

    char *buf = calloc(1, size);
    if (is_push) {
        // Wait for connection to establish
        zmq_sleep(1);
        for (int i = 0; i < count; i++) {
            snprintf(buf, size, "msg-%d", i);
            rc = zmq_send(sock, buf, size, 0);
            if (rc < 0) {
                fprintf(stderr, "send[%d] failed: %s\n", i, zmq_strerror(zmq_errno()));
                break;
            }
        }
    } else {
        for (int i = 0; i < count; i++) {
            rc = zmq_recv(sock, buf, size, 0);
            if (rc < 0) {
                fprintf(stderr, "recv[%d] failed: %s\n", i, zmq_strerror(zmq_errno()));
                break;
            }
        }
        // Print success marker
        printf("OK %d\n", count);
        fflush(stdout);
    }

    free(buf);
    zmq_close(sock);
    zmq_ctx_destroy(ctx);
    return 0;
}
