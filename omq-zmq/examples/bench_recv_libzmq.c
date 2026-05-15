// Push/pull throughput and req/rep latency bench for libzmq.
// Build: cc -O2 -o bench_libzmq bench_recv_libzmq.c -lzmq -lpthread
// Run:   ./bench_libzmq

#include <zmq.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <assert.h>
#include <unistd.h>

static int BATCH = 100000;
static int RT_ITERS = 10000;

typedef struct {
    void *sock;
    const char *payload;
    int payload_len;
    pthread_barrier_t *barrier;
    int count;
} sender_args_t;

static void *push_thread(void *arg) {
    sender_args_t *a = (sender_args_t *)arg;
    for (int i = 0; i < a->count; i++) {
        int rc = zmq_send(a->sock, a->payload, a->payload_len, 0);
        assert(rc == a->payload_len);
    }
    if (a->barrier) pthread_barrier_wait(a->barrier);
    return NULL;
}

typedef struct {
    void *rep;
    int msg_size;
    int count;
} rep_args_t;

static void *rep_thread(void *arg) {
    rep_args_t *a = (rep_args_t *)arg;
    char *buf = calloc(1, a->msg_size + 1);
    for (int i = 0; i < a->count; i++) {
        int n = zmq_recv(a->rep, buf, a->msg_size + 1, 0);
        if (n < 0) break;
        zmq_send(a->rep, buf, n, 0);
    }
    free(buf);
    return NULL;
}

static double now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e9 + (double)ts.tv_nsec;
}

static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a, db = *(const double *)b;
    return (da > db) - (da < db);
}

static void bench_push_pull(const char *transport, int msg_size) {
    void *ctx = zmq_ctx_new();
    void *push = zmq_socket(ctx, ZMQ_PUSH);
    void *pull = zmq_socket(ctx, ZMQ_PULL);

    int hwm = BATCH * 2;
    zmq_setsockopt(push, ZMQ_SNDHWM, &hwm, sizeof(hwm));
    zmq_setsockopt(pull, ZMQ_RCVHWM, &hwm, sizeof(hwm));
    int timeo = 10000;
    zmq_setsockopt(pull, ZMQ_RCVTIMEO, &timeo, sizeof(timeo));

    zmq_bind(pull, transport);
    zmq_connect(push, transport);
    usleep(50000);

    char *payload = calloc(1, msg_size > 0 ? msg_size : 1);
    char *recv_buf = calloc(1, msg_size + 1);

    int rounds = 7;
    double samples[7];

    for (int r = 0; r < rounds; r++) {
        double t0 = now_ns();

        sender_args_t args = { push, payload, msg_size, NULL, BATCH };
        pthread_t tid;
        pthread_create(&tid, NULL, push_thread, &args);

        for (int i = 0; i < BATCH; i++) {
            zmq_recv(pull, recv_buf, msg_size + 1, 0);
        }
        pthread_join(tid, NULL);

        double elapsed = now_ns() - t0;
        samples[r] = elapsed / BATCH;
    }

    qsort(samples, rounds, sizeof(double), cmp_double);
    double best_mmps = 1000.0 / samples[0];
    double median_mmps = 1000.0 / samples[rounds / 2];
    printf("  sz=%7d  best=%6.2f  median=%6.2f  M msg/s\n",
           msg_size, best_mmps, median_mmps);

    free(payload);
    free(recv_buf);
    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

static void bench_req_rep(const char *transport, int msg_size) {
    void *ctx = zmq_ctx_new();
    void *req = zmq_socket(ctx, ZMQ_REQ);
    void *rep = zmq_socket(ctx, ZMQ_REP);

    int timeo = 5000;
    zmq_setsockopt(req, ZMQ_RCVTIMEO, &timeo, sizeof(timeo));
    zmq_setsockopt(rep, ZMQ_RCVTIMEO, &timeo, sizeof(timeo));

    zmq_bind(rep, transport);
    zmq_connect(req, transport);
    usleep(50000);

    char *payload = calloc(1, msg_size > 0 ? msg_size : 1);
    char *recv_buf = calloc(1, msg_size + 1);

    int rounds = 7;
    double samples[7];

    for (int r = 0; r < rounds; r++) {
        // +10 for warmup
        rep_args_t rargs = { rep, msg_size, RT_ITERS + 10 };
        pthread_t tid;
        pthread_create(&tid, NULL, rep_thread, &rargs);

        // Warmup
        for (int i = 0; i < 10; i++) {
            zmq_send(req, payload, msg_size, 0);
            zmq_recv(req, recv_buf, msg_size + 1, 0);
        }

        double t0 = now_ns();
        for (int i = 0; i < RT_ITERS; i++) {
            zmq_send(req, payload, msg_size, 0);
            zmq_recv(req, recv_buf, msg_size + 1, 0);
        }
        double elapsed = now_ns() - t0;
        samples[r] = elapsed / RT_ITERS;

        pthread_join(tid, NULL);
    }

    qsort(samples, rounds, sizeof(double), cmp_double);
    double best_krt = 1000000.0 / samples[0];
    double median_krt = 1000000.0 / samples[rounds / 2];
    printf("  sz=%7d  best=%7.1f  median=%7.1f  k rt/s\n",
           msg_size, best_krt, median_krt);

    free(payload);
    free(recv_buf);
    zmq_close(req);
    zmq_close(rep);
    zmq_ctx_term(ctx);
}

int main(int argc, char **argv) {
    if (argc > 1) BATCH = atoi(argv[1]);
    if (argc > 2) RT_ITERS = atoi(argv[2]);

    char ipc_addr[128];
    snprintf(ipc_addr, sizeof(ipc_addr), "ipc:///tmp/zmq-bench-%d.sock", getpid());

    int sizes[] = { 8, 64, 256, 1024, 16384 };

    char tcp_pp_addr[64], tcp_rr_addr[64];

    printf("=== libzmq push/pull throughput (%d msgs/round) ===\n", BATCH);
    printf("--- inproc ---\n");
    for (int i = 0; i < 5; i++) bench_push_pull("inproc://bench-pp", sizes[i]);
    printf("--- ipc ---\n");
    for (int i = 0; i < 5; i++) bench_push_pull(ipc_addr, sizes[i]);
    printf("--- tcp ---\n");
    for (int i = 0; i < 5; i++) {
        snprintf(tcp_pp_addr, sizeof(tcp_pp_addr),
                 "tcp://127.0.0.1:%d", 15570 + i);
        bench_push_pull(tcp_pp_addr, sizes[i]);
    }

    printf("\n=== libzmq req/rep latency (%d round-trips/round) ===\n", RT_ITERS);
    printf("--- inproc ---\n");
    for (int i = 0; i < 5; i++) bench_req_rep("inproc://bench-rr", sizes[i]);
    printf("--- ipc ---\n");
    for (int i = 0; i < 5; i++) bench_req_rep(ipc_addr, sizes[i]);
    printf("--- tcp ---\n");
    for (int i = 0; i < 5; i++) {
        snprintf(tcp_rr_addr, sizeof(tcp_rr_addr),
                 "tcp://127.0.0.1:%d", 15575 + i);
        bench_req_rep(tcp_rr_addr, sizes[i]);
    }

    return 0;
}
