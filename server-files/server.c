#include "segel.h"
#include "request.h"
#include "log.h"
#include <pthread.h>
#include "queue.h"
#include <stdlib.h>
#include <sys/time.h>


//
// server.c: A very, very simple web server
//
// To run:
//  ./server <portnum (above 2000)>
//
// Repeatedly handles HTTP requests sent to this port number.
// Most of the work is done within routines written in request.c
//

sem_t tasks;
sem_t queue_slots;
pthread_mutex_t queue_mutex;
struct Queue *queue = malloc(sizeof(struct Queue));

// Parses command-line arguments
void getargs(int *tcp_port, int *udp_port, int *thread_count, int *que_size, int *sleep_time, int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        exit(1);
    }
    *tcp_port = atoi(argv[1]);

    if (argc < 6){
        *udp_port = -1;
        *thread_count = 100;
        *que_size = 100;
        *sleep_time = 50;
        return;
    }

    *udp_port = atoi(argv[2]);
    *thread_count = atoi(argv[3]);
    *que_size = atoi(argv[4]);
    *sleep_time = atoi(argv[5]);
}

// TODO: HW3 — Task 1: Initialize the thread pool and request queue.
// This server currently handles all requests in the main thread.

void* process_request(struct Task task, threads_stats t) {
    //time_stats dum = task.time_stats;

    // gettimeofday(&arrival, NULL);

    requestHandle(task.connfd, task.time_stats, t, task.log);

    Close(task.connfd); // Close the connection
    return NULL;
}

void* find_task(void* arg) {
    struct Task task;
    int id = *(int*)arg;
    free(arg);
    threads_stats t = malloc(sizeof(struct Threads_stats));

    t->id = id;
    t->stat_req = 0;       // Static request count
    t->dynm_req = 0;       // Dynamic request count
    t->post_req = 0;       // POST request count
    t->total_req = 0;      // Total request count

    while (1) {
        sem_wait(&tasks);
        pthread_mutex_lock(&queue_mutex);

        task = dequeue(queue);
        gettimeofday(&task.time_stats.task_dispatch, NULL);
        printf("seconds: %ld, microseconds: %ld\n", (long)task.time_stats.task_dispatch.tv_sec, (long)task.time_stats.task_dispatch.tv_usec);

        pthread_mutex_unlock(&queue_mutex);
        sem_post(&queue_slots);
        process_request(task, t);
    }

    free(t); // Cleanup
    return NULL;
}


// TODO: HW3 — Task 4: Add the UDP channel (see the UDP_* wrappers in segel.c).

// TODO: HW3 — Extend getargs() to parse the full argument list.

int main(int argc, char *argv[])
{

    struct timeval start_time;

    // Create the global server log
    server_log log = create_log();

    int listenfd, connfd, clientlen;
    struct sockaddr_in clientaddr;

    // in prog
    int tcp_port, udp_port, thread_count, que_size, sleep_time;
    int udp_line = -1;

    getargs(&tcp_port, &udp_port, &thread_count, &que_size, &sleep_time, argc, argv);
    // end prog

    listenfd = Open_listenfd(tcp_port);

    if(udp_port != -1){
        udp_line = UDP_Open(udp_port);
    }

    // initialize queue
    initialize_queue(queue, que_size);
    sem_init(&tasks,  0, 0); //may need to be 100
    sem_init(&queue_slots,  0, queue->max_size);
    pthread_mutex_init(&queue_mutex, NULL);

    // create N worker threads
    pthread_t worker_threads[thread_count];
    for (int i = 0; i < thread_count; i++) {
        int *id = malloc(sizeof(int));
        *id = i;
        if (!pthread_create(&worker_threads[i], NULL, find_task, id)) {
            //return some error;
        }
    }

    while (1) {
        struct Task task;
        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t*) &clientlen);

        // TODO: HW3 — Record the request arrival time here.
        gettimeofday(&task.time_stats.task_arrival, NULL);
        task.connfd = connfd;
        task.log = log;
        printf("seconds: %ld, microseconds: %ld\n", (long)task.time_stats.task_arrival.tv_sec, (long)task.time_stats.task_arrival.tv_usec);

        sem_wait(&queue_slots);
        pthread_mutex_lock(&queue_mutex);
        //printf("enqueue fd=%d, task.fd=%d\n", connfd, task.connfd);
        enqueue(queue, task);
        pthread_mutex_unlock(&queue_mutex);
        sem_post(&tasks);

    }

    // Clean up the server log before exiting
    destroy_log(log);

    // TODO: HW3 — Add cleanup code for the thread pool and queue.
}
