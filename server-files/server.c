#include "segel.h"
#include "request.h"
#include "log.h"
#include <pthread.h>
#include "queue.h"
#include <stdlib.h>

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
pthread_mutex_t queue_mutex;

// Parses command-line arguments
void getargs(int *port, int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        exit(1);
    }
    *port = atoi(argv[1]);
}

// TODO: HW3 — Task 1: Initialize the thread pool and request queue.
// This server currently handles all requests in the main thread.

void* process_request(struct Task task) {
    threads_stats t = malloc(sizeof(struct Threads_stats));
    t->id = 0;             // Thread ID (placeholder)
    t->stat_req = 0;       // Static request count
    t->dynm_req = 0;       // Dynamic request count
    t->post_req = 0;       // POST request count
    t->total_req = 0;      // Total request count

    time_stats dum;

    // gettimeofday(&arrival, NULL);

    // Call the request handler (immediate in master thread — DEMO ONLY)
    //printf("Processing request\n");
    //sleep(5);
    requestHandle(task.connfd, dum, t, task.log);

    free(t); // Cleanup
    Close(task.connfd); // Close the connection
}

void* find_task(void* arg) {
    //printf("Worker thread started\n");
    struct Queue *queue = arg; // needs to be made global
    struct Task task;
    while (1) {
        sem_wait(&tasks);
        pthread_mutex_lock(&queue_mutex);
        task = dequeue(queue);
        //printf("dequeued fd=%d\n", task.connfd);
        //printf("worker %lu processing\n", pthread_self());
        pthread_mutex_unlock(&queue_mutex);
        process_request(task);
    }

    return NULL;
}


// TODO: HW3 — Task 4: Add the UDP channel (see the UDP_* wrappers in segel.c).

// TODO: HW3 — Extend getargs() to parse the full argument list.

int main(int argc, char *argv[])
{

    // initialize queue
    struct Queue *queue = malloc(sizeof(struct Queue));
    initialize_queue(queue);
    sem_init(&tasks,  0, 0); //may need to be 100
    pthread_mutex_init(&queue_mutex, NULL);

    // create N worker threads
    pthread_t worker_threads[100]; // may need to change 100 to something from argv
    for (int i = 0; i < 100; i++) {
        if (!pthread_create(&worker_threads[i], NULL, find_task, queue)) {
            //return some error;
        }
    }

    // Create the global server log
    server_log log = create_log();

    int listenfd, connfd, port, clientlen;
    struct sockaddr_in clientaddr;

    getargs(&port, argc, argv);

    listenfd = Open_listenfd(port);
    while (1) {
        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t*) &clientlen);

        // TODO: HW3 — Record the request arrival time here.


        struct Task task;
        task.connfd = connfd;
        task.log = log;
        pthread_mutex_lock(&queue_mutex);
        //printf("enqueue fd=%d, task.fd=%d\n", connfd, task.connfd);
        enqueue(queue, task);
        pthread_mutex_unlock(&queue_mutex);
        sem_post(&tasks);

        //Close(connfd); // Close the connection
    }

    // Clean up the server log before exiting
    destroy_log(log);

    // TODO: HW3 — Add cleanup code for the thread pool and queue.
}
