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

pthread_cond_t tasks;
sem_t queue_slots;
pthread_mutex_t queue_mutex;
struct Queue *queue;
pthread_mutex_t udp_lock;
pthread_mutex_t check_lock;

struct Queue *UDP_Ques;
int udp_fd;

// Parses command-line arguments


int char_to_int(const char* arr) {
    int num = 0;

    for (int i = 0; arr[i] != '\0'; i++) {
        if (arr[i] >= '0' && arr[i] <= '9') {
            num = num * 10 + (arr[i] - '0');
        } else {
            unix_error("invalid input");
        }
    }

    return num;
}


void getargs(int *tcp_port, int *udp_port, int *thread_count, int *que_size, int *sleep_time, int argc, char *argv[])
{
    if (argc < 6) {unix_error("invalid argv");}
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        unix_error("invalid argv");
    }
    *tcp_port = char_to_int(argv[1]);

    if (argc < 6){
        *udp_port = -1;
        *thread_count = 100;
        *que_size = 100;
        *sleep_time = 50;
        return;
    }

    *udp_port = char_to_int(argv[2]);
    *thread_count = char_to_int(argv[3]);
    *que_size = char_to_int(argv[4]);
    *sleep_time = char_to_int(argv[5]);

    if(*udp_port < 1024 || *udp_port > 49151 || *tcp_port < 1024 || *tcp_port > 49151){
        unix_error("invalid argv");
    }
    if(*udp_port == *tcp_port) {
        unix_error("invalid argv");
    }
    if(*thread_count <= 0 || *que_size <= 0) {
        unix_error("invalid argv");
    }

}



// TODO: HW3 — Task 1: Initialize the thread pool and request queue.
// This server currently handles all requests in the main thread.

void* process_request(struct Task task, threads_stats t) {
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
        pthread_mutex_lock(&check_lock);
        pthread_cond_wait(&tasks, &check_lock);

        pthread_mutex_lock(&udp_lock);
        if(UDP_Ques[id-1].size != 0)
        {
            while(UDP_Ques[id-1].size != 0)
            {
                task = dequeue(&UDP_Ques[id-1]);
                pthread_mutex_unlock(&udp_lock);
                char buf[128];
                buf[0] = '\0';
                int length = append_thread_log(buf, t);
                UDP_Write(udp_fd, task.from, buf, length);
                pthread_mutex_lock(&udp_lock);
            }
            pthread_mutex_unlock(&udp_lock);
        }
        else if (queue->size != 0)
        {
            pthread_mutex_unlock(&udp_lock);
            pthread_mutex_lock(&queue_mutex);

            task = dequeue(queue);
            gettimeofday(&task.time_stats.task_dispatch, NULL);

            pthread_mutex_unlock(&queue_mutex);
            pthread_mutex_unlock(&check_lock);
            sem_post(&queue_slots);
            process_request(task, t);
        } else
        {
            pthread_mutex_unlock(&udp_lock);
            pthread_mutex_unlock(&check_lock);
        }

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

    int tcp_fd, connfd, clientlen;
    struct sockaddr_in clientaddr;

    // in prog
    int tcp_port, udp_port, thread_count, que_size, sleep_time;
    udp_fd = -1;

    getargs(&tcp_port, &udp_port, &thread_count, &que_size, &sleep_time, argc, argv);
    // end prog

    tcp_fd = Open_listenfd(tcp_port);

    if(udp_port != -1){
        udp_fd = UDP_Open(udp_port);
    }

    server_log log = create_log(sleep_time);

    // initialize queue
    queue = malloc(sizeof(struct Queue));
    if (queue == NULL) {
        return -1;
    }
    initialize_queue(queue, que_size);
    pthread_cond_init(&tasks,  NULL); //may need to be 100
    sem_init(&queue_slots,  0, queue->max_size);
    pthread_mutex_init(&queue_mutex, NULL);
    pthread_mutex_init(&udp_lock, NULL);
    pthread_mutex_init(&check_lock, NULL);

    // create N worker threads
    pthread_t worker_threads[thread_count];
    for (int i = 0; i < thread_count; i++) {
        int *id = malloc(sizeof(int));
        *id = i + 1;
        if (pthread_create(&worker_threads[i], NULL, find_task, id) != 0) {
            unix_error("failed to creat threads");
        }
    }


    UDP_Ques = malloc(sizeof(struct Queue) * thread_count);
    if (UDP_Ques == NULL) {
        return -1;
    }
    for (int i = 0; i < thread_count; i++) {
        initialize_queue(&UDP_Ques[i], 1000);
    }

    fd_set readfds;

    while (1) {
        FD_ZERO(&readfds);

        FD_SET(tcp_fd, &readfds);

        int maxfd = tcp_fd;

        if (udp_fd >= 0) {
            FD_SET(udp_fd, &readfds);

            if (udp_fd > maxfd) {
                maxfd = udp_fd;
            }
        }

        select(maxfd + 1, &readfds, NULL, NULL, NULL);

        if (FD_ISSET(tcp_fd, &readfds)) {
            struct Task task;
            clientlen = sizeof(clientaddr);
            connfd = Accept(tcp_fd, (SA *)&clientaddr, (socklen_t*) &clientlen);

        // TODO: HW3 — Record the request arrival time here.
            gettimeofday(&task.time_stats.task_arrival, NULL);
            task.connfd = connfd;
            task.log = log;

            sem_wait(&queue_slots);
            pthread_mutex_lock(&queue_mutex);
            enqueue(queue, task);
            pthread_mutex_unlock(&queue_mutex);

            pthread_mutex_lock(&udp_lock);
            pthread_cond_broadcast(&tasks);
            pthread_mutex_unlock(&udp_lock);
        }
        else if (udp_fd >= 0 && FD_ISSET(udp_fd, &readfds)) {
            char buf[1024];

            int n = UDP_Read(udp_fd, &clientaddr, buf, sizeof(buf));
            if(n > 0){
                buf[n] = '\0';
                int id = char_to_int(buf);
                if(id < 1 || id > thread_count){unix_error("invalid thread id");
                }
                else
                {
                    struct Task task;
                    task.from = malloc(sizeof(struct sockaddr_in));
                    *task.from = clientaddr;
                    pthread_mutex_lock(&udp_lock);
                    enqueue(&UDP_Ques[id-1], task);
                    pthread_cond_broadcast(&tasks);
                    pthread_mutex_unlock(&udp_lock);
                }
            }  else {

            }
        }
    }

    // Clean up the server log before exiting
    destroy_log(log);

    // TODO: HW3 — Add cleanup code for the thread pool and queue.
}