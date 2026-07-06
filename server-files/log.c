#include <stdlib.h>
#include <string.h>
#include "log.h"
#include <pthread.h>
#include "request.h"



//end of locking system
struct log_entry{
    struct log_entry* next;
    char* data;
    int len;
};


// Opaque struct definition
struct Server_Log {
    // TODO: Implement internal log storage (e.g., dynamic buffer, linked list, etc.)
    struct log_entry* head;
    struct log_entry* tail;
    int size;

    int readers_inside, writers_inside;
    pthread_cond_t read_allowed;
    pthread_cond_t write_allowed;
    pthread_mutex_t global_lock;
};

// Creates a new server log instance (stub)
server_log create_log() {
    // TODO: Allocate and initialize internal log structure
    server_log log = (server_log)malloc(sizeof(struct Server_Log));
    if(log == NULL) return NULL;

    readers_writers_init(log);

    log->head = malloc(sizeof(struct log_entry));
    log->tail = malloc(sizeof(struct log_entry));

    if(log->head == NULL) return NULL;
    if(log->tail == NULL) return NULL;

    log->head->next = NULL;
    log->tail->next = log->head;

    log->head->data = NULL;
    log->tail->data = "";

    log->head->len = 0;
    log->tail->len = 0;

    log->size = 0;



    return log;
}

// Destroys and frees the log (stub)
void destroy_log(server_log log) {
    // TODO: Free all internal resources used by the log
    if (log == NULL) return;
    struct log_entry* curr = log->tail;
    struct log_entry* temp;
    while(curr != log->head){
        temp = curr;
        curr = curr->next;
        free(temp->data);
        free(temp);
    }
    free(curr->data);
    free(curr);

    pthread_cond_destroy(&log->read_allowed);
    pthread_cond_destroy(&log->write_allowed);
    pthread_mutex_destroy(&log->global_lock);

    free(log);
}

//locking system

void readers_writers_init(server_log log) {
    log->readers_inside = 0;
    log->writers_inside = 0;
    pthread_cond_init(&log->read_allowed, NULL);
    pthread_cond_init(&log->write_allowed, NULL);
    pthread_mutex_init(&log->global_lock, NULL);
}

void reader_lock(server_log log) {
    pthread_mutex_lock(&log->global_lock);
    while (log->writers_inside > 0)
        pthread_cond_wait(&log->read_allowed, &log->global_lock);
    log->readers_inside++;
    pthread_mutex_unlock(&log->global_lock);
}

void reader_unlock(server_log log) {
    pthread_mutex_lock(&log->global_lock);
    log->readers_inside--;
    if (log->readers_inside == 0)
        pthread_cond_signal(&log->write_allowed);
    pthread_mutex_unlock(&log->global_lock);
}

void writer_lock(server_log log) {
    pthread_mutex_lock(&log->global_lock);
    while (log->writers_inside + log->readers_inside > 0)
        pthread_cond_wait(&log->write_allowed, &log->global_lock);
    log->writers_inside++;
    pthread_mutex_unlock(&log->global_lock);
}

void writer_unlock(server_log log) {
    pthread_mutex_lock(&log->global_lock);
    log->writers_inside--;
    if (log->writers_inside == 0) {
        pthread_cond_broadcast(&log->read_allowed);
        pthread_cond_signal(&log->write_allowed);
    }
    pthread_mutex_unlock(&log->global_lock);
}

// Returns dummy log content as string (stub)
int get_log(server_log log, char** dst) {
    // TODO: Return the full contents of the log as a dynamically allocated string
    // This function should handle concurrent access
    reader_lock(log);

    struct log_entry *curr = log->tail;
    int t_len = 0;

    while(curr != log->head){
        t_len += curr->len;
        curr = curr->next;
    }

    if(t_len == 0)
    {
        reader_unlock(log);
        return 0;
    }

    *dst = (char*)malloc(t_len + 1);

    if (*dst == NULL) {
        reader_unlock(log);
        return 0;
    }

    (*dst)[0] = '\0';

    curr = log->tail;

    while(curr != log->head){
        strcat(*dst, curr->data);
        curr = curr->next;
    }

    reader_unlock(log);
    return t_len;
}

// Appends a new entry to the log (no-op stub)
void add_to_log(server_log log, const char* data, int data_len) {
    // TODO: Append the provided data to the log
    //gettimeofday(&task.time_stats.log_enter, NULL);
    //printf("seconds: %ld, microseconds: %ld\n", (long)task.log_arrival_time.tv_sec, (long)task.log_arrival_time.tv_usec);
    writer_lock(log);
    struct log_entry *curr = log->head;
    curr->next = malloc(sizeof(struct log_entry));
    log->head = curr->next;
    if(log->head == NULL)
    {
        writer_unlock(log);
        return;
    }

    log->head->next = NULL;

    log->head->data = NULL;

    log->head->len = 0;

    curr->data = (char*)malloc(data_len + 1);
    if(curr->data  == NULL)
    {
        writer_unlock(log);
        return;
    }

    strcpy(curr->data, data);

    curr->len = data_len;

    log->size ++;

    //gettimeofday(&task.time_stats.log_exit, NULL);
    //printf("seconds: %ld, microseconds: %ld\n", (long)task.log_dispatch_time.tv_sec, (long)task.log_dispatch_time.tv_usec);
    writer_unlock(log);
    // This function should handle concurrent access
}
