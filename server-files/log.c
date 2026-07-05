#include <stdlib.h>
#include <string.h>
#include "log.h"
#include <pthread.h>

//locking system
int readers_inside, writers_inside;
pthread_cond_t read_allowed;
pthread_cond_t write_allowed;
pthread_mutex_t global_lock;

void readers_writers_init() {
    readers_inside = 0;
    writers_inside = 0;
    pthread_cond_init(&read_allowed, NULL);
    pthread_cond_init(&write_allowed, NULL);
    pthread_mutex_init(&global_lock, NULL);
}

void reader_lock() {
    pthread_mutex_lock(&global_lock);
    while (writers_inside > 0)
        pthread_cond_wait(&read_allowed, &global_lock);
    readers_inside++;
    pthread_mutex_unlock(&global_lock);
}

void reader_unlock() {
    pthread_mutex_lock(&global_lock);
    readers_inside--;
    if (readers_inside == 0)
        pthread_cond_signal(&write_allowed);
    pthread_mutex_unlock(&global_lock);
}

void writer_lock() {
    pthread_mutex_lock(&global_lock);
    while (writers_inside + readers_inside > 0)
        pthread_cond_wait(&write_allowed, &global_lock);
    writers_inside++;
    pthread_mutex_unlock(&global_lock);
}

void writer_unlock() {
    pthread_mutex_lock(&global_lock);
    writers_inside--;
    if (writers_inside == 0) {
        pthread_cond_broadcast(&read_allowed);
        pthread_cond_signal(&write_allowed);
    }
    pthread_mutex_unlock(&global_lock);
}

//end of locking system

struct log_entry{
    struct log_entry* next;
    char* data;
    int length;
};

// Opaque struct definition
struct Server_Log {
    // TODO: Implement internal log storage (e.g., dynamic buffer, linked list, etc.)
    struct log_entry* head;
    struct log_entry* tail;
};

// Creates a new server log instance (stub)
server_log create_log() {
    // TODO: Allocate and initialize internal log structure
    server_log log = (server_log)malloc(sizeof(struct Server_Log));
    //log->tail->next = log->head;

    return log;
}

// Destroys and frees the log (stub)
void destroy_log(server_log log) {
    // TODO: Free all internal resources used by the log
    struct log_entry* curr = log->head;
    struct log_entry* temp;
    while(curr != log->tail){
        temp = curr;
        curr = curr->next;
        free(temp->data);
        free(temp);
    }
    free(curr->data);
    free(curr);
    free(log);
}

// Returns dummy log content as string (stub)
int get_log(server_log log, char** dst) {
    // TODO: Return the full contents of the log as a dynamically allocated string
    // This function should handle concurrent access

    const char* dummy = "Log is not implemented.\n";
    int len = strlen(dummy);
    *dst = (char*)malloc(len + 1); // Allocate for caller
    if (*dst != NULL) {
        strcpy(*dst, dummy);
    }
    return len;
}

// Appends a new entry to the log (no-op stub)
void add_to_log(server_log log, const char* data, int data_len) {
    // TODO: Append the provided data to the log
    // This function should handle concurrent access
}
