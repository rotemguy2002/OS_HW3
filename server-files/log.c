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
    int len;
};


// Opaque struct definition
struct Server_Log {
    // TODO: Implement internal log storage (e.g., dynamic buffer, linked list, etc.)
    struct log_entry* head;
    struct log_entry* tail;
    int size;
};

// Creates a new server log instance (stub)
server_log create_log() {
    // TODO: Allocate and initialize internal log structure
    readers_writers_init();

    server_log log = (server_log)malloc(sizeof(struct Server_Log));
    if(log == NULL) return NULL;

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
    free(log);
}

// Returns dummy log content as string (stub)
int get_log(server_log log, char** dst) {
    // TODO: Return the full contents of the log as a dynamically allocated string
    // This function should handle concurrent access
    reader_lock();

    struct log_entry *curr = log->tail;
    int t_len = 0;

    while(curr != log->head){
        t_len += curr->len;
        curr = curr->next;
    }

    if(t_len == 0)
    {
        reader_unlock();
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

    reader_unlock();
    return t_len;
}

// Appends a new entry to the log (no-op stub)
void add_to_log(server_log log, const char* data, int data_len) {
    // TODO: Append the provided data to the log
    writer_lock();
    struct log_entry *curr = log->head;
    curr->next = malloc(sizeof(struct log_entry));
    log->head = curr->next;
    if(log->head == NULL)
    {
        writer_unlock();
        return;
    }

    log->head->next = NULL;

    log->head->data = NULL;

    log->head->len = 0;

    curr->data = (char*)malloc(data_len + 1);
    if(curr->data  == NULL)
    {
        writer_unlock();
        return;
    }

    strcpy(curr->data, data);

    curr->len = data_len;

    log->size ++;

    writer_unlock();
    // This function should handle concurrent access
}
