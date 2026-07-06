//
// Created by Asus on 05/07/2026.
//

#ifndef QUEUE_H
#define QUEUE_H

#include <stdlib.h>

struct Task {
    int connfd;
    server_log log;
    struct Time_stats time_stats;

    struct sockaddr_in *from;
};

//struct Queue {
//    struct Task* tasks;
//    int size;
//    int max_size;
//    int head;
//    int tail;
//};

struct Node {
    struct Task task;
    struct Node* next;
    //struct Node* previous;
};

struct Queue{
    struct Node *head;
    struct Node *tail;
    int size;
    int max_size;
};

void enqueue(struct Queue *queue, struct Task task) {
    if (queue->size >= queue->max_size) return;
    struct Node *new_node = malloc(sizeof(struct Node));
    if (!new_node) return;

    new_node->task = task;
    new_node->next = NULL;
    //new_node->previous = queue->head;

    if (queue->head != NULL) {
        queue->head->next = new_node;
    } else {
        queue->tail = new_node;
    }

    queue->head = new_node;
    queue->size++;
}

struct Task dequeue(struct Queue *queue) {
    struct Task empty = {0};
    if (queue == NULL || queue->tail == NULL)
        return empty; //maybe better to throw

    struct Node* temp = queue->tail;
    struct Task task = temp->task;
    queue->tail = queue->tail->next;
    free(temp);
    queue->size--;

    if (queue->tail == NULL)
        queue->head = NULL;
    return task;
}

void initialize_queue(struct Queue *queue, int max_size) {
    queue->head = NULL;
    queue->tail = NULL;
    queue->size = 0;
    queue->max_size = max_size;
}

#endif //QUEUE_H

