#include <stdlib.h>

// From Proj1
typedef struct node {
    int count;
    char *word;
    struct node *next;
}node;

typedef struct queue
{
    node *head;
    node *tail;
    int size;
}queue;

queue *create_list ();
void enqueue(queue *q, int count, char *word);
node *end_pop ( queue *q);
node *search ( queue *q, char *word);
void printToFile (FILE* f, queue *q);