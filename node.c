#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "node.h"

// initialize the list of structs that hold the word/count pairs
// list is put into the buffer
 queue *create_list ( ){
	queue *list = malloc( sizeof( queue ) );
	list->size = 0;
	list->head = list->tail = NULL;
	return list;
}


void enqueue( queue *list, int count, char *word ){
	node *current = ( node *)malloc( sizeof( node ) );
	current->word = malloc( sizeof( char ) *(strlen( word ) + 1 ) ); // strlen does not include null terminator
	current->count = count;

	strcpy( current->word , word );
	current->next = NULL;

	if (list->tail == NULL ){ //empty
		list->head = current;
		list->tail = current;
	} 
	else {
		list->tail->next = current; //point tail to new node
		list->tail = current;
	}
	list->size++;
}

node *end_pop (  queue *list ){
	 node *current = list->head;
	 node *prev = NULL;
	if (current == NULL ){ // empty
		return NULL;
	}
	while( current->next != NULL ) { // iterate to end
		prev = current;
		current = current->next;
	}
	if (current != list->head ) {
		prev->next = NULL;	
		list->tail = prev;
	}
	else {
		list->head = list->tail = NULL; // one element
	}
	list->size--;
	return current;
}

// look for word in list and return node current
// if not in list, return NULL
node *search ( queue *list, char *word ){
	node *current = list->head;
	if (current == NULL )
		return NULL;
	while (current->next ){
		if ( strcmp ( word, current->word ) == 0 ) 
			return current;
		current = current->next;
	}
	return NULL;
}

// print everything in the list and print it to FILE f.
void printToFile (FILE* f, queue *list ) {
	node *current = list->head;
	while (current ){
		fprintf( f, "%s %d\n", current->word, current->count );
		current = current->next;
	}
}