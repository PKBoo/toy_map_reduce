// toy_map_reduce
// for usage directions read README
// test
#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <pthread.h>
#include <string.h>
#include "node.h"	

// Global Variables
int THREADS; // 1 5 10
int BUFSIZE; // 1 10 100
queue** listarray; // shared buffer between adder and reducer
int *adderstatus; // keep track of adders that have finished

void *reader (void *args);
void *adder (void *adata);
void *reducer (void *args);

// structs for pthreads
///////////////////////
typedef struct rdata_t {
	char* chunk;
	int index;
} readerData; // pass into pthread as argument

typedef struct adata_t {
	pthread_mutex_t lock;
	int index;
	int wasRead;
	queue *adderlist;
} adderData;

// define to use a min function 
int min(int a, int b) {
	if (a < b) {
		return a;
	} 
	else {
		return b;
	}
}

void *reader (void *args){
	// cast passed data back to original type
	readerData *rdata = (readerData *)args; // rdata 
	int index = rdata->index;
	char *chunk = rdata->chunk;
	queue *wordlist = create_list();

	// printf("Reader %d Created\n", index);

	pthread_mutex_t lock;
	pthread_mutex_init(&lock, NULL);

	// put data in adder struct for pthread parameter
	adderData *adata = malloc(sizeof(adderData));
	adata->adderlist = wordlist;
	adata->index = index;
	adata->wasRead = 0;
	adata->lock = lock;

	pthread_t adderthread;
	pthread_create(&adderthread, NULL, adder, adata);

	// parse chunk for words and store them into the wordlist
	char *ptr = strtok(chunk, " \n\r\t;:.!?',\"-\0");
	while (ptr != NULL){
		while(wordlist->size == BUFSIZE);

		pthread_mutex_lock(&lock);
		enqueue (wordlist, 1, ptr); // critical section
		pthread_mutex_unlock(&lock);
		ptr = strtok(NULL,  " \n\r\t;:.!?',\"-\0");
	}

	adata->wasRead = 1;
	// printf("Reader %d Completed\n", index);
	pthread_exit(NULL);
}

void *adder (void *args) {
	// cast passed data back to original type
	adderData *adata = ( adderData *)args;
	int index = adata->index;
	queue* wordlist = adata->adderlist;
	pthread_mutex_t lock = adata->lock;

	// printf("Adder %d Created\n", index);

	while(!adata->wasRead || wordlist->size > 0){ // make sure 
		while ( wordlist->size == 0 ); // spin until there's something in the 

		pthread_mutex_lock(&lock);	
		node* dict = end_pop(wordlist);
	 	pthread_mutex_unlock(&lock);
	 	// printf("popped %s\n", dict->word);	
		node* foundnode = search(listarray[index], dict->word);

	 	if (foundnode == NULL) {
	 		enqueue(listarray[index], dict->count, dict->word);
	 	}
	 	else {
	 		foundnode->count += dict->count;
	 	}
	 	free(dict);
	}

	adderstatus[index] = 1;
	// printf("Adder %d Completed\n", index);
	pthread_exit(NULL);
}

void *reducer (void *args) {
	// printf("Reducer Created\n");
	char* filename = args;

	int i;
	for (i = 0; i < THREADS; i++) {
		while(!adderstatus[i]);

	}
	queue* finalbuf = create_list();
	for (i = 0; i < THREADS; i++) {
		while(listarray[i]->size > 0) {
			node* dict = end_pop(listarray[i]);

			node* foundnode = search(finalbuf, dict->word);
		 	if (foundnode == NULL) {
		 		enqueue(finalbuf, dict->count, dict->word);
		 	}
		 	else {
		 		foundnode->count += dict->count;
		 	}
		 	free(dict);
		}

	}
	FILE* f = fopen("output.txt", "w+");
	printToFile(f, finalbuf);
	close(f);
	// printf("%d\n", finalbuf->size);
	pthread_exit(NULL);
}

main(int argc, char *argv[]){

	if (argc != 4){
		printf("%d\n", argc);
		perror("Please enter the input file, then the number of threads, then the buffer size");
		printf("ie: ./mr FILE_NAME 10 100\n", );
		exit(1);
	}

	FILE * file;
	BUFSIZE = atoi(argv[3]);
	THREADS = atoi(argv[2]);

	// allocate memory
	pthread_t *readerthread = malloc(sizeof(pthread_t) * THREADS);
	listarray = malloc(sizeof(queue *) * THREADS);
	adderstatus = malloc(sizeof(int) * THREADS);

	// initialize 
	int i;
	for (i = 0; i < THREADS; i++) {
		adderstatus[i] = 0;
		listarray[i] = create_list();
	}

	file = fopen(argv[1], "r+");
	if (file == NULL ){
		perror("read error");
		exit(1);
	}
	
	// get filesize for mmap
	fseek(file,0L,SEEK_END);
	int length = ftell(file);
	fseek(file, 0L, SEEK_SET);
	int fd = fileno(file);
	
	char *buf = mmap(0, length, PROT_READ, MAP_PRIVATE, fd, 0);

	readerData* readerDataArray = malloc(sizeof(readerData) * THREADS);

	i = 0;
	int MIN_CHUNK_SIZE = length / THREADS;
	int index = 0; // thread number
	for (i = 0;i < length && index < THREADS;) { // loop until all of the file is read
		char *chunk;
		int chunksize = MIN_CHUNK_SIZE;
		int start = i;
		int end = min(start + MIN_CHUNK_SIZE, length);

		// get chunksize by iterating to end of word from MIN_START_SIZE
		while (end != length && buf[end] != ' ' && buf[end] != '\0' && buf[end] != '\n') {
			end++;
			chunksize++;
		}
		i = end;

		chunk = malloc(sizeof(char) * (chunksize + 1));
		
		int j;
		for (j = start; j < start + chunksize; j++) {
			chunk[j - start] = buf[j];
		}
		chunk[chunksize] = '\0';

		// put data into the readerData struct to pass into each reader thread
		readerDataArray[index].chunk = chunk;
		readerDataArray[index].index = index;

		pthread_create(&readerthread[index], NULL, reader, &(readerDataArray[index]));
		index++;
	}

	// if all adder threads aren't used after adder completes, force all 
	// adder threads to have completed status so reducer doesn't wait indefinitely
	while (index < THREADS) {
		adderstatus[index] = 1;
		index++;
	}

	close(fd);
	pthread_t reducerthread;

	pthread_create(&reducerthread, NULL, reducer, NULL);
	pthread_exit(NULL);
	return 0;
}
