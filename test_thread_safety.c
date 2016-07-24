#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <db.h>
#include <inttypes.h>
#include <errno.h>
#include <string.h>

#define COUNT 10000
#define THREAD_NUM 5
#define ERROR 1
#define FILENAME "retrieve.txt"

void * func(void *arg);
void * func2(void *arg);

uint64_t key = 0;
pthread_spinlock_t spinlock;


int main(int argc, char **argv)
{	
	DB * bdb;
	int ret, i;
	
	if(ret = db_create(&bdb, NULL, 0))
	{
		fprintf(stderr, "DB creation error: %s.\n", db_strerror(ret));
		exit(ERROR);
	}
	
	if(ret = bdb->open(bdb, NULL, "mydb", NULL, DB_BTREE, DB_CREATE | DB_THREAD, 0))
	{
		fprintf(stderr, "DB open error: %s.\n", db_strerror(ret));
		exit(ERROR);
	}
	
	pthread_spin_init(&spinlock, PTHREAD_PROCESS_PRIVATE);
	pthread_t pid[THREAD_NUM];
	for(i = 0;i < THREAD_NUM;i++)
		if(ret = pthread_create(&pid[i], NULL, func, NULL))
		{
			fprintf(stderr, "Storing thread creation failed: %s\n", strerror(errno));
			exit(ERROR);
		}
	
	for(i = 0;i < THREAD_NUM;i++)
		pthread_join(pid[i], NULL);
	
	pthread_t reid;
	if(ret = pthread_create(&reid, NULL, func2, NULL))
	{
		fprintf(stderr, "Retrieving thread creation failed: %s\n", strerror(errno));
		exit(ERROR);
	}
	
	pthread_join(reid, NULL);
		
	return 0;
}

void * func(void *arg)
{
	uint64_t storekey;
	
	while(1)
	{
		pthread_spin_lock(&spinlock);
		if(key == COUNT)
			break;
		
		storekey = ++key;
		pthread_spin_unlock(&spinlock);
		
		
		store_record(sizeof(uint64_t), &storekey, sizeof(uint64_t), &storekey);
	}
}

void * func2(void *arg)
{
	FILE *fp = fopen(FILENAME, "w");
	int ret;
	uint64_t i;
	size_t datasize;
	uint64_t *data;
	
	for(i = 1;i <= COUNT;i++)
	{
		if(ret = retrieve_record(sizeof(uint64_t), &i, &datasize, &data))
			fprintf(fp, "%"PRIu64":ERROR: %s\n", i, db_strerror(ret));
		else
			fprintf(fp, "%"PRIu64":%"PRIu64"\n", i, *data);
	}
}