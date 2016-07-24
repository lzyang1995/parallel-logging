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
DB * bdb;

int main(int argc, char **argv)
{	
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
		{
			pthread_spin_unlock(&spinlock);
			break;
		}
		
		storekey = ++key;
		pthread_spin_unlock(&spinlock);
		
		DBT key, data;
		memset(&key, 0, sizeof(DBT));
		memset(&data, 0, sizeof(DBT));
		
		key.data = &storekey;
		key.size = sizeof(uint64_t);
		data.data = &storekey;
		data.size = sizeof(uint64_t);
		
		bdb->put(bdb, NULL, &key, &data, DB_AUTO_COMMIT);
	}
}

void * func2(void *arg)
{
	FILE *fp = fopen(FILENAME, "w");
	int ret;
	uint64_t i;
	DBT key, data;
	
	for(i = 1;i <= COUNT;i++)
	{
		memset(&key, 0, sizeof(DBT));
		memset(&data, 0, sizeof(DBT));
		
		key.data = &i;
		key.size = sizeof(uint64_t);
		
		data.flags = DB_DBT_MALLOC;
		
		if(ret = bdb->get(bdb, NULL, &key, &data, 0))
			fprintf(fp, "%"PRIu64":ERROR: %s\n", i, db_strerror(ret));
		else
			fprintf(fp, "%"PRIu64":%"PRIu64"\n", i, *((uint64_t *)data.data));
	}
}