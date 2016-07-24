#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <db.h>
#include <unistd.h>
#include <inttypes.h>
#include <time.h>

#define THREAD_NUM 20
#define COUNT 10 * 100000UL
#define BILLION 1000000000UL
#define FILENAME "timeslice.txt"

void * func(void *arg);
void * func2(void *arg);

uint64_t key = 0;
pthread_spinlock_t spinlock, filelock;
//const char *data = "become legendary";

int main()
{
	int i, ret;
	pthread_t pid[THREAD_NUM];
	//pthread_t retrieve_id;
	FILE *fp = fopen(FILENAME, "w");

	initialize_db(0);
	pthread_spin_init(&spinlock, PTHREAD_PROCESS_PRIVATE);
	pthread_spin_init(&filelock, PTHREAD_PROCESS_PRIVATE);

	for(i = 0;i < THREAD_NUM;i++)
		if(ret = pthread_create(&pid[i], NULL, func, (void *)fp))
		{
			fprintf(stderr, "Thread creation failed: %d\n", i);
			exit(1);
		}
		
	for(i = 0;i < THREAD_NUM;i++)
		pthread_join(pid[i], NULL);

	return 0;
}

void * func(void *arg)
{
	uint64_t thekey;
	FILE *fp = (FILE *)arg;
	//struct timespec start_time, end_time;
	uint64_t diff1, diff2, diff3, diff4;

	while(1)
	{
		pthread_spin_lock(&spinlock);
		if(key == COUNT)
		{
			pthread_spin_unlock(&spinlock);
			break;
		}
		
		thekey = key++;
		pthread_spin_unlock(&spinlock);

		//clock_gettime(CLOCK_MONOTONIC, &start_time);
		store_record(sizeof(uint64_t), &thekey, sizeof(uint64_t), &thekey, &diff1, &diff2, &diff3, &diff4);
		//clock_gettime(CLOCK_MONOTONIC, &end_time);
		//diff = BILLION * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_nsec - start_time.tv_nsec;
		
		pthread_spin_lock(&filelock);
		fprintf(fp, "%"PRIu64"\t%"PRIu64"\t%"PRIu64"\t%"PRIu64"\n", diff1, diff2, diff3, diff4);
		pthread_spin_unlock(&filelock);
	}
}

void * func2(void *arg)
{
	FILE *fp = fopen("test.txt", "w");

	uint64_t rekey = 9876;
	size_t data_size;
	uint64_t *data_data;

	while(1)
	{
		int i = retrieve_record(sizeof(uint64_t), (void *)&rekey, &data_size, (void **)&data_data);

		if(i != 0)
			fprintf(fp, "retrieve error\n");
		else
			fprintf(fp, "%"PRIu64" : %"PRIu64"\n", rekey, *data_data);

		rekey += 1659;
	}
}