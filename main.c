#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <db.h>
#include <unistd.h>
#include <inttypes.h>

void * func(void *arg);
void * func2(void *arg);

uint64_t key;
pthread_spinlock_t spinlock;
//const char *data = "become legendary";

int main()
{
	int i, ret;
	pthread_t pid[5];
	pthread_t retrieve_id;

	initialize_db(0);
	key = 0;
	pthread_spin_init(&spinlock, PTHREAD_PROCESS_PRIVATE);

	for(i = 0;i < 5;i++)
		if(ret = pthread_create(&pid[i], NULL, func, NULL))
		{
			fprintf(stderr, "Thread creation failed: %d\n", i);
			exit(1);
		}

	usleep(3000000L);

	if(ret = pthread_create(&retrieve_id, NULL, func2, NULL))
	{
		fprintf(stderr, "Thread func2 creation failed");
		exit(1);
	}

	while(1);

	return 0;
}

void * func(void *arg)
{
	uint64_t thekey;

	while(1)
	{
		pthread_spin_lock(&spinlock);
		thekey = key++;
		pthread_spin_unlock(&spinlock);

		store_record(sizeof(uint64_t), &thekey, sizeof(uint64_t), &thekey);
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