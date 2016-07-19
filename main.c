#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

void * func(void *arg);

int key;
pthread_spinlock_t spinlock;
const char *data = "become legendary";

int main()
{
	int i, ret;
	pthread_t pid[5];

	initialize_db(0);
	key = 0;
	pthread_spin_init(&spinlock, PTHREAD_PROCESS_PRIVATE);

	for(i = 0;i < 5;i++)
		if(ret = pthread_create(&pid[i], NULL, func, NULL))
		{
			fprintf(stderr, "Thread creation failed: %d\n", i);
			exit(1);
		}

	while(1);

	return 0;
}

void * func(void *arg)
{
	int thekey;

	while(1)
	{
		pthread_spin_lock(&spinlock);
		thekey = key++;
		pthread_spin_unlock(&spinlock);

		store_record(sizeof(int), &thekey, 17, data);
	}
}