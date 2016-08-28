//test the performance of store_record

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <db.h>
#include <time.h>
#include <inttypes.h>

#define BILLION 1000000000UL
#define MILLION 1000000UL

int main(int argc, char **argv)
{
	uint64_t key = 0;
	struct timespec start_time, end_time;
	uint64_t diff;
	FILE *fp = fopen(argv[1], "w");
	
	initialize_db(0);
	
	while(key < MILLION)
	{
		clock_gettime(CLOCK_MONOTONIC, &start_time);
		store_record(sizeof(uint64_t), &key, sizeof(uint64_t), &key);
		clock_gettime(CLOCK_MONOTONIC, &end_time);
		diff = BILLION * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_nsec - start_time.tv_nsec;
		fprintf(fp, "%"PRIu64"\n", diff);
		
		key++;
	}
	
	return 0;
}