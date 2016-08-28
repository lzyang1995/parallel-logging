#include <db.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdint.h>
#include <inttypes.h>

#define BILLION 1000000000UL
#define MILLION 1000000UL

int main()
{
	DB *dbp;
	int ret;

	ret = db_create(&dbp, NULL, 0);
	if (ret != 0) 
	{
 		/* Error handling goes here */
 		exit(1);
	}

	ret = dbp->open(dbp, NULL, "my_db.db", NULL, DB_BTREE, DB_CREATE, 0);
	if(ret != 0)
	{
		exit(1);
	}

	uint64_t i = 0;
	DBT key, data;
	struct timespec start_time, end_time;
	uint64_t diff;
	FILE *fp = fopen(argv[1], "w");

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	while(i < MILLION)
	{
		key.data = &i;
		key.size = sizeof(i);
		data.data = &i;
		data.size = sizeof(i); 

		clock_gettime(CLOCK_MONOTONIC, &start_time);
		ret = dbp->put(dbp, NULL, &key, &data, DB_AUTO_COMMIT);
		clock_gettime(CLOCK_MONOTONIC, &end_time);
		diff = BILLION * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_nsec - start_time.tv_nsec;
		fprintf(fp, "%"PRIu64"\n", diff);
		
		i++;
	}

	if(dbp != NULL)
		dbp->close(dbp, 0);
}