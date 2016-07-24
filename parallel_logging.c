#include <sys/stat.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <db.h>
#include <pthread.h>
#include <inttypes.h>
#include <unistd.h>

#define DEBUG
//#define USE_ENV
#define ERROR 1
#define NAME_LENGTH 31
#define ARRAY_SIZE 4
//ARRAY_SIZE shoule be an even number

//constants
const char *db_path = "./DB";
const char *dbname_prefix = "node_test_";
const int MAX_RES = 1;		//notice 2
const int MAX_PUT = 100000;

//struct definition
typedef struct _db_info
{
	DB *dbp;
	char name[NAME_LENGTH];
}db_info;

//global variables
#ifdef USE_ENV
DB_ENV *db_env;
#endif
db_info bdb_array[ARRAY_SIZE];
struct
{
	db_info *slot_ptr;
	int slot_pos;
}store_db;						//notice : store_db is not a pointer but a struct
struct
{
	DB **all_db_handle;		//notice 1
	uint64_t sum;			//notice 5
}all_db;
int res;
int put_number;
pthread_mutex_t mutex, alldb_mtx;
pthread_cond_t empty, full;
pthread_rwlock_t rwlock;
pthread_spinlock_t pn_lock;		//notice 4

//function declaration
void initialize_db(uint32_t flag);
void * db_manage(void *arg);
void consume();
void switch_slot();
int store_record(size_t key_size,void *key_data,size_t data_size,void *data);
void close_db();
int retrieve_record(size_t key_size,void *key_data,size_t *data_size,void **data);
void warm_up(DB *dbp);

void initialize_db(uint32_t flag)
{
	int ret, i;
	pthread_t db_manage_id;
	char tmp[10];


	if(ret = mkdir(db_path, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH))
	{
		fprintf(stderr, "DB : Dir Creation failed: %s\n", strerror(errno));
		exit(ERROR);
	}

#ifdef USE_ENV
	if(ret = db_env_create(&db_env, 0))
	{
		fprintf(stderr, "DB : Error creating env handle: %s\n", db_strerror(ret));
		exit(ERROR);
	}

	if(ret = db_env->open(db_env, db_path, DB_CREATE|DB_INIT_CDB|DB_INIT_MPOOL|DB_THREAD, 0))
	{
		fprintf(stderr, "DB : Environment open failed: %s\n", db_strerror(ret));
		exit(ERROR);
	}
#else
	if(ret = chdir(db_path))
	{
		fprintf(stderr, "DB : Dir Creation failed: %s\n", strerror(errno));
		exit(ERROR);
	}
#endif

	all_db.all_db_handle = (DB **)malloc(ARRAY_SIZE * sizeof(DB *));
	all_db.sum = ARRAY_SIZE;
	for(i = 0;i < ARRAY_SIZE;i++)
	{
		if(ret = db_create(&bdb_array[i].dbp,
#ifdef USE_ENV
							db_env,
#else
							NULL, 
#endif
							flag))
		{
			fprintf(stderr, "DB : Database %d Creation failed: %s\n", i, db_strerror(ret));
			exit(ERROR);
		}

		memcpy(bdb_array[i].name, dbname_prefix, strlen(dbname_prefix));
		sprintf(tmp, "%d", i);
		memcpy(bdb_array[i].name + strlen(dbname_prefix), tmp, strlen(tmp));
		bdb_array[i].name[strlen(dbname_prefix) + strlen(tmp)] = '\0';

		if(ret = bdb_array[i].dbp->open(bdb_array[i].dbp, NULL, bdb_array[i].name, NULL, DB_BTREE, DB_THREAD|DB_CREATE, 0))
		{
			fprintf(stderr, "DB : Database %d Open failed: %s\n", i, db_strerror(ret));
			exit(ERROR);
		}
		
		warm_up(bdb_array[i].dbp);

#ifdef DEBUG
		printf("node_test_%d created.\n", i);
#endif
		all_db.all_db_handle[i] = bdb_array[i].dbp;
	}

	store_db.slot_ptr = bdb_array;
	store_db.slot_pos = 0;

	res = MAX_RES;
	put_number = 0;

	pthread_cond_init(&empty, NULL);
	pthread_cond_init(&full, NULL);
	pthread_rwlock_init(&rwlock, NULL);
	pthread_spin_init(&pn_lock, PTHREAD_PROCESS_PRIVATE);

	//create db_manage thread
	if(ret = pthread_create(&db_manage_id, NULL, db_manage, (void *)flag))
	{
		fprintf(stderr, "DB : db_manage thread creation failed: %s\n", strerror(errno));
		exit(ERROR);
	}

	return;
}

void * db_manage(void *arg)
{
	int ret, i;
	uint32_t flag = (uint32_t)arg;
	int start_index;
	DB **tmp;
	char str[10];
	pthread_t db_manage_id;

	while(1)
	{
		pthread_mutex_lock(&mutex);
		while(res == MAX_RES)
			pthread_cond_wait(&empty, &mutex);

		//create new databases and update bdb_array
		pthread_mutex_lock(&alldb_mtx);

		if((all_db.sum / (ARRAY_SIZE / 2)) % 2 == 0)
			start_index = 0;
		else
			start_index = ARRAY_SIZE / 2;

		tmp = (DB **)malloc(sizeof(DB *) * (all_db.sum + ARRAY_SIZE / 2));
		for(i = 0;i < all_db.sum;i++)
		{
			tmp[i] = all_db.all_db_handle[i];
		}
		free(all_db.all_db_handle);		//notice
		all_db.all_db_handle = tmp;

		for(i = start_index;i < start_index + ARRAY_SIZE / 2;i++)
		{
			if(ret = db_create(&bdb_array[i].dbp,
#ifdef USE_ENV
								db_env,
#else
								NULL, 
#endif
								flag))
			{
				fprintf(stderr, "DB : Database %"PRIu64" Creation failed: %s\n", all_db.sum, db_strerror(ret));
				exit(ERROR);
			}

			sprintf(str, "%"PRIu64"", all_db.sum);
			memcpy(bdb_array[i].name + strlen(dbname_prefix), str, strlen(str));
			bdb_array[i].name[strlen(dbname_prefix) + strlen(str)] = '\0';

			if(ret = bdb_array[i].dbp->open(bdb_array[i].dbp, NULL, bdb_array[i].name, NULL, DB_BTREE, DB_THREAD|DB_CREATE, 0))
			{
				fprintf(stderr, "DB : Database %"PRIu64" Open failed: %s\n", all_db.sum, db_strerror(ret));
				exit(ERROR);
			}
			
			warm_up(bdb_array[i].dbp);
#ifdef DEBUG
			printf("node_test_%"PRIu64" created.\n", all_db.sum);
#endif

			all_db.all_db_handle[all_db.sum] = bdb_array[i].dbp;
			all_db.sum++;
		}
		pthread_mutex_unlock(&alldb_mtx);

		//finished

		res++;
		pthread_cond_signal(&full);
		pthread_mutex_unlock(&mutex);
	}
}

void consume()
{
	pthread_mutex_lock(&mutex);
	while(res == 0)
		pthread_cond_wait(&full, &mutex);
	switch_slot();
	res--;
	pthread_cond_signal(&empty);
	pthread_mutex_unlock(&mutex);
}

void switch_slot()
{
	store_db.slot_pos = (1 + store_db.slot_pos) % ARRAY_SIZE;

	pthread_rwlock_wrlock(&rwlock);
	store_db.slot_ptr = &bdb_array[store_db.slot_pos];
	pthread_rwlock_unlock(&rwlock);

	pthread_spin_lock(&pn_lock);
	put_number = 0;
	pthread_spin_unlock(&pn_lock);
}

int store_record(size_t key_size,void *key_data,size_t data_size,void *data)
{
	int ret = 1;
	DBT key,db_data;
	db_info *pdb_info;

	memset(&key,0,sizeof(key));
	memset(&db_data,0,sizeof(db_data));
	key.data = key_data;
	key.size = key_size;
	db_data.data = data;
	db_data.size = data_size;

	pthread_rwlock_rdlock(&rwlock);
	pdb_info = store_db.slot_ptr;
	pthread_rwlock_unlock(&rwlock);

	if(ret = pdb_info->dbp->put(pdb_info->dbp, NULL, &key, &db_data, DB_NOOVERWRITE))
	{
		fprintf(stderr, "DB : Store record failed: %s\n", db_strerror(ret));
	}
	else
	{
		pthread_spin_lock(&pn_lock);
		put_number++;
		pthread_spin_unlock(&pn_lock);

		if (put_number == MAX_PUT)
		{
			if ((store_db.slot_pos + 1) % (ARRAY_SIZE / 2) == 0)
				consume();
			else
				switch_slot();
		}
	}

	return ret;
}

//what will happen if it is called?
//notice
void close_db()
{
	pthread_mutex_lock(&alldb_mtx);

	//close all DB handles
#ifdef USE_ENV
	db_env->close(db_env, DB_FORCESYNC);
#else
	uint64_t i;
	for(i = 0;i < all_db.sum;i++)
		if(all_db.all_db_handle[i] != NULL)
			all_db.all_db_handle[i]->close(all_db.all_db_handle[i], 0);
#endif

	//destroy the sychronization variables
	pthread_cond_destroy(&empty);
	pthread_cond_destroy(&full);
	pthread_rwlock_destroy(&rwlock);
	pthread_spin_destroy(&pn_lock);

	//free all_db_handle
	free(all_db.all_db_handle);

	pthread_mutex_unlock(&alldb_mtx);

	return;
}

//notice : has the buffer be allocated before calling this function? 
int retrieve_record(size_t key_size,void *key_data,size_t *data_size,void **data)
{
	int ret = 1;
	DBT key,db_data;
	db_info *pdb_info;
	int i, index;

	memset(&key,0,sizeof(key));
	memset(&db_data,0,sizeof(db_data));

	key.data = key_data;
	key.size = key_size;

	db_data.flags = DB_DBT_MALLOC;

	pthread_rwlock_rdlock(&rwlock);
	pdb_info = store_db.slot_ptr;
	pthread_rwlock_unlock(&rwlock);

	index = atoi(pdb_info->name + strlen(dbname_prefix));

	pthread_mutex_lock(&alldb_mtx);
	for(i = index;i >= 0;i--)
	{
		if(ret = all_db.all_db_handle[i]->get(all_db.all_db_handle[i], NULL, &key, &db_data, 0))
		{
			if(ret == DB_NOTFOUND)
				continue;
			else
			{
				fprintf(stderr, "DB : Retrieve record failed: %s\n", db_strerror(ret));
				return ret;
			}
		}
		else
			break;
	}
	pthread_mutex_unlock(&alldb_mtx);

	if(i < 0)
		return DB_NOTFOUND;
	else
	{
		//not sure whether the following is correct
		*data_size = db_data.size;
		*data = db_data.data;
		return 0;
	}
}

void warm_up(DB *dbp)
{
	DBT key, data;
	uint64_t key_data = 0, data_data = 0;
	
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	
	key.size = sizeof(uint64_t);
	key.data = &key_data;
	data.size = sizeof(uint64_t);
	data.data = &data_data;
	
	dbp->put(dbp, NULL, &key, &data, DB_AUTO_COMMIT);
	dbp->del(dbp, NULL, &key, 0);
}