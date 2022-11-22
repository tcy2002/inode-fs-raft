// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  mutex(PTHREAD_MUTEX_INITIALIZER),
  cond(PTHREAD_COND_INITIALIZER),
  nacquire(0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
    pthread_mutex_lock(&mutex);
    if (locks.count(lid) == 0) {
        printf("%d, add a lock: %lld\n",clt , lid);
        locks.emplace(lid, LOCKED);
    } else if (locks[lid] == FREE) {
        printf("%d, get a lock: %lld\n",clt , lid);
        locks[lid] = LOCKED;
    } else {
        // wait
        printf("%d, wait a lock: %lld\n",clt , lid);
        while (locks[lid] == LOCKED)
            pthread_cond_wait(&cond, &mutex);
        printf("%d, get a lock after waiting: %lld\n",clt , lid);
        locks[lid] = LOCKED;
    }
    pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
    printf("%d, release a lock: %lld\n",clt , lid);
    pthread_mutex_lock(&mutex);
    locks[lid] = FREE;
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mutex);
  return ret;
}