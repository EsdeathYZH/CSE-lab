// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0), mutex(PTHREAD_MUTEX_INITIALIZER), cond_val(PTHREAD_COND_INITIALIZER)
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
	// Your lab2 part2 code goes here
  pthread_mutex_lock(&mutex);
  if(lock_map.count(lid) == 0){
    lock_map.insert(std::pair<lock_protocol::lockid_t,int>(lid,FREE));
  }
  while (lock_map.at(lid) == LOCKED){
    pthread_cond_wait(&cond_val, &mutex);
  }
  lock_map[lid] = LOCKED;
  nacquire++;
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here
  pthread_mutex_lock(&mutex);
  if(lock_map.count(lid) != 0 && lock_map[lid] == LOCKED){
    lock_map[lid] = FREE;
    nacquire--;
  }
  pthread_cond_signal(&cond_val);
  pthread_mutex_unlock(&mutex);
  return ret;
}
