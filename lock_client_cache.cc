// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"
#include <unistd.h>


int lock_client_cache::last_port = 0;

ClientLock::ClientLock()
:lock_status(lock_client_cache::NONE), cond(PTHREAD_COND_INITIALIZER), revoke_arrive(false), retry_arrive(false), hold_thread(0)
{}

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu), mutex(PTHREAD_MUTEX_INITIALIZER)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  char hname[100];
  VERIFY(gethostname(hname, sizeof(hname)) == 0);
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
  // pthread_mutex_init(&mutex, NULL) == 0;
  // pthread_cond_init(&cv, NULL) == 0;
}


lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  int r;
  pthread_mutex_lock(&mutex);
  if(lock_cache.count(lid) == 0){
    lock_cache[lid] = ClientLock();
  }
  while(lock_cache[lid].lock_status != FREE){
    //Client don't hold the lock and no other thread is acquiring
    if(lock_cache[lid].lock_status == NONE || lock_cache[lid].lock_status == RELEASING){
      tprintf("%s-%d Acquiring lock..%d\n", id.c_str(), pthread_self(), lock_cache[lid].lock_status);
      lock_cache[lid].lock_status = ACQUIRING;
      pthread_mutex_unlock(&mutex);
      ret = cl->call(lock_protocol::acquire, lid, id, r);
      pthread_mutex_lock(&mutex);
      //if server grant the lock, or retry has arrived, return 
      if(lock_cache[lid].lock_status == FREE || lock_cache[lid].retry_arrive || ret == lock_protocol::OK){
        break;
      }else if(ret == lock_protocol::RETRY){//if return value is RETRY, sleep
        tprintf("%s-%d:sleep and wait\n", id.c_str(), pthread_self());
        pthread_cond_wait(&lock_cache[lid].cond, &mutex);
      }
      else{
        tprintf("impossible!!!\n");
      }
    }else{  //Acquiring or locked, it means other threads are aquiring this lock 
      tprintf("%s-%d:sleep and wait\n", id.c_str(), pthread_self());
      pthread_cond_wait(&lock_cache[lid].cond, &mutex);
    }
  }
  tprintf("%s-%d:get lock\n", id.c_str(), pthread_self());
  //clean the retry flag
  lock_cache[lid].retry_arrive = false;
  lock_cache[lid].lock_status = LOCKED;
  lock_cache[lid].hold_thread = pthread_self();
  pthread_mutex_unlock(&mutex);
  return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  int r;
  pthread_mutex_lock(&mutex);
  if(lock_cache[lid].revoke_arrive){
    //clear flag
    lock_cache[lid].revoke_arrive = false;
    lock_cache[lid].retry_arrive = false;
    lock_cache[lid].lock_status = RELEASING;
    //wake up sleeping client to acquire
    pthread_cond_signal(&lock_cache[lid].cond);
    tprintf("%s-%d:release and give back lock\n", id.c_str(), pthread_self());
    pthread_mutex_unlock(&mutex);
    cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&mutex);
    //if lock status is ACQUIRING, it means during releasing, another thread acquire this lock. 
    if(lock_cache[lid].lock_status == RELEASING){
      lock_cache[lid].lock_status = NONE;
    }
  }else{
    //If no revoke flag, wake a waiting thread
    tprintf("%s-%d:release and wake next thread\n", id.c_str(), pthread_self());
    lock_cache[lid].lock_status = FREE;
    pthread_cond_signal(&lock_cache[lid].cond);
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, int &)
{
  int r;
  //An impossible event
  if(lock_cache[lid].revoke_arrive){
    tprintf("revoke twice on:%s!!\n", id.c_str());
  }
  pthread_mutex_lock(&mutex);
  //Client can not release lock at once
  if(lock_cache[lid].lock_status == ACQUIRING || lock_cache[lid].lock_status == LOCKED || lock_cache[lid].retry_arrive){
    lock_cache[lid].revoke_arrive = true;
    tprintf("%s-%d:receive revoke but not free\n", id.c_str(), pthread_self());
  }else if(lock_cache[lid].lock_status == FREE){ //No thread is using this lock, client release lock at once
    lock_cache[lid].lock_status = RELEASING;
    lock_cache[lid].retry_arrive = false;
    tprintf("%s-%d:receive revoke and free\n", id.c_str(), pthread_self());
    pthread_mutex_unlock(&mutex);
    cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&mutex);
    //if lock status is ACQUIRING, it means during releasing, another thread acquire this lock. 
    if(lock_cache[lid].lock_status == RELEASING){
      lock_cache[lid].lock_status = NONE;
    }
  }else{
    tprintf("impossible!!!!!!!!!\n");
  }
  pthread_mutex_unlock(&mutex);
  return rlock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, int revoke_flag, int& )
{
  if(lock_cache[lid].retry_arrive){
    tprintf("retry twice on:%s!!\n", id.c_str());
    return rlock_protocol::OK;
  }
  if(lock_cache[lid].lock_status == RELEASING || lock_cache[lid].lock_status == LOCKED || lock_cache[lid].lock_status == FREE){
    tprintf("retry granted lock on:%s!!\n", id.c_str());
    return rlock_protocol::OK;
  }
  pthread_mutex_lock(&mutex);
  tprintf("%s-%d:receive retry\n", id.c_str(), pthread_self());
  //Set retry flag and grant lock to client, wake up a client waiting the lock
  if(revoke_flag == 1) lock_cache[lid].revoke_arrive = true;
  lock_cache[lid].retry_arrive = true;
  lock_cache[lid].lock_status = FREE;
  pthread_cond_signal(&lock_cache[lid].cond);
  pthread_mutex_unlock(&mutex);
  return rlock_protocol::OK;
}


