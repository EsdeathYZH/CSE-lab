// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

Lockinfo::Lockinfo()
: status(lock_server_cache::FREE)
{
}

lock_server_cache::lock_server_cache() : mutex(PTHREAD_MUTEX_INITIALIZER)
{
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, int &)
{
    lock_protocol::status ret = lock_protocol::OK;
    pthread_mutex_lock(&mutex);
    tprintf("%s acquire %d\n", id.c_str(), (int)lid);
    if(lock_map.count(lid) == 0){
      lock_map[lid] = Lockinfo();
    }
    //if no client hold the lock, grant the lock to client
    if(lock_map[lid].status == FREE){
        lock_map[lid].status = LOCKED;
        lock_map[lid].hold_client = id;
        ret = lock_protocol::OK;
    }else if(lock_map[lid].wait_client.size() == 0){//if no other client is waiting, send a revoke to hold_client and return RETRY
        tprintf("%s start to wait\n", id.c_str());
        lock_map[lid].wait_client.push_back(id);
        handle hold_handle = handle(lock_map[lid].hold_client);
        rpcc *cl = hold_handle.safebind();
        int rret = rlock_protocol::OK;
        if(cl) {
            tprintf("revoke %s\n", lock_map[lid].hold_client.c_str());
            pthread_mutex_unlock(&mutex);
            rret = cl->call(rlock_protocol::revoke, lid, ret);
            pthread_mutex_lock(&mutex);
        }
        if(!cl || rret != rlock_protocol::OK) {    
            // handle failure
            tprintf("%s bind error1:%s!!!\n", lock_map[lid].hold_client.c_str(), id.c_str());
        }
        ret = lock_protocol::RETRY;
    }else{//if some clients are waiting, push the client into queue and return a RETRY
      tprintf("%s start to wait\n", id.c_str());
      lock_map[lid].wait_client.push_back(id);
      ret = lock_protocol::RETRY;
    }
    pthread_mutex_unlock(&mutex);
    return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, int &)
{
    lock_protocol::status ret = lock_protocol::OK;
    int r;
    int revoke_flag = 0;
    pthread_mutex_lock(&mutex);
    tprintf("%s release %d\n", id.c_str(), (int)lid);
    //hold_client mismatch release sender, it's an impossible event.
    if(lock_map[lid].hold_client != id){
        tprintf("Client mismatch,hold:%s, release:%s!!!\n", lock_map[lid].hold_client.c_str(), id.c_str());
        pthread_mutex_unlock(&mutex);
        return lock_protocol::OK;
    }
    //no client is waiting this lock, it's an impossible event.
    if(lock_map[lid].wait_client.empty()){
        tprintf("No wait client, release:%s!!!\n", lock_map[lid].hold_client.c_str(), id.c_str());
        pthread_mutex_unlock(&mutex);
        return lock_protocol::OK;
    }
    //pop the first client and grant the lock to it(send it a retry)
    handle next_client = handle(lock_map[lid].wait_client.front());
    lock_map[lid].hold_client = lock_map[lid].wait_client.front();
    lock_map[lid].wait_client.pop_front();
    rpcc* lc = next_client.safebind();
    if(lc){
        //if there are two or more client waiting, it means we need to send a retry signal along witth a revoke signal
        if(lock_map[lid].wait_client.size()>0){
            revoke_flag = 1;
            tprintf("retry %s with flag\n", lock_map[lid].hold_client.c_str());
            pthread_mutex_unlock(&mutex);
            lc->call(rlock_protocol::retry, lid, revoke_flag, r);
            pthread_mutex_lock(&mutex);
        }else{
            tprintf("retry %s without flag\n", lock_map[lid].hold_client.c_str());
            pthread_mutex_unlock(&mutex);
            lc->call(rlock_protocol::retry, lid, revoke_flag, r);
            pthread_mutex_lock(&mutex);   
        }
    }
    if(!lc){
        tprintf("bind error!!!\n");
    }
    pthread_mutex_unlock(&mutex);
    return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}
