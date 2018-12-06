#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>


#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include "handle.h"

class Lockinfo {
public:
  std::string hold_client;
  int status;
  std::list<std::string> wait_client;

  Lockinfo();
};

class lock_server_cache {
 private:
  int nacquire;
  std::map<lock_protocol::lockid_t,struct Lockinfo> lock_map;
  pthread_mutex_t mutex;

 public:
  enum server_lock_status {FREE, LOCKED, REVOKING};
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif