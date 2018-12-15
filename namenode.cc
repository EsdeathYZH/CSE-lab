#include "namenode.h"
#include "extent_client.h"
#include "lock_client_cache.h"
#include <sys/stat.h>
#include <unistd.h>
#include "threader.h"

using namespace std;

void NameNode::init(const string &extent_dst, const string &lock_dst) {
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
  yfs = new yfs_client(extent_dst, lock_dst);
  //yfs = new yfs_client(ec, lc);

  /* Add your init logic here */
  int check_interval = 2;
  NewThread(this, &NameNode::CheckLiveness, check_interval);
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino) {
  printf("GetBlockLocations start\n");fflush(stdout);
  list<blockid_t>block_ids;
  list<LocatedBlock>located_blocks;
  yfs_client::fileinfo attr;
  //get block ids
  ec->get_block_ids(ino, block_ids);
  //get file size
  if(!Getfile(ino, attr)){
    printf("getfile error!\n");fflush(stdout);
  }

  int size = attr.size;
  int block_index = 0;
  for(list<blockid_t>::iterator iter = block_ids.begin(); iter != block_ids.end(); iter++){
    located_blocks.push_back(LocatedBlock(*iter, block_index * BLOCK_SIZE, MIN(attr.size-BLOCK_SIZE*block_index, BLOCK_SIZE), GetDatanodes()));
    block_index++;
    size -= BLOCK_SIZE;  
  }
  return located_blocks;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size) {
  printf("complete start!\n");fflush(stdout);
  int r;
  if((r = ec->complete(ino, new_size)) != extent_protocol::OK){
    printf("complete failed\n");fflush(stdout);
    return false;
  }
  if((r = lc->release(ino)) != lock_protocol::OK){
    printf("release failed\n");fflush(stdout);
    return false;
  }
  return true;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino) {
  printf("appendblock start!\n");fflush(stdout);
  int r;
  blockid_t block_id;
  yfs_client::fileinfo attr;
  if(!Getfile(ino, attr)){
    printf("getattr error!\n");fflush(stdout);
  }
  if((r = ec->append_block(ino, block_id)) != extent_protocol::OK){
    printf("appendblock error!\n");fflush(stdout);
  }
  //add the id to data_blocks
  data_blocks.insert(block_id);
  return LocatedBlock(block_id, ((attr.size + BLOCK_SIZE -1)/BLOCK_SIZE) * BLOCK_SIZE, BLOCK_SIZE, GetDatanodes());
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name) {
  int r;
  if((r = yfs->rename(src_dir_ino, src_name.c_str(), dst_dir_ino, dst_name.c_str())) != yfs_client::OK){
    printf("rename failed\n");fflush(stdout);
    return false;
  }
  printf("rename ok\n");fflush(stdout);
  return true;
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  int r;
  if((r = yfs->mkdir(parent, name.c_str(), 0, ino_out)) != yfs_client::OK){
    printf("mkdir failed\n");fflush(stdout);
    return false;
  }
  printf("mkdir ok\n");fflush(stdout);
  return true;
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  int r;
  printf("create start\n");fflush(stdout);
  if((r = yfs->create(parent, name.c_str(), mode, ino_out)) != yfs_client::OK){
    return false;
  }
  printf("create ok\n");fflush(stdout);
  if((r = lc->acquire(ino_out)) != lock_protocol::OK){
    printf("acquire failed\n");fflush(stdout);
    return false;
  }
  printf("lock ok");fflush(stdout);
  return true;
}

bool NameNode::Isfile(yfs_client::inum ino) {
  return yfs->_isfile(ino);
}

bool NameNode::Isdir(yfs_client::inum ino) {
  return yfs->_isdir(ino);
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info) {
  int r;
  if((r = yfs->_getfile(ino, info)) != yfs_client::OK){
    printf("getfile failed\n");fflush(stdout);
    return false;
  }
  printf("getfile ok\n");fflush(stdout);
  return true;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info) {
  int r;
  if((r = yfs->_getdir(ino, info)) != yfs_client::OK){
    printf("getdir failed\n");fflush(stdout);
    return false;
  }
  printf("getdir ok\n");fflush(stdout);
  return true;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir) {
  int r;
  if((r = yfs->_readdir(ino, dir)) != yfs_client::OK){
    printf("readdir failed\n");fflush(stdout);
    return false;
  }
  printf("readdir ok\n");fflush(stdout);
  return true;
}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino) {
  int r;
  list<blockid_t> unlink_blocks;
  ec->get_block_ids(ino, unlink_blocks);
  if((r = yfs->_unlink(parent, name.c_str())) != yfs_client::OK){
    printf("unlink failed\n");fflush(stdout);
    return false;
  }
  // if((r = lc->release(ino)) != lock_protocol::OK){
  //   printf("release(unlink) failed\n");fflush(stdout);
  //   return false;
  // }
  for(list<blockid_t>::iterator iter = unlink_blocks.begin(); iter != unlink_blocks.end(); iter++){
    data_blocks.erase(*iter);
  }
  printf("unlink ok\n");fflush(stdout);
  return true;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id) {
  printf("receive heartbeat from %s\n", id.hostname().c_str());
  if(liveness_map[id.hostname()] == false){
    printf("a data node is recovery...\n");fflush(stdout);
  }
  liveness_map[id.hostname()] = true;
  heartbeattime_map[id.hostname()] = time(NULL);
}

void NameNode::RegisterDatanode(DatanodeIDProto id) {
  printf("%s register~\n", id.hostname().c_str());fflush(stdout);
  slave_datanodes.insert(id);
  //replicate all blocks created by HDFS
  for(set<blockid_t>::iterator iter = data_blocks.begin(); iter != data_blocks.end(); iter++){
    if(replicablocks_map[id.hostname()].count(*iter) == 0){
      NameNode::ReplicateBlock(*iter, master_datanode, id);
      replicablocks_map[id.hostname()].insert(*iter);
    }
  }
  replica_ok_map[id.hostname()] = true;
}

list<DatanodeIDProto> NameNode::GetDatanodes() {
  list<DatanodeIDProto> live_nodes;
  for(set<DatanodeIDProto>::iterator iter = slave_datanodes.begin(); iter != slave_datanodes.end(); iter++){
    if(liveness_map.count(iter->hostname()) != 0 && liveness_map[iter->hostname()] == true &&
       replica_ok_map.count(iter->hostname()) != 0 && replica_ok_map[iter->hostname()] == true){
       live_nodes.push_back(*iter);
    }
  }
  return live_nodes;
}

void NameNode::CheckLiveness(int check_interval){
  while(true){
    time_t current_time = time(NULL);
    printf("start to check...\n");fflush(stdout);
    list<DatanodeIDProto> liveness_nodes = GetDatanodes();
    for(list<DatanodeIDProto>::iterator iter = liveness_nodes.begin(); iter != liveness_nodes.end(); iter++){
      if((current_time - heartbeattime_map[iter->hostname()]) > 3){
        printf("a datanode is dead!\n");fflush(stdout);
        liveness_map[iter->hostname()] = false;
        replica_ok_map[iter->hostname()] = false;
      }
    }
    sleep(check_interval);
  }
}
