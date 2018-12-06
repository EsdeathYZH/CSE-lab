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

  /* Add your init logic here */
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino) {
  list<blockid_t>block_ids;
  list<LocatedBlock>located_blocks;
  yfs_client::fileinfo attr;
  //get block ids
  ec->get_block_ids(ino, block_ids);
  //get file size
  if(!Getfile(ino, attr)){
    printf("getfile error!\n");
  }

  int size = attr.size;
  int block_index = 0;
  for(list<blockid_t>::iterator iter = block_ids.begin(); iter != block_ids.end(); iter++){
    located_blocks.push_back(LocatedBlock(*iter, block_index * BLOCK_SIZE, MIN(size-BLOCK_SIZE*block_index, BLOCK_SIZE), master_datanode));
    block_index++;
    size -= BLOCK_SIZE;
  }
  return located_blocks;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size) {
  int r;
  if((r = ec->complete(ino, new_size)) < 0){
    return false;
  }
  if((r = lc->release(ino)) < 0){
    return false;
  }
  return true;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino) {
  int r;
  blockid_t block_id;
  yfs_client::fileinfo attr;
  if(!Getfile(ino, attr)){
    printf("getattr error!\n");
  }
  if((r = ec->append_block(ino, block_id)) < 0){
    printf("appendblock error!\n");
  }
  //add the id to data_blocks
  data_blocks.push_back(block_id);
  return LocatedBlock(block_id, ((attr.size + BLOCK_SIZE -1)/BLOCK_SIZE) * BLOCK_SIZE, BLOCK_SIZE, master_datanode);
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name) {
  int r;
  yfs_client::inum src_ino, dst_ino;
  if((r = yfs->rename(src_dir_ino, src_name.c_str(), dst_dir_ino, dst_name.c_str())) < 0){
    return false;
  }
  return true;
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  int r;
  if((r = yfs->mkdir(parent, name.c_str(), 0, ino_out)) < 0){
    return false;
  }
  return true;
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  int r;
  printf("create start\n");
  fflush(stdout);
  if((r = yfs->create(parent, name.c_str(), 0, ino_out)) < 0){
    return false;
  }
  printf("create ok");
  fflush(stdout);
  if((r = lc->acquire(ino_out)) != lock_protocol::OK){
    return false;
  }
  return true;
}

bool NameNode::Isfile(yfs_client::inum ino) {
  int r;
  if((r = yfs->_isfile(ino)) != yfs_client::OK){
    return false;
  }
  return true;
}

bool NameNode::Isdir(yfs_client::inum ino) {
  int r;
  if((r = yfs->_isdir(ino)) != yfs_client::OK){
    return false;
  }
  return true;
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info) {
  int r;
  if((r = yfs->_getfile(ino, info)) != yfs_client::OK){
    return false;
  }
  return true;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info) {
  int r;
  if((r = yfs->_getdir(ino, info)) != yfs_client::OK){
    return false;
  }
  return true;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir) {
  int r;
  if((r = yfs->_readdir(ino, dir)) != yfs_client::OK){
    return false;
  }
  return true;
}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino) {
  int r;
  if((r = yfs->_unlink(parent, name.c_str())) != yfs_client::OK){//TODO: we don't use ino here!!!
    return false;
  }
  return true;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id) {
}

void NameNode::RegisterDatanode(DatanodeIDProto id) {
  // slave_datanodes.push_back(id);
  // //replicate all blocks created by HDFS
  // for(list<blockid_t>::iterator iter = data_blocks.begin(); iter != data_blocks.end(); iter++){
  //     NameNode::ReplicateBlock(*iter, master_datanode, id);
  // }
  // liveness_map.insert(pair<string, bool>(id.datanodeuuid(), true));
  
}

list<DatanodeIDProto> NameNode::GetDatanodes() {
  // list<DatanodeIDProto> live_nodes;
  // for(list<DatanodeIDProto>::iterator iter = slave_datanodes.begin(); iter != slave_datanodes.end(); iter++){
  //   if(liveness_map[iter->datanodeuuid()]){
  //     live_nodes.push_back(*iter);
  //   }
  // }
  // return live_nodes;
  return list<DatanodeIDProto>();
}
