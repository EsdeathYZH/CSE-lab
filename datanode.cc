#include "datanode.h"
#include <arpa/inet.h>
#include "extent_client.h"
#include <unistd.h>
#include <algorithm>
#include "threader.h"

using namespace std;

int DataNode::init(const string &extent_dst, const string &namenode, const struct sockaddr_in *bindaddr) {
  ec = new extent_client(extent_dst);

  // Generate ID based on listen address
  id.set_ipaddr(inet_ntoa(bindaddr->sin_addr));
  id.set_hostname(GetHostname());
  id.set_datanodeuuid(GenerateUUID());
  id.set_xferport(ntohs(bindaddr->sin_port));
  id.set_infoport(0);
  id.set_ipcport(0);

  // Save namenode address and connect
  make_sockaddr(namenode.c_str(), &namenode_addr);
  if (!ConnectToNN()) {
    delete ec;
    ec = NULL;
    return -1;
  }

  // Register on namenode
  if (!RegisterOnNamenode()) {
    delete ec;
    ec = NULL;
    close(namenode_conn);
    namenode_conn = -1;
    return -1;
  }

  /* Add your initialization here */
  int interval = 1;
  NewThread(this, &DataNode::keepSendHeartbeat, interval);
  
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
  return 0;
}

void DataNode::keepSendHeartbeat(int interval){
  bool heartbeat_response = true;
  while(heartbeat_response){
    printf("send a heartbeat..\n");fflush(stdout);
    heartbeat_response = SendHeartbeat();
    sleep(interval);
  }
}

bool DataNode::ReadBlock(blockid_t bid, uint64_t offset, uint64_t len, string &buf) {
  /* Your lab4 part 2 code */
  printf("read_block start!\n");fflush(stdout);
  if(offset >= BLOCK_SIZE){
    printf("offset invalid!\n");fflush(stdout);
    return true;
  }
  int r;
  string block_content;
  if((r = ec->read_block(bid, block_content)) != extent_protocol::OK){
    printf("read_block(in read) failed!\n");fflush(stdout);
    return false;
  }
  buf = block_content.substr(offset, len);
  return true;
}

bool DataNode::WriteBlock(blockid_t bid, uint64_t offset, uint64_t len, const string &buf) {
  /* Your lab4 part 2 code */
  printf("writeblock start!\n");fflush(stdout);
  int r;
  string block_content;
  if((r = ec->read_block(bid, block_content)) != extent_protocol::OK){
    printf("read_block(in write) failed!\n");
    fflush(stdout);
    return false;
  }
  if(offset >= BLOCK_SIZE){
    printf("offset invalid(in write)!\n");fflush(stdout);
    return true;
  }
  block_content = block_content.replace(offset, len, buf);
  if((r = ec->write_block(bid, block_content)) != extent_protocol::OK){
    printf("writeblock failed!\n");
    fflush(stdout);
    return false;
  }
  return true;
}

