#include "inode_manager.h"
#include "time.h" 
#include <math.h>

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  uint reserve_block_num = (sb.nblocks)/BPB + 2 + INODE_NUM*IPB;
  char bitblock[BLOCK_SIZE];
  for(int i=0; i<BLOCK_NUM/BPB; i++){
    read_block(i+2,bitblock);
    for(int j=0; j<BLOCK_SIZE; j++){
      if(bitblock[j] != -1){
        int offset = 0;
        uint temp = (uint)bitblock[j];
        //We can't alloc the reserved blocks
        while(offset<8 && (temp%2!=0 || BPB*i + j*8 + offset < reserve_block_num)){
          offset++;
          temp = temp/2;
        }
        if(BPB*i + j*8 + offset < reserve_block_num || offset == 8){
          continue;
        }
        //Mark the offset and write back
        bitblock[j] |= (1<<offset);
        printf("block offset：%d, j:%d,i:%d,bitblock:%x\n",offset,j,i,bitblock[j]);
        write_block(2,bitblock);
        return BPB*i + j*8 + offset;
      }
    }
  }
  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  char block[BLOCK_SIZE];
  read_block(BBLOCK(id),block);
  int index = (id % BPB)/8;
  int offset = (id % BPB)%8;
  block[index] &= ~((char)(1<<offset));
  write_block(BBLOCK(id),block);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  time_t rawtime;
  struct inode* inode;
  uint32_t num = 1;
  //Find free inode number
  for(; num<INODE_NUM; num++){
    inode = get_inode(num);
    if(inode == NULL){
      break;
    }
    free(inode);
  }
  //If there is no residual inode.
  if(num == INODE_NUM){
    printf("\tim: error! There is no inode left!\n");
    exit(0);
  }
  inode = (struct inode*)malloc(sizeof(struct inode));
  inode->size = 0;
  inode->type = type;
  inode->ctime = time(&rawtime);
  inode->mtime = time(&rawtime);
  inode->atime = time(&rawtime);
  put_inode(num,inode);
  return num;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  struct inode* old_inode = get_inode(inum);
  if(old_inode == NULL){
    printf("\tThis block has been freed.\n");
    return;
  }
  struct inode* blank_inode = (struct inode*)malloc(sizeof(struct inode));
  blank_inode->size = 0;
  blank_inode->type = 0;
  put_inode(inum,blank_inode);
  free(old_inode);
  free(blank_inode);
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum <= 0 || inum > INODE_NUM) {                                    //modified
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  time_t rawtime;
  struct inode* inode = get_inode(inum);
  if(inode == NULL) return;
  uint node_size = inode->size;
  if(node_size == 0) return;
  *size = node_size;
  uint block_num = node_size%BLOCK_SIZE==0?(node_size/BLOCK_SIZE):(node_size/BLOCK_SIZE+1);
  //buf_out = (char**)malloc(block_num);
  uint max_index = MIN(NDIRECT,block_num); 

  char* block_data = (char*)malloc(block_num*BLOCK_SIZE);
  //Read direct block data
  for(int i=0; i<max_index; i++){
    blockid_t block_id = inode->blocks[i];
    bm->read_block(block_id,(block_data+i*BLOCK_SIZE));
  }

  //Read indirect block data
  if(max_index != block_num){
    char inblock[BLOCK_SIZE];
    bm->read_block(inode->blocks[NDIRECT],inblock);
    for(int i=0; i<(block_num - max_index); i++){
      blockid_t block_id = ((blockid_t*)inblock)[i];
      char* inblock_data = (char*)malloc(BLOCK_SIZE);
      bm->read_block(block_id,(block_data+(i+max_index)*BLOCK_SIZE));
    }
  }
  *buf_out = block_data;
  printf("\tread result:%s,size = %d;\n",(block_data+8*BLOCK_SIZE),node_size);
  //Update inode metadata
  inode->atime = time(&rawtime);
  put_inode(inum, inode);
  free(inode);
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  printf("\tinode_manager-write_file:%d\n",size);
  time_t rawtime;
  struct inode* inode = get_inode(inum);
  if(inode == NULL) return;
  uint old_num = inode->size%BLOCK_SIZE==0?(inode->size/BLOCK_SIZE):(inode->size/BLOCK_SIZE+1);
  uint new_num = size%BLOCK_SIZE==0?(size/BLOCK_SIZE):(size/BLOCK_SIZE+1);
  if(old_num > new_num){
    int i=0;
    if(old_num > NDIRECT && new_num > NDIRECT){
      for(; i<NDIRECT; i++){
        bm->write_block(inode->blocks[i],(buf+i*BLOCK_SIZE));
      }
      char inblock[BLOCK_SIZE];
      bm->read_block(inode->blocks[NINDIRECT],inblock);
      for(i=0; i<(new_num-NDIRECT); i++){
        blockid_t block_id = ((blockid_t*)inblock)[i];
        bm->write_block(block_id,(buf+(i+NDIRECT)*BLOCK_SIZE));
      }
      for(; i<(old_num-NDIRECT); i++){
        blockid_t block_id = ((blockid_t*)inblock)[i];
        bm->free_block(block_id);
      }
    }else if(old_num > NDIRECT && new_num <= NDIRECT){
      for(; i<new_num; i++){
        bm->write_block(inode->blocks[i],(buf+i*BLOCK_SIZE));
      }
      for(; i<NDIRECT; i++){
        bm->free_block(inode->blocks[i]);
      }
      char inblock[BLOCK_SIZE];
      bm->read_block(inode->blocks[NINDIRECT],inblock);
      for(i=0; i<(old_num-NDIRECT); i++){
        blockid_t block_id = ((blockid_t*)inblock)[i];
        bm->free_block(block_id);
      }
    }else{// node_size/BLOCK_SIZE <= NDIRECT
      for(; i<new_num; i++){
        bm->write_block(inode->blocks[i],(buf+i*BLOCK_SIZE));
      }
      for(; i<old_num; i++){
        bm->free_block(inode->blocks[i]);
      }
    }
  }else if(old_num < new_num){
    int i = 0;
    if(new_num > NDIRECT && old_num > NDIRECT){
      for(; i<NDIRECT; i++){
        bm->write_block(inode->blocks[i],(buf+i*BLOCK_SIZE));
      }
      char inblock[BLOCK_SIZE];
      bm->read_block(inode->blocks[NDIRECT],inblock);
      for(; i<old_num; i++){
        bm->write_block(((uint*)inblock)[i-NDIRECT],(buf+i*BLOCK_SIZE));
      }
      for(; i<new_num; i++){
        ((uint*)inblock)[i-NDIRECT] = bm->alloc_block();
        bm->write_block(((uint*)inblock)[i-NDIRECT],(buf+i*BLOCK_SIZE));
      }
      bm->write_block(inode->blocks[NDIRECT],inblock);
    }else if(new_num > NDIRECT && old_num <= NDIRECT){
      for(; i<old_num; i++){
        bm->write_block(inode->blocks[i],(buf+i*BLOCK_SIZE));
      }
      for(; i<NDIRECT; i++){
        inode->blocks[i] = bm->alloc_block();
        bm->write_block(inode->blocks[i],(buf+i*BLOCK_SIZE));
      }
      inode->blocks[NDIRECT] = bm->alloc_block();
      char inblock[BLOCK_SIZE];
      bm->read_block(inode->blocks[NDIRECT],inblock);
      for(; i<new_num; i++){
        ((uint*)inblock)[i-NDIRECT] = bm->alloc_block();
        bm->write_block(((uint*)inblock)[i-NDIRECT],(buf+i*BLOCK_SIZE));
      }
      bm->write_block(inode->blocks[NDIRECT],inblock);
    }else{// size/BLOCK_SIZE < NDIRECT
      for(; i<old_num; i++){
        bm->write_block(inode->blocks[i],(buf+i*BLOCK_SIZE));
      }
      for(; i<new_num; i++){
        inode->blocks[i] = bm->alloc_block();
        bm->write_block(inode->blocks[i],(buf+i*BLOCK_SIZE));
        char new_data[BLOCK_SIZE];
        bm->read_block(inode->blocks[i],new_data);
        printf("\tnew_data:%s\n",new_data);
      }
    }
  }else{ //node_size/BLOCK_SIZE == size/BLOCK_SIZE
    int i = 0;
    if(old_num > NDIRECT){
      for(; i<NDIRECT; i++){
        bm->write_block(inode->blocks[i],(buf+i*BLOCK_SIZE));
      }
      char inblock[BLOCK_SIZE];
      bm->read_block(inode->blocks[NDIRECT],inblock);
      for(; i<old_num; i++){
        bm->write_block(((uint*)inblock)[i-NDIRECT],(buf+i*BLOCK_SIZE));
      }
    }else{
      for(; i<old_num; i++){
        bm->write_block(inode->blocks[i],(buf+i*BLOCK_SIZE));
      }
    }
  }
  //Update inode metadata
  inode->size = size;
  inode->mtime = time(&rawtime);
  inode->atime = time(&rawtime);
  inode->ctime = time(&rawtime);
  put_inode(inum,inode);
  free(inode);
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  struct inode* inode = get_inode(inum);
  if(inode!=NULL){
    a.type = inode->type;
    a.size = inode->size;
    a.ctime = inode->ctime;
    a.mtime = inode->mtime;
    a.atime = inode->atime;
    free(inode);
  }
  return ;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  struct inode* old_inode = get_inode(inum);
  uint block_num = old_inode->size%BLOCK_SIZE==0?(old_inode->size/BLOCK_SIZE):(old_inode->size/BLOCK_SIZE+1);
  for(int i=0; i<MIN(NDIRECT,block_num); i++){
    bm->free_block(old_inode->blocks[i]);
  }
  if(NDIRECT < block_num){
    char inblock[BLOCK_SIZE];
    bm->read_block(old_inode->blocks[NDIRECT],inblock);
    for(int i=0; i<(block_num-NDIRECT); i++){
      blockid_t block_id = ((blockid_t*)inblock)[i];
      bm->free_block(block_id);
    }
    bm->free_block(old_inode->blocks[NDIRECT]);
  }
  free(old_inode);
  free_inode(inum);
  return;
}
