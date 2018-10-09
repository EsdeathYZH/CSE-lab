// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "extent_protocol.h"

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst); //TODO : shall we lock root directory here?
  lc = new lock_client(lock_dst);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
}


yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::_isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("\tisfile: %lld is a file\n", inum);
        return true;
    } 
    printf("\tisfile: %lld isn't a file\n", inum);
    return false;
}

bool
yfs_client::isfile(inum inum)
{
    lc->acquire(inum);
    bool result = _isfile(inum);
    lc->release(inum);
    return result;
}

/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool 
yfs_client::issymlink(inum inum){
    lc->acquire(inum);
    bool result = _issymlink(inum);
    lc->release(inum);
    return result;
}

bool 
yfs_client::_issymlink(inum inum){
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("\tissymlink: %lld is a symbol link\n", inum);
        return true;
    } 
    printf("\tissymlink: %lld isn't a symbol link\n", inum);
    return false;
}

bool
yfs_client::_isdir(inum inum)
{
    return !(_isfile(inum)||_issymlink(inum));
}


bool
yfs_client::isdir(inum inum)
{
    lc->acquire(inum);
    bool result = _isdir(inum);
    lc->release(inum);
    return result;
}

int 
yfs_client::readlink(inum inum, std::string& buf)
{
    lc->acquire(inum);
    int r = OK;
    r = ec->get(inum,buf);
    lc->release(inum);
    return r;
}

int 
yfs_client::symlink(inum parent, const char *link, const char *name, inum& ino_out){
    int r = OK;
    bool if_exist = false;
    std::string filename(name);
    if(filename.find('/') != std::string::npos || filename.find('\0') != std::string::npos){
        printf("Wrong link name:%s!",name);
        r = NOENT; // TODO:I don't know the meaning of NOENT....
        return r;
    }
    //acquire
    lc->acquire(parent);
    r = _lookup(parent,name,if_exist,ino_out);
    if(if_exist){
        lc->release(parent);
        r = EXIST;
        return r;
    }
    r = ec->create(extent_protocol::T_SYMLINK,ino_out);
    //FIX ME:check if the content of link is legal
    r = ec->put(ino_out,std::string(link));
    std::string origin_data;
    ec->get(parent,origin_data);
    std::ostringstream ost;
    //FIX ME:check if the content of name is legal
    ost << origin_data << std::string(name) << ":" << ino_out << ";";
    r = ec->put(parent,ost.str());
    //release
    lc->release(parent);
    return r;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;
    //lock
    lc->acquire(inum);
    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    //release 
    lc->release(inum);
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    //lock
    lc->acquire(inum);
    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    //release
    lc->release(inum);
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string data;
    //lock
    lc->acquire(ino);
    printf("\tyfs_client-setattr:%d\n",size);
    r = ec->get(ino,data);
    if(r != OK){
        lc->release(ino);
        return r;
    }
    if(data.size() >= size){
        data = data.substr(0,size);
    }else{
        data += std::string(size-data.size(),'\0');
    }
    ec->put(ino,data);
    //release
    lc->release(ino);
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    std::string filename(name);
    if(filename.find('/') != std::string::npos || filename.find('\0') != std::string::npos){
        printf("Wrong file name:%s!",name);
        r = NOENT; // TODO:I don't know the meaning of NOENT....
        return r;
    }
    //lock
    lc->acquire(parent);
    bool if_exist = false;
    r = _lookup(parent,name,if_exist,ino_out);
    if(if_exist){
        //release
        lc->release(parent);
        r = EXIST;
        return r;
    }
    if(r != OK){
        //release
        lc->release(parent);
        return r;
    }
    ec->create(extent_protocol::T_FILE,ino_out);
    std::string origin_data;
    ec->get(parent,origin_data);
    std::ostringstream ost;
    ost << origin_data << std::string(name) << ":" << ino_out << ";";
    ec->put(parent,ost.str());
    //release
    lc->release(parent);
    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    std::string filename(name);
    if(filename.find('/') != std::string::npos || filename.find('\0') != std::string::npos){
        printf("\tMkdir:Unlegal directory name!\n");
        r = NOENT; // TODO:I don't know the meaning of NOENT....
        return r;
    }
    //lock
    lc->acquire(parent);
    bool if_exist = false;
    r = _lookup(parent,name,if_exist,ino_out);
    if(if_exist){
        //release
        lc->release(parent);
        r = EXIST;
        return r;
    }
    if(r != OK){
        //release
        lc->release(parent);
        return r;
    }
    ec->create(extent_protocol::T_DIR,ino_out);
    std::string origin_data;
    ec->get(parent,origin_data);
    std::ostringstream ost;
    ost << origin_data << std::string(name) << ":" << ino_out << ";";
    ec->put(parent,ost.str());
    //release
    lc->release(parent);
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    lc->acquire(parent);
    int r = _lookup(parent,name,found,ino_out);
    lc->release(parent);
    return r;
}

int 
yfs_client::_lookup(inum parent, const char *name, bool &found, inum &ino_out){
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.   //directory content: ([filename]:[inode-number];)*
     */
    std::list<dirent> dirent_list;
    r = readdir(parent,dirent_list);
    if(r != OK){
        found = false;
        return r;
    }
    std::list<dirent>::iterator iter = dirent_list.begin();
    while(iter != dirent_list.end()){
        dirent temp = *iter;
        if(!strcmp(temp.name.c_str(),name)){
            found = true;
            ino_out = temp.inum;
            break;
        }
        iter++;
    }
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    if(!_isdir(dir)){
        return ENOENT;
    }
    printf("\treaddir %d\n",dir);
    std::string dir_content;
    r = ec->get(dir,dir_content);
    if(r != extent_protocol::OK){
        printf("error get, return not OK\n");
        return r;
    }
    //Parse the directory content
    printf("\tdir_content:%s\n",dir_content.c_str());
    int semicolon_pos = dir_content.find(';');
    int last_pos = 0;
    while(std::string::npos != semicolon_pos){
        std::string temp = dir_content.substr(last_pos,semicolon_pos-last_pos);
        int colon_pos = temp.find(':');
        dirent temp_dirent;
        temp_dirent.inum = n2i(temp.substr(colon_pos+1));
        temp_dirent.name = temp.substr(0,colon_pos);
        list.push_back(temp_dirent);
        //Update last pos and semicolon pos 
        last_pos = semicolon_pos + 1;
        semicolon_pos = dir_content.find(';',last_pos);
    }
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    //lock
    lc->acquire(ino);
    printf("\tyfs_client::read(%d,%d,%d)",ino,size,off);
    std::string file_content;
    r = ec->get(ino,file_content);
    if(r != OK || file_content.size() < off){
        lc->release(ino);
        return r;
    }else{
        data = file_content.substr(off,size);
    }
    //release
    lc->release(ino);
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    //lock
    lc->acquire(ino);
    printf("\tyfs-write:%d\n",ino);
    std::string origin_data;
    r = ec->get(ino,origin_data);
    if(r != OK){
        lc->release(ino);
        bytes_written = 0;
        return r;
    }
    std::string new_data(data,size);
    std::string front_data = origin_data.substr(0,off);
    std::string hole(off-front_data.size(),'\0');
    std::string back_data;
    std::string final_data;
    if(origin_data.size() > (new_data.size()+off)){
        back_data = origin_data.substr(new_data.size()+off);
        final_data = (front_data + new_data + back_data);
        bytes_written = new_data.size();//What's the meaning of byte_written?
    }else{
        final_data = (front_data + hole + new_data);
        bytes_written = new_data.size() + hole.size();//What's the meaning of byte_written?
    }
    r = ec->put(ino,final_data);
    //ec->get(ino,origin_data);
    if(r != OK){
        lc->release(ino);
        bytes_written = 0;
        return r;
    }
    //release 
    lc->release(ino);
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    std::list<dirent> dir_content;
    //lock
    lc->acquire(parent);
    r = readdir(parent,dir_content);
    if(r != OK){
        //release
        lc->release(parent);
        return r;
    }
    std::list<dirent>::iterator iter = dir_content.begin();
    unsigned long long inode_id;
    bool found = false;
    while(iter != dir_content.end()){
        //Find the item and delete it
        if(!strcmp(iter->name.c_str(),name)){
            found = true;
            inode_id = iter->inum;
            dir_content.erase(iter);
            break;
        }
        iter++;
    }
    if(!found){
        printf("File %s doesn't exist!",name);
        //release
        lc->release(parent);
        r = IOERR;
        return r;
    }
    lc->acquire(inode_id);
    ec->remove(inode_id);
    std::ostringstream ost;
    for(iter = dir_content.begin(); iter!= dir_content.end(); iter++){
        ost << iter->name << ":" << iter->inum << ";";
    }
    r = ec->put(parent,ost.str());
    //release
    lc->release(inode_id);
    lc->release(parent);
    return r;
}



