// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"

#include "inode_manager.h"
#include "persister.h"

class extent_server {
 protected:
#if 0
  typedef struct extent {
    std::string data;
    struct extent_protocol::attr attr;
  } extent_t;
  std::map <extent_protocol::extentid_t, extent_t> extents;
#endif
  inode_manager *im;
  chfs_persister *_persister;

 public:
  extent_server();

  int tx_begin(int, chfs_command::txid_t &tid);
  int tx_commit(int, chfs_command::txid_t &tid);
  int create(uint32_t type, chfs_command::txid_t tid, extent_protocol::extentid_t &id);
  int put(extent_protocol::extentid_t id, std::string, chfs_command::txid_t &tid);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, chfs_command::txid_t &tid);

  // Your code here for lab2A: add logging APIs
};

#endif 







