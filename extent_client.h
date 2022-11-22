// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "extent_server.h"

class extent_client {
 private:
  rpcc *cl;

 public:
  extent_client(std::string dst);

  extent_protocol::status tx_begin(chfs_command::txid_t &tid);
  extent_protocol::status tx_commit(chfs_command::txid_t tid);
  extent_protocol::status create(chfs_command::txid_t tid, uint32_t type, extent_protocol::extentid_t &eid);
  extent_protocol::status get(extent_protocol::extentid_t eid, std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, extent_protocol::attr &a);
  extent_protocol::status put(chfs_command::txid_t tid, extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(chfs_command::txid_t tid, extent_protocol::extentid_t eid);
};

#endif 

