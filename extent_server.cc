// the extent server implementation

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <vector>

#include "extent_server.h"
#include "persister.h"

extent_server::extent_server() 
{
  im = new inode_manager();
  _persister = new chfs_persister("log", im); // DO NOT change the dir name here
  _persister->restore_logdata();
}

int extent_server::tx_begin(chfs_command::txid_t &tid) {
    tid = _persister->append_begin();
    return extent_protocol::OK;
}

int extent_server::tx_commit(chfs_command::txid_t tid) {
    _persister->append_commit(tid);
    return extent_protocol::OK;
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id, chfs_command::txid_t tid)
{
  // alloc a new inode and return inum
  printf("extent_server: create inode\n");

  char t = type + 48;
  std::string t_str(1, t);

  id = im->alloc_inode(type);
  _persister->append_log(tid, chfs_command::CMD_CREATE, id, "", t_str);

  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, chfs_command::txid_t tid)
{
  printf("extent_server: put %lld\n", id);
  id &= 0x7fffffff;

  printf("\nput_buf: %.*s\n\n", (int)buf.size(), buf.c_str());
  std::string str;
  get(id, str);
  printf("put_str: %.*s\n\n", (int)str.size(), str.c_str());

  im->write_file(id, buf.c_str(), buf.size());
  _persister->append_log(tid, chfs_command::CMD_PUT, id, str, buf);

  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("extent_server: getattr %lld\n", id);

  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->get_attr(id, attr);
  a = attr;

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, chfs_command::txid_t tid)
{
  printf("extent_server: remove %lld\n", id);
  id &= 0x7fffffff;

  std::string str;
  get(id, str);
  extent_protocol::attr a;
  getattr(id, a);
  char t = a.type + 48;
  std::string t_str(1, t);

  im->remove_file(id);
  _persister->append_log(tid, chfs_command::CMD_REMOVE, id, str, t_str);

  return extent_protocol::OK;
}

