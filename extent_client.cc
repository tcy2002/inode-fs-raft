// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::tx_begin(chfs_command::txid_t &tid) {
    int r;
    extent_protocol::status ret = cl->call(extent_protocol::begin, r, tid);
    return ret;
}

extent_protocol::status
extent_client::tx_commit(chfs_command::txid_t tid) {
    int r;
    extent_protocol::status ret = cl->call(extent_protocol::commit, tid, r);
    return ret;
}

extent_protocol::status
extent_client::create(chfs_command::txid_t tid, uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  ret = cl->call(extent_protocol::create, tid, type, id);
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  ret = cl->call(extent_protocol::get, eid, buf);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  ret = cl->call(extent_protocol::getattr, eid, attr);
  return ret;
}

extent_protocol::status
extent_client::put(chfs_command::txid_t tid, extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  int r;
  ret = cl->call(extent_protocol::put, tid, eid, buf, r);
  return ret;
}

extent_protocol::status
extent_client::remove(chfs_command::txid_t tid, extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  int r;
  ret = cl->call(extent_protocol::remove, tid, eid, r);
  return ret;
}


