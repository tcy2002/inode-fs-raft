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
    extent_protocol::status ret = cl->call(extent_protocol::begin, cl->id(), r, tid);
    return ret;
}

extent_protocol::status
extent_client::tx_commit(chfs_command::txid_t tid) {
    int r;
    extent_protocol::status ret = cl->call(extent_protocol::commit, cl->id(), r, tid);
    return ret;
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id, chfs_command::txid_t tid)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  ret = cl->call(extent_protocol::create, cl->id(), type, tid, id);
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  ret = cl->call(extent_protocol::get, cl->id(), eid, buf);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  ret = cl->call(extent_protocol::create, cl->id(), eid, attr);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf, chfs_command::txid_t tid)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  ret = cl->call(extent_protocol::create, cl->id(), eid, buf, tid);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid, chfs_command::txid_t tid)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  ret = cl->call(extent_protocol::create, cl->id(), eid, tid);
  return ret;
}


