// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef CEPH_MMDSSCRUBINODE_H
#define CEPH_MMDSSCRUBINODE_H

#include "msg/Message.h"


class MMDSScrubInode : public Message {
 public:
  inodeno_t inodeno;
  inodeno_t root;
  std::string tag;
  bool force;
  bool repair;

  MMDSScrubInode() : Message(MSG_MDS_SCRUBINODE) {}
  MMDSScrubInode(inodeno_t _inodeno, ScrubHeader& header)
    : Message(MSG_MDS_SCRUBINODE), inodeno(_inodeno), root(header.get_oi()),
      tag(header.get_tag()), force(header.get_force()),
      repair(header.get_repair()) {}
private:
  ~MMDSScrubInode() {}

public:
  const char *get_type_name() const { return "Si"; }
  void print(ostream& o) const {
    o << "scrub_inode(" << inodeno << " tag " << tag << " force=" << force
      << " repair=" << repair << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(inodeno, payload);
    ::encode(root, payload);
    ::encode(tag, payload);
    ::encode(force, payload);
    ::encode(repair, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(inodeno, p);
    ::decode(root, p);
    ::decode(tag, p);
    ::decode(force, p);
    ::decode(repair, p);
  }

};

#endif
