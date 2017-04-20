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


#ifndef CEPH_MMDSSCRUBPATH_H
#define CEPH_MMDSSCRUBPATH_H

#include "msg/Message.h"


class MMDSScrubPath : public Message {
 public:
  dirfrag_t dirfrag;
  inodeno_t root;
  std::string tag;
  bool force;
  bool repair;

  MMDSScrubPath() : Message(MSG_MDS_SCRUBPATH) {}
  MMDSScrubPath(dirfrag_t _dirfrag, ScrubHeader& header)
    : Message(MSG_MDS_SCRUBPATH), dirfrag(_dirfrag), root(header.get_oi()),
      tag(header.get_tag()), force(header.get_force()),
      repair(header.get_repair()) {}
private:
  ~MMDSScrubPath() {}

public:
  const char *get_type_name() const { return "Sc"; }
  void print(ostream& o) const {
    o << "scrub(" << dirfrag << " tag " << tag << " force=" << force
      << " repair=" << repair << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(dirfrag, payload);
    ::encode(root, payload);
    ::encode(tag, payload);
    ::encode(force, payload);
    ::encode(repair, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(dirfrag, p);
    ::decode(root, p);
    ::decode(tag, p);
    ::decode(force, p);
    ::decode(repair, p);
  }

};

#endif
