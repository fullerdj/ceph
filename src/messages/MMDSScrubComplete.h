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


#ifndef CEPH_MMDSSCRUBCOMPLETE_H
#define CEPH_MMDSSCRUBCOMPLETE_H

#include "msg/Message.h"

#include "mds/mdstypes.h"

class MMDSScrubComplete : public Message {
 public:  
  dirfrag_t dirfrag;
  nest_info_t rstat;

  MMDSScrubComplete() : Message(MSG_MDS_SCRUBCOMPLETE) {}
  MMDSScrubComplete(dirfrag_t _dirfrag, nest_info_t _rstat)
    : Message(MSG_MDS_SCRUBCOMPLETE), dirfrag(_dirfrag), rstat(_rstat) {}
private:
  ~MMDSScrubComplete() {}

public:
  const char *get_type_name() const { return "SC"; }
  void print(ostream& o) const {
    o << "scrub_complete " << dirfrag << " rstat " << rstat;
  }

  void encode_payload(uint64_t features) {
    ::encode(dirfrag, payload);
    ::encode(rstat, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(dirfrag, p);
    ::decode(rstat, p);
  }

};

#endif
