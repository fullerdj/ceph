// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef SCRUB_HEADER_H_
#define SCRUB_HEADER_H_

class CInode;

/**
 * Externally input parameters for a scrub, associated with the root
 * of where we are doing a recursive scrub
 */
class ScrubHeader {
protected:
  std::string tag;
  bool force;
  bool recursive;
  bool repair;
  Formatter * formatter;
  CInode *origin;
  inodeno_t oi;

public:
  ScrubHeader() {}
  ScrubHeader(const std::string &tag_, bool force_, bool recursive_,
              bool repair_, Formatter *f_)
      : tag(tag_), force(force_), recursive(recursive_), repair(repair_),
        formatter(f_), origin(nullptr) {}

  // Set after construction because it won't be known until we've
  // started resolving path and locking
  void set_origin(CInode *origin_) { origin = origin_; }
  void set_oi(inodeno_t oi_) { oi = oi_; }

  bool get_recursive() const { return recursive; }
  bool get_repair() const { return repair; }
  bool get_force() const { return force; }
  inodeno_t get_oi() const { return oi; }
  const CInode *get_origin() const { return origin; }
  const std::string &get_tag() const { return tag; }
  Formatter *get_formatter() const { return formatter; }

  void encode(bufferlist& bl) const {
	  ::encode(oi, bl);
	  ::encode(tag, bl);
	  ::encode(recursive, bl);
	  ::encode(repair, bl);
  }

  void decode(bufferlist::iterator& bl) {
	  ::decode(oi, bl);
	  ::decode(tag, bl);
	  ::decode(recursive, bl);
	  ::decode(repair, bl);
  }

  operator bool() const { return oi != inodeno_t(); }
};
WRITE_CLASS_ENCODER(ScrubHeader)

typedef ceph::shared_ptr<ScrubHeader> ScrubHeaderRef;
typedef ceph::shared_ptr<const ScrubHeader> ScrubHeaderRefConst;

#endif // SCRUB_HEADER_H_
