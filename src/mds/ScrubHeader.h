// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef SCRUB_HEADER_H_
#define SCRUB_HEADER_H_

class CInode;

/**
 * Externally input parameters for a scrub, associated with the root
 * of where we are doing a recursive scrub
 *
 * TODO: swallow up 'recurse' and 'children' settings here instead of
 * passing them down into every scrub_info structure
 */
class ScrubHeader {
public:
  CInode *origin;
  inodeno_t oi;
  std::string tag;

  utime_t start;

  bool force;
  bool recursive;
  bool repair;
  Formatter *formatter;

  bool operator==(ScrubHeader& sh) {
	  return sh.start == start && sh.oi == oi;
  }

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

