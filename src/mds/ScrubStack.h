// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef SCRUBSTACK_H_
#define SCRUBSTACK_H_

#include <atomic>

#include "common/Mutex.h"
#include "CDir.h"
#include "CDentry.h"
#include "CInode.h"
#include "MDSContext.h"
#include "ScrubHeader.h"

#include "include/elist.h"

/* XXX */
#include "common/LogClient.h"
#include "mds/MDSRank.h"
#include "mds/MDCache.h"

class MDCache;
class Finisher;

class ScrubStack {
  friend class C_MDS_ScrubDirfrag;
protected:
  /// A finisher needed so that we don't re-enter kick_off_scrubs
  Finisher *finisher;

  Mutex stack_lock;
  /// The stack of dentries we want to scrub
  elist<CInode*> inode_stack;
  /// current number of dentries we're actually scrubbing
  std::atomic_int scrubs_in_progress;
  ScrubStack *scrubstack; // hack for dout
  int stack_size;

  class C_KickOffScrubs : public MDSInternalContext {
    ScrubStack *stack;
    bool finished;
  public:
    C_KickOffScrubs(MDCache *mdcache, ScrubStack *s) :
      MDSInternalContext(mdcache->mds), stack(s), finished(false) {
      stack->scrubs_in_progress++;
    }
    ~C_KickOffScrubs() { if (!finished) stack->scrubs_in_progress--; }
    void finish (int r) {
      finished = true;
      stack->scrubs_in_progress--;
      stack->kick_off_scrubs();
    }
  };

public:
  MDCache *mdcache;
  ScrubStack(MDCache *mdc, Finisher *finisher_) :
    finisher(finisher_),
    stack_lock("ScrubStack::stack_lock"),
    inode_stack(member_offset(CInode, item_scrub)),
    scrubs_in_progress(0),
    scrubstack(this),
    stack_size(0),
    mdcache(mdc) {}
  ~ScrubStack() {
    assert(inode_stack.empty());
    assert(!scrubs_in_progress);
  }
  /**
   * Put a inode on the top of the scrub stack, so it is the highest priority.
   * If there are other scrubs in progress, they will not continue scrubbing new
   * entries until this one is completed.
   * @param in The inodey to scrub
   * @param header The ScrubHeader propagated from whereever this scrub
   *               was initiated
   */
  void enqueue_inode_top(CInode *in, const ScrubHeader& header,
			 MDSInternalContextBase *on_finish) {
    enqueue_inode(in, header, on_finish, true);
  }
  /** Like enqueue_inode_top, but we wait for all pending scrubs before
   * starting this one.
   */
  void enqueue_inode_bottom(CInode *in, const ScrubHeader& header,
			    MDSInternalContextBase *on_finish) {
    enqueue_inode(in, header, on_finish, false);
  }

private:
  /**
   * Put the inode at either the top or bottom of the stack, with
   * the given scrub params, and then try and kick off more scrubbing.
   */
  void enqueue_inode(CInode *in, const ScrubHeader& header,
                      MDSInternalContextBase *on_finish, bool top);
  void _enqueue_inode(CInode *in, CDentry *parent, const ScrubHeader& header,
                      MDSInternalContextBase *on_finish, bool top);
  /**
   * Kick off as many scrubs as are appropriate, based on the current
   * state of the stack.
   */
  /* XXX */ public:
  void kick_off_scrubs();
  /* XXX */ private:
  /**
   * Push a indoe on top of the stack.
   */
  inline void push_inode(CInode *in);
  /**
   * Push a inode to the bottom of the stack.
   */
  void push_inode_bottom(CInode *in);
  /**
   * Pop the given inode off the stack.
   */
  /* XXX */ public:
  void pop_inode_if(CInode *in);
  /*inline*/ void pop_inode(CInode *in);
  /* XXX */ private:
  CInode *front_inode();

  void dump_stack();

  /**
   * Scrub a file inode.
   * @param in The indoe to scrub
   */
  void scrub_file_inode(CInode *in);

  /**
   * Callback from completion of CInode::validate_disk_state
   * @param in The inode we were validating
   * @param r The return status from validate_disk_state
   * @param result Populated results from validate_disk_state
   */
  void _validate_inode_done(CInode *in, int r,
			    const CInode::validated_data &result);
  friend class C_InodeValidated;

  /**
   * Make progress on scrubbing a directory-representing dirfrag and
   * its children..
   *
   * 1) Select the next dirfrag which hasn't been scrubbed, and make progress
   * on it if possible.
   *
   * 2) If not, move on to the next dirfrag and start it up, if any.
   *
   * 3) If waiting for results from dirfrag scrubs, do nothing.
   *
   * 4) If all dirfrags have been scrubbed, scrub my inode.
   *
   * @param in The CInode to scrub as a directory
   * @param added_dentries set to true if we pushed some of our children
   * onto the ScrubStack
   * @param is_terminal set to true if there are no descendant dentries
   * remaining to start scrubbing.
   * @param done set to true if we and all our children have finished scrubbing
   */
  void scrub_dir_inode(CInode *in, bool *added_children, bool *is_terminal,
                       bool *done);
  /**
   * Make progress on scrubbing a dirfrag. It may return after each of the
   * following steps, but will report making progress on each one.
   *
   * 1) enqueues the next unscrubbed child directory dentry at the
   * top of the stack.
   *
   * 2) Initiates a scrub on the next unscrubbed file dentry
   *
   * If there are scrubs currently in progress on child dentries, no more child
   * dentries to scrub, and this function is invoked, it will report no
   * progress. Try again later.
   *
   */
  /* XXX */ public:
  void scrub_dirfrag(CDir *dir, ScrubHeader& header,
		     bool *added_children, bool *is_terminal, bool *done,
		     MDSInternalContext *onfinish);
  void scrub_complete(dirfrag_t df, nest_info_t rstat);
  /* XXX */ private:
  /**
   * Scrub a directory-representing dentry.
   *
   * @param in The directory inode we're doing final scrub on.
   */
  void scrub_dir_inode_final(CInode *in);

  /**
   * Get a CDir into memory, and return it if it's already complete.
   * Otherwise, fetch it and kick off scrubbing when done.
   *
   * @param in The Inode to get the next directory from
   * @param new_dir The CDir we're returning to you. NULL if
   * not ready yet or there aren't any.
   * @returns false if you have to wait, true if there's no work
   * left to do (we returned it, or there are none left in this inode).
   */
  bool get_next_cdir(CInode *in, CDir **new_dir);

};

class C_MDS_ScrubDirfrag : public MDSInternalContext {
protected:
  ScrubStack *ss;
  dirfrag_t fg;
  ScrubHeader header;
  bool finished;

public:
  C_MDS_ScrubDirfrag (ScrubStack* _ss, dirfrag_t _fg,
		      ScrubHeader _header)
    : MDSInternalContext(_ss->mdcache->mds), ss(_ss), fg(_fg),
      header(_header), finished(false) {
      LogChannelRef log = ss->mdcache->mds->clog;
      log->info() << "start scrub op " << this;
      ++ss->scrubs_in_progress;
  }

  ~C_MDS_ScrubDirfrag() { if (!finished) ss->scrubs_in_progress--; }

  void finish (int r) {
    bool added_children, is_terminal, done;
    CDir *dir = mds->mdcache->get_dirfrag(fg);
    LogChannelRef clog = ss->mdcache->mds->clog;
    ss->scrubs_in_progress--;
    finished = true;
    clog->info() << "end scrub op " << this;
    if (!dir) {
      CInode *ino = ss->mdcache->get_inode(fg.ino);
      if (!ino) {
	C_MDS_ScrubDirfrag *sdf = new C_MDS_ScrubDirfrag(ss, fg, header);
	ss->mdcache->open_ino(fg.ino, 0, sdf);
	return;
      }
      assert(ino->is_dir());
      if (!ino->is_auth()) {
	MMDSScrubPath *msg = new MMDSScrubPath(fg, header);
	mds->send_message_mds(msg, ino->authority().first);
	if (ino->item_scrub.is_on_list()) {
	  ss->pop_inode(ino);
	}
	ss->kick_off_scrubs();
	return;
      } else {
	assert(0);
      }
    }

    dir->scrub_info()->header = header;
    C_MDS_ScrubDirfrag *sdf = new C_MDS_ScrubDirfrag(ss, fg, header);
    ss->scrub_dirfrag(dir, header, &added_children, &is_terminal, &done, sdf);

    if (!dir->is_auth()) {
      clog->info() << "context: lost " << *dir;
      dir->scrub_abort();
      return;
    }

    if (is_terminal && done) {
      dir->get_inode()->scrub_dirfrag_finished(fg.frag);
      ss->scrub_dir_inode_final(dir->get_inode());
      return;
    }

    if (!dir->get_inode()->scrub_is_in_progress()) {
      CDentry *parent_dn = dir->inode->get_parent_dn();
      dir->get_inode()->scrub_initialize(parent_dn, header, NULL);
    }

    ss->push_inode_bottom(dir->get_inode());
    ss->kick_off_scrubs();
  }
};

#endif /* SCRUBSTACK_H_ */
