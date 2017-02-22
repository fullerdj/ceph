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

#include <iostream>

#include "ScrubStack.h"
#include "common/Finisher.h"
#include "mds/MDSRank.h"
#include "mds/MDCache.h"
#include "mds/MDSContinuation.h"

#include "messages/MMDSScrubPath.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, scrubstack->mdcache->mds)
static ostream& _prefix(std::ostream *_dout, MDSRank *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".scrubstack ";
}

std::ostream& operator<<(std::ostream& out, const ScrubHeader& sh) {
  out << "{ " << sh.oi << " " << sh.tag << " }";
  return out;
}

void ScrubStack::push_inode(CInode *in)
{
  dout(20) << "pushing " << *in << " on top of ScrubStack" << dendl;
  if (!in->item_scrub.is_on_list()) {
    in->get(CInode::PIN_SCRUBQUEUE);
    stack_size++;
  }
  inode_stack.push_front(&in->item_scrub);
}

void ScrubStack::push_inode_bottom(CInode *in)
{
  dout(20) << "pushing " << *in << " on bottom of ScrubStack" << dendl;
  if (!in->item_scrub.is_on_list()) {
    in->get(CInode::PIN_SCRUBQUEUE);
    stack_size++;
  }
  inode_stack.push_back(&in->item_scrub);
}

void ScrubStack::pop_inode(CInode *in)
{
  dout(20) << "popping " << *in
          << " off of ScrubStack" << dendl;
  assert(in->item_scrub.is_on_list());
  in->put(CInode::PIN_SCRUBQUEUE);
  in->item_scrub.remove_myself();
  stack_size--;
}

void ScrubStack::_enqueue_inode(CInode *in, CDentry *parent,
				const ScrubHeader& header,
				MDSInternalContextBase *on_finish, bool top)
{
  dout(10) << __func__ << " with {" << *in << "}"
           << ", on_finish=" << on_finish << ", top=" << top << dendl;
  assert(mdcache->mds->mds_lock.is_locked_by_me());
  assert(mdcache->have_inode(in->inode.ino));
  if (!in->scrub_is_in_progress()) {
    in->scrub_initialize(parent, header, on_finish);
  } else if (in->scrub_info()->scrub_parent == NULL && parent) {
    in->scrub_info()->scrub_parent = parent;
    parent->get(CDentry::PIN_SCRUBPARENT);
  }
  if (top)
    push_inode(in);
  else
    push_inode_bottom(in);
}

void ScrubStack::enqueue_inode(CInode *in, const ScrubHeader& header,
                               MDSInternalContextBase *on_finish, bool top)
{
  _enqueue_inode(in, NULL, header, on_finish, top);
  kick_off_scrubs();
}

void ScrubStack::kick_off_scrubs()
{
  dout(20) << __func__ << " entering with " << scrubs_in_progress << " in "
              "progress and " << stack_size << " in the stack" << dendl;
  bool can_continue = true;
  elist<CInode*>::iterator i = inode_stack.begin();

  /* XXX */
  for (elist<CInode*>::iterator j = inode_stack.begin(); !j.end(); ++j) {
    dout(20) << **j << dendl;
  }
  dout(20) << "--end stack--" << dendl;

  while (g_conf->mds_max_scrub_ops_in_progress > scrubs_in_progress &&
      can_continue && !i.end()) {
    CInode *curi = *i;
    ++i; // we have our reference, push iterator forward

    dout(20) << __func__ << " examining " << *curi << dendl;
    if (!curi->item_scrub.is_on_list()) {
      dout(20) << *curi << " got popped, skipping" << dendl;
      continue;
    }
#if 0
    if (!curi->is_auth()) {
      if (curi->is_dir()) { 
	dout(20) << __func__ << " ssss-popped rep dir " << *curi << dendl;
	list<CDir*> dirfrags;
	curi->get_dirfrags(dirfrags);
	if (dirfrags.empty()) { /* XXX WTF? */
	  dout(20) << __func__ << " no dirfrags?" << dendl;
	  MMDSScrubPath *msg = new MMDSScrubPath(dirfrag_t(curi->ino(),
							   frag_t()),
						 curi->scrub_info()->header);
	  dout(20) << __func__ << " sending to " << curi->authority().first
		   << dendl;
	  mdcache->mds->send_message_mds(msg, curi->authority().first);
	} else {
	  for (auto j : dirfrags) {
	    MMDSScrubPath *msg = new MMDSScrubPath(j->dirfrag(),
						   curi->scrub_info()->header);
	    dout(20) << __func__ << " sending to " << j->authority().first
		     << dendl;
	    mdcache->mds->send_message_mds(msg, j->authority().first);
	  }
	}
	if (curi->scrub_is_in_progress()) {
	  MDSInternalContextBase *c = NULL;
	  curi->scrub_finished(&c);
	  if (c) {
	    finisher->queue(new MDSIOContextWrapper(mdcache->mds, c), 0);
	  }
	}
	pop_inode(curi);
	can_continue = true;
	continue;
      } else {
	dout(20) << __func__ << " popped rep inode at " << *curi << dendl;
      }
    }
#endif

    if (!curi->is_dir()) {
      // it's a regular file, symlink, or hard link
      pop_inode(curi); // we only touch it this once, so remove from stack

      if (!curi->scrub_info()->on_finish) {
	scrubs_in_progress++;
	curi->scrub_set_finisher(&scrub_kick);
      }
      scrub_file_inode(curi);
      can_continue = true;
    } else {
      bool completed; // it's done, so pop it off the stack
      bool terminal; // not done, but we can start ops on other directories
      bool progress; // it added new dentries to the top of the stack
      scrub_dir_inode(curi, &progress, &terminal, &completed);
      if (completed) {
        dout(20) << __func__ << " dir completed" << dendl;
        pop_inode(curi);
      } else if (progress) {
        dout(20) << __func__ << " dir progressed" << dendl;
        // we added new stuff to top of stack, so reset ourselves there
        i = inode_stack.begin();
      } else {
        dout(20) << __func__ << " dir no-op: " << *curi << dendl;
	if (curi->item_scrub.is_on_list()) {
	  pop_inode(curi);
	  push_inode_bottom(curi);
	}
      }

      can_continue = progress || terminal || completed;
    }
  }
}

void ScrubStack::scrub_dir_inode(CInode *in,
                                 bool *added_children,
                                 bool *terminal,
                                 bool *done)
{
  dout(10) << __func__ << *in << dendl;

  *added_children = false;
  bool all_frags_terminal = true;
  bool all_frags_done = true;

  ScrubHeader& header = in->scrub_info()->header;

  if (header.recursive) {
    list<frag_t> scrubbing_frags;
    list<CDir*> scrubbing_cdirs;
    in->scrub_dirfrags_scrubbing(&scrubbing_frags);
    dout(20) << __func__ << " iterating over " << scrubbing_frags.size()
      << " scrubbing frags" << dendl;
    for (list<frag_t>::iterator i = scrubbing_frags.begin();
	i != scrubbing_frags.end();
	++i) {
      // turn frags into CDir *
      CDir *dir = in->get_dirfrag(*i);

      if (dir) {
	if (dir->needs_scrub_reset()) {
	  dir->scrub_reset();
	}
	scrubbing_cdirs.push_back(dir);
	dout(25) << __func__ << " got CDir " << *dir << " presently scrubbing" << dendl;
      } else {
	if (in->scrub_is_in_progress()) {
	  MDSInternalContextBase *c = NULL;
	  in->scrub_finished(&c);
	  if (c) {
	    finisher->queue(new MDSIOContextWrapper(mdcache->mds, c), 0);
	  }
	}
	dout(25) << __func__ << " missing dirfrag " << *i << " skip scrubbing" << dendl;
      }
    }

    dout(20) << __func__ << " consuming from " << scrubbing_cdirs.size()
	     << " scrubbing cdirs" << dendl;

    list<CDir*>::iterator i = scrubbing_cdirs.begin();
    while (g_conf->mds_max_scrub_ops_in_progress > scrubs_in_progress) {
      // select next CDir
      CDir *cur_dir = NULL;
      if (i != scrubbing_cdirs.end()) {
	cur_dir = *i;
	++i;
	dout(20) << __func__ << " got cur_dir = " << *cur_dir << dendl;
      } else {
	bool skip = false;
	bool ready = get_next_cdir(in, &cur_dir, &skip);
	dout(20) << __func__ << " get_next_cdir ready=" << ready << dendl;
	//dout(20) << __func__ << " get_next_cdir skip=" << skip << dendl;
	//if (skip) continue;

	if (ready && cur_dir) {
	  scrubbing_cdirs.push_back(cur_dir);
	} else if (!ready) {
	  // We are waiting for load of a frag
	  all_frags_done = false;
	  all_frags_terminal = false;
	  break;
	} else {
	  // Finished with all frags
	  break;
	}
      }
      // scrub that CDir
      bool frag_added_children = false;
      bool frag_terminal = true;
      bool frag_done = false;
      scrub_dirfrag(cur_dir, header,
		    &frag_added_children, &frag_terminal, &frag_done, false);
      if (frag_done && cur_dir->inode->scrub_is_in_progress()) {
	cur_dir->inode->scrub_dirfrag_finished(cur_dir->frag);
      }
      *added_children |= frag_added_children;
      all_frags_terminal = all_frags_terminal && frag_terminal;
      all_frags_done = all_frags_done && frag_done;
    }

    dout(20) << "finished looping; all_frags_terminal=" << all_frags_terminal
	     << ", all_frags_done=" << all_frags_done << dendl;
  } else {
    dout(20) << "!scrub_recursive" << dendl;
  }

  if (all_frags_done) {
    assert (!*added_children); // can't do this if children are still pending

    // OK, so now I can... fire off a validate on the dir inode, and
    // when it completes, come through here again, noticing that we've
    // set a flag to indicate the validate happened, and 
    scrub_dir_inode_final(in);
  }

  *terminal = all_frags_terminal;
  *done = all_frags_done;
  dout(10) << __func__ << " is exiting " << *terminal << " " << *done << dendl;
  return;
}

bool ScrubStack::get_next_cdir(CInode *in, CDir **new_dir, bool *skip)
{
  dout(20) << __func__ << " on " << *in << dendl;
  frag_t next_frag;
  int r = in->scrub_dirfrag_next(&next_frag);
  assert (r >= 0);

  if (r == 0) {
    // we got a frag to scrub, otherwise it would be ENOENT
    dout(25) << "looking up new frag " << next_frag << dendl;
    if (!in->is_auth()) {
      dout(20) << __func__ << " ssss-lost auth for " << *in << dendl;
      /*
      MMDSScrubPath *msg = new MMDSScrubPath(dirfrag_t(in->ino(), next_frag),
					     in->scrub_info()->header);
      dout(20) << __func__ << " lost auth for " << *in << ", sending to "
	       << in->authority().first << dendl;
      in->scrub_initialize(NULL, in->scrub_info()->header, NULL);
      mdcache->mds->send_message_mds(msg, in->authority().first);
      */
      *skip = true;
      //return true;
      return false;
    }
    //CDir *next_dir = in->get_or_open_dirfrag(mdcache, next_frag);
    CDir *next_dir = in->get_dirfrag(next_frag);
    if (!next_dir) dout(20) << "xoxo- need to open " << next_frag << dendl;
    next_dir = in->get_or_open_dirfrag(mdcache, next_frag);

    if (!next_dir->is_complete() && next_dir->is_auth()) {
      scrubs_in_progress++;
      dout(20) << "fetching " << *next_dir << dendl;
      next_dir->fetch(&scrub_kick);
      return false;
    } else if (!next_dir->is_auth()) {
      dout(20) << __func__ << " ssss-no auth for " << *next_dir << dendl;
      if (next_dir->is_ambiguous_auth()) {
	dout(20) << __func__ << " ssss-ambiguous inside " << dendl;
      }
      MMDSScrubPath *msg = new MMDSScrubPath(next_dir->dirfrag(),
					     in->scrub_info()->header);
      dout(20) << __func__ << " sending to " << next_dir->authority().first
	       << dendl;
      pop_inode(in);
      next_dir->scrub_abort();
      mdcache->mds->send_message_mds(msg, next_dir->authority().first);
      *skip = true;
      //return true;
      return false;
    }
      
    *new_dir = next_dir;
    dout(25) << "returning dir " << *new_dir << dendl;
    return true;
  }
  assert(r == ENOENT);
  // there are no dirfrags left
  *new_dir = NULL;
  return true;
}

class C_InodeValidated : public MDSInternalContext
{
  public:
    ScrubStack *stack;
    CInode::validated_data result;
    CInode *target;

    C_InodeValidated(MDSRank *mds, ScrubStack *stack_, CInode *target_)
      : MDSInternalContext(mds), stack(stack_), target(target_)
    {}

    void finish(int r)
    {
      stack->_validate_inode_done(target, r, result);
    }
};


void ScrubStack::scrub_dir_inode_final(CInode *in)
{
  dout(20) << __func__ << *in << dendl;

  // Two passes through this function.  First one triggers inode validation,
  // second one sets finally_done
  // FIXME: kind of overloading scrub_in_progress here, using it while
  // dentry is still on stack to indicate that we have finished
  // doing our validate_disk_state on the inode
  // FIXME: the magic-constructing scrub_info() is going to leave
  // an unneeded scrub_infop lying around here
  if (!in->scrub_info()->children_scrubbed) {
    if (!in->scrub_info()->on_finish) {
      scrubs_in_progress++;
      in->scrub_set_finisher(&scrub_kick);
    }

    in->scrub_children_finished();
    C_InodeValidated *fin = new C_InodeValidated(mdcache->mds, this, in);
    in->validate_disk_state(&fin->result, fin);
  }

  return;
}

class C_MDS_ScrubDirfrag : public MDSInternalContext {
protected:
  ScrubStack *ss;
  dirfrag_t fg;
  ScrubHeader header;
  bool frob;

public:
  C_MDS_ScrubDirfrag (ScrubStack* _ss, dirfrag_t _fg,
		      ScrubHeader _header, bool _frob)
    : MDSInternalContext(_ss->mdcache->mds), ss(_ss), fg(_fg),
      header(_header), frob(_frob) {}

  void finish (int r) {
    bool added_children, is_terminal, done;
    CDir *dir = mds->mdcache->get_dirfrag(fg);
    if (!dir) {
      if (!ss->mdcache->have_inode(fg.ino)) {
	C_MDS_ScrubDirfrag *sdf = new C_MDS_ScrubDirfrag(ss, fg, header, true);
	ss->mdcache->open_ino(fg.ino, 0, sdf);
      } else {
	CInode *ino = ss->mdcache->get_inode(fg.ino);
	assert(ino);
	assert(ino->is_dir());
	LogChannelRef clog = ss->mdcache->mds->clog;
	clog->info() << "got : " << *ino;
	if (!ino->is_auth()) {
	  clog->info() << __func__ << *ino << " ssss-no auth in context";

	  {
	  MMDSScrubPath *msg = new MMDSScrubPath(fg, header);
	  clog->info() << "sending to " << ino->authority().first;
	  mds->send_message_mds(msg, ino->authority().first);
	  if (ino->item_scrub.is_on_list()) {
	    ss->pop_inode(ino);
	  }
	  }
	  MDSInternalContextBase *c = NULL;
	  if (ino->scrub_is_in_progress()) {
	    ino->scrub_finished(&c);
	  }
	  if (c) {
	    ss->finisher->queue(new MDSIOContextWrapper(mds, c), 0);
	  }
	} else {
	  dir = ino->get_dirfrag(fg.frag);
	  assert(dir);
	}
      }
      ss->kick_off_scrubs();
      return;
    }
    dir->scrub_info()->header = header;
    {
      LogChannelRef clog = ss->mdcache->mds->clog;
      clog->info() << " blah, sent tag " << header.tag;
    }
    ss->scrub_dirfrag(dir, header, &added_children, &is_terminal, &done, frob);
    if (dir->is_auth() && is_terminal && done) {
      dir->scrub_initialize(header);
      ss->scrub_dir_inode_final(dir->get_inode());
    } else {
      if (dir->is_auth() && !dir->get_inode()->scrub_is_in_progress()) {
	dir->get_inode()->scrub_initialize(NULL, header, NULL);
      }
      if (dir->is_auth()) {
	ss->push_inode_bottom(dir->get_inode());
      } else if (dir->is_scrubbing()) {
	dir->scrub_finished();
      }
      ss->scrubs_in_progress--;
    }
    ss->kick_off_scrubs();
  }
};
    
void ScrubStack::scrub_dirfrag(CDir *dir,
			       ScrubHeader& header,
			       bool *added_children, bool *is_terminal,
			       bool *done, bool frob)
{
  assert(dir != NULL);

  dout(20) << __func__ << " " << dir << dendl;
  dout(20) << dir->authority() << dendl;
  dout(20) << __func__ << " on " << *dir << dendl;
  dout(20) << *dir << " header " << header << " tag " << header.tag << dendl;
  *added_children = false;
  *is_terminal = false;
  *done = false;

  if (dir->is_ambiguous_auth()) {
    dout(20) << "scrub: ambiguous: " << *dir << dendl;
    C_MDS_ScrubDirfrag *sdf = new C_MDS_ScrubDirfrag(this, dir->dirfrag(),
						     header, true);
    dir->scrub_remove_dentries();
    dir->scrub_reset();
    dir->add_waiter(CDir::WAIT_SCRUBQUEUE, sdf);
    if (!frob) pop_inode(dir->inode);
    /*
    *done = true;
    *is_terminal = true;
    */
    return;
  }

  if (dir->is_frozen()) {
    dout(20) << "scrub: frozen: " << *dir << dendl;
    C_MDS_ScrubDirfrag *sdf = new C_MDS_ScrubDirfrag(this, dir->dirfrag(),
						     header, true);
    dir->scrub_remove_dentries();
    dir->scrub_reset();
    dir->add_waiter(CDir::WAIT_UNFREEZE, sdf);
    if (!frob) pop_inode(dir->inode);
    /*
    *done = true;
    *is_terminal = true;
    */
    return;
  }

  if (!dir->is_auth()) {
    dout(20) << __func__ << " ssss-subtree boundary at " << *dir << dendl;
    MMDSScrubPath *msg = new MMDSScrubPath(dir->dirfrag(), header);
    dout(20) << __func__ << " sending to " << dir->authority().first
	     << dendl;
    if (!dir->is_scrubbing()) {
      dir->scrub_initialize(header);
    }
    if (!dir->get_inode()->scrub_is_in_progress()) {
      dir->get_inode()->scrub_initialize(NULL, header, NULL);
    }
    dir->scrub_remove_dentries();
    mdcache->mds->send_message_mds(msg, dir->authority().first);
    {
      MDSInternalContextBase *c = NULL;
      if (dir->inode->scrub_is_in_progress()) {
	dir->inode->scrub_finished(&c);
      } else {
	dout(20) << __func__ << " no scrub on " << dir->inode << dendl;
      }
      //dir->scrub_finished();
      if (c) {
	finisher->queue(new MDSIOContextWrapper(mdcache->mds, c), 0);
      }
    }
    *done = true;
    *is_terminal = true;
    return;
  }

  //if (!dir->scrub_info()->directory_scrubbing) {
    // Get the frag complete before calling
    // scrub initialize, so that it can populate its lists
    // of dentries.
    if (!dir->is_complete()) {
      scrubs_in_progress++;
      if (frob) {
	dout(20) << "need fetch, header " << header.tag << dendl;
	C_MDS_ScrubDirfrag *sdf = new C_MDS_ScrubDirfrag(this, dir->dirfrag(),
							 header, frob);
	dir->fetch(sdf);
      } else {
	dir->fetch(&scrub_kick);
      }
      return;
    }

  if (!dir->scrub_info()->directory_scrubbing) {
    dir->scrub_initialize(header);
  }

  int r = 0;
  while(r == 0) {
    CDentry *dn = NULL;
    scrubs_in_progress++;
    if (frob) {
      C_MDS_ScrubDirfrag *sdf = new C_MDS_ScrubDirfrag(this, dir->dirfrag(),
						       header, frob);
      r = dir->scrub_dentry_next(sdf, &dn);
    } else {
      r = dir->scrub_dentry_next(&scrub_kick, &dn);
    }
    if (r != EAGAIN) {
      scrubs_in_progress--;
    }

    if (r == EAGAIN) {
      // Drop out, CDir fetcher will call back our kicker context
      dout(20) << __func__ << " waiting for fetch on " << *dir << dendl;
      return;
    }

    if (r == ENOENT) {
      // Nothing left to scrub, are we done?
      std::list<CDentry*> scrubbing;
      dir->scrub_dentries_scrubbing(&scrubbing);
      if (scrubbing.empty()) {
        dout(20) << __func__ << " dirfrag done: " << *dir << dendl;
        // FIXME: greg: What's the diff meant to be between done and terminal
	dir->scrub_finished();
        *done = true;
        *is_terminal = true;
      } else {
        dout(20) << __func__ << " " << scrubbing.size() << " dentries still "
	  "scrubbing in " << *dir << dendl;
	bool changed = false;
	for (auto i : scrubbing) {
	  // check scrubstack for descendents and reorder if needed
	  dout(20) << *i << dendl;
	  CInode *in = i->get_projected_inode();
	  if (dir->get_inode()->item_scrub.is_on_list() &&
	      in->item_scrub.is_on_list()) {
	    pop_inode(in);
	    push_inode(in);
	    changed = true;
	  }
	}
	if (changed) kick_off_scrubs();
      }
      return;
    }

    // scrub_dentry_next defined to only give EAGAIN, ENOENT, 0 -- we should
    // never get random IO errors here.
    assert(r == 0);

    _enqueue_inode(dn->get_projected_inode(), dn, header, NULL, true);

    *added_children = true;
  }
}

void ScrubStack::scrub_complete(dirfrag_t df, nest_info_t rstat, utime_t start)
{
  CDir *dir = mdcache->get_dirfrag(df);
  assert(dir);

  CDir *target = dir->get_parent_dir();
  assert(target);

  dout(20) << "yyyy- " << rstat << " " << dir->inode->inode.rstat << dendl;
  if (rstat.same_sums(dir->inode->inode.rstat)) {
    dout(20) << "ssss- sums match for " << *dir << dendl;
  } else {
    dout(20) << "ssss- divergence for " << *dir << dendl;
    mdcache->repair_inode_stats(dir->inode);
    dout(20) << "ssss- completed repair for " << *dir->inode << dendl;
  }

  MDSInternalContextBase *c = NULL;
  if (dir->inode->scrub_is_in_progress()) {
    dir->inode->scrub_finished(&c);
    dir->scrub_abort();
  } /* XXX */ /* XXX */

  dout(20) << "ssss-continuing with " << *target << dendl;

  /* XXX XXX XXX XXX XXX */

  auto it = scrub_ops.find(start);
  //assert(it != scrub_ops.end());

  ScrubHeader header = dir->scrub_info()->header;
  dout(20) << "zzzz-" << header.tag << dendl;

  if (!target->inode->scrub_is_in_progress()) {
    mdcache->enqueue_scrub_dirfrag(target->dirfrag(), header.tag,
				   header.start, header.oi,
				   header.force, header.repair);
  }

  push_inode(target->inode);
  kick_off_scrubs();
}


void ScrubStack::scrub_file_inode(CInode *in)
{
  C_InodeValidated *fin = new C_InodeValidated(mdcache->mds, this, in);
  // At this stage the DN is already past scrub_initialize, so
  // it's in the cache, it has PIN_SCRUBQUEUE and it is authpinned
  in->validate_disk_state(&fin->result, fin);
}

void ScrubStack::_validate_inode_done(CInode *in, int r,
				      const CInode::validated_data &result)
{
  // FIXME: do something real with result!  DamageTable!  Spamming
  // the cluster log for debugging purposes
  LogChannelRef clog = mdcache->mds->clog;
  clog->info() << __func__ << " " << *in << " r=" << r;
#if 0
  assert(in->scrub_infop != NULL);
  in->scrub_infop->inode_validated = true;
#endif
  const ScrubHeader& header = in->scrub_info()->header;

  MDSInternalContextBase *c = NULL;
  if (in->scrub_is_in_progress()) {
    in->scrub_finished(&c);
  }

  if (!header.recursive && in == header.origin) {
    if (r >= 0) { // we got into the scrubbing dump it
      result.dump(header.formatter);
    } else { // we failed the lookup or something; dump ourselves
      header.formatter->open_object_section("results");
      header.formatter->dump_int("return_code", r);
      header.formatter->close_section(); // results
    }
  }
  if (c) {
    finisher->queue(new MDSIOContextWrapper(mdcache->mds, c), 0);
  }
}

ScrubStack::C_KickOffScrubs::C_KickOffScrubs(MDCache *mdcache, ScrubStack *s)
  : MDSInternalContext(mdcache->mds), stack(s) { }
