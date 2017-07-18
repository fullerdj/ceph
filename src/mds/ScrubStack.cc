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
#include <atomic>

#include "ScrubStack.h"
#include "common/Finisher.h"
#include "common/Mutex.h"
#include "mds/MDSRank.h"
#include "mds/MDCache.h"
#include "mds/MDSContinuation.h"
#include "messages/MMDSScrubPath.h"
#include "messages/MMDSScrubInode.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, scrubstack->mdcache->mds)
static ostream& _prefix(std::ostream *_dout, MDSRank *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".scrubstack ";
}

std::ostream& operator<<(std::ostream& out, const ScrubHeader& sh) {
  out << "{ " << sh.get_oi() << " " << sh.get_tag() << " }";
  return out;
}

void ScrubStack::push_inode(CInode *in)
{
  dout(20) << "pushing " << *in << " on top of ScrubStack" << dendl;
  Mutex::Locker l(stack_lock);
  if (!in->item_scrub.is_on_list()) {
    in->get(CInode::PIN_SCRUBQUEUE);
    stack_size++;
  }
  inode_stack.push_front(&in->item_scrub);
}

void ScrubStack::push_inode_bottom(CInode *in)
{
  dout(20) << "pushing " << *in << " on bottom of ScrubStack" << dendl;
  Mutex::Locker l(stack_lock);
  if (!in->item_scrub.is_on_list()) {
    in->get(CInode::PIN_SCRUBQUEUE);
    stack_size++;
  }
  inode_stack.push_back(&in->item_scrub);
}

void ScrubStack::pop_inode_if(CInode *in)
{
  bool pop;
  {
    Mutex::Locker l(stack_lock);
    if (in->item_scrub.is_on_list()) {
      pop = true;
    } else {
      pop = false;
    }
  }

  if (pop) {
    pop_inode(in);
  }
}

void ScrubStack::pop_inode(CInode *in)
{
  dout(20) << "popping " << *in << " off of ScrubStack" << dendl;
  Mutex::Locker l(stack_lock);
  assert(in->item_scrub.is_on_list());
  in->put(CInode::PIN_SCRUBQUEUE);
  in->item_scrub.remove_myself();
  stack_size--;
}

CInode *ScrubStack::front_inode() {
  Mutex::Locker l(stack_lock);
  return stack_size ? inode_stack.front() : nullptr;
}

void ScrubStack::dump_stack() {
  Mutex::Locker l(stack_lock);
  dout(20) << __func__ << dendl;
  for (auto i : inode_stack) {
    dout(20) << *i << dendl;
  }
  dout(20) << "end stack" << dendl;
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
  } else if (in->scrub_info()->scrub_parent == nullptr && parent) {
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
  int tries = 0;

  dump_stack();

  CInode *nop = nullptr;
  while (g_conf->mds_max_scrub_ops_in_progress > scrubs_in_progress &&
	 can_continue && front_inode()) {
    CInode *curi = front_inode();

    dout(20) << __func__ << " examining " << *curi << dendl;

    if (!curi->is_dir()) {
      // it's a regular file, symlink, or hard link
      pop_inode(curi); // we only touch it this once, so remove from stack

      if (!curi->scrub_info()->on_finish) {
	curi->scrub_set_finisher(new C_KickOffScrubs(mdcache, this));
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
      } else {
        dout(20) << __func__ << " dir no-op: " << *curi << dendl;
	if (curi->item_scrub.is_on_list()) {
	  pop_inode(curi);
	  push_inode_bottom(curi);
	  if (!nop) {
	    nop = curi;
	    dout(20) << "looking for " << *curi << dendl;
	    continue;
	  } else if (nop != curi) {
	    if (tries++ < stack_size) {
	      dout(20) << "trying again" << dendl;
	      continue;
	    } else {
	      break;
	    }
	  } else {
	    dout(20) << "found " << *curi << dendl;
	  }
	} else {
	  continue;
	}
      }

      can_continue = progress || terminal || completed;
    }
  }

  {
    Mutex::Locker l(stack_lock);

    dout(20) << pending_dirfrags.size() << " pending dirfrags" << dendl;

    if (!pending_dirfrags.empty()) {
      pair<dirfrag_t, ScrubHeader> pr = pending_dirfrags.front();
      dout(20) << "queuing " << pr.first << dendl;
      pending_dirfrags.pop_front();
      C_MDS_ScrubDirfrag *sdf = new C_MDS_ScrubDirfrag(this,
						       pr.first, pr.second);
      MDSIOContextWrapper *c = new MDSIOContextWrapper(mdcache->mds, sdf);
      mdcache->mds->finisher->queue(c);
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

  if (header.get_recursive()) {
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
	dout(25) << __func__ << " got CDir " << *dir
		 << " presently scrubbing" << dendl;
      } else {
	dout(25) << __func__ << " missing dirfrag " << *i
		 << " skip scrubbing" << dendl;
	if (in->scrub_is_in_progress()) {
	  in->scrub_dirfrag_finished(*i);
	  MDSInternalContextBase *c = in->scrub_info()->on_finish;
	  if (c) {
	    in->scrub_info()->on_finish = nullptr;
	    finisher->queue(new MDSIOContextWrapper(mdcache->mds, c), 0);
	  }
	}
      }
    }

    dout(20) << __func__ << " consuming from " << scrubbing_cdirs.size()
	     << " scrubbing cdirs" << dendl;

    list<CDir*>::iterator i = scrubbing_cdirs.begin();
    while (g_conf->mds_max_scrub_ops_in_progress > scrubs_in_progress) {
      // select next CDir
      CDir *cur_dir = nullptr;
      if (i != scrubbing_cdirs.end()) {
	cur_dir = *i;
	++i;
	dout(20) << __func__ << " got cur_dir = " << *cur_dir << dendl;
      } else {
	bool ready = get_next_cdir(in, &cur_dir);
	dout(20) << __func__ << " get_next_cdir ready=" << ready << dendl;

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
      scrub_dirfrag(cur_dir, header, &frag_added_children, &frag_terminal,
		    &frag_done, new C_KickOffScrubs(mdcache, this));
      if (frag_done) {
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

bool ScrubStack::get_next_cdir(CInode *in, CDir **new_dir)
{
  dout(20) << __func__ << " on " << *in << dendl;
  frag_t next_frag;
  int r = in->scrub_dirfrag_next(&next_frag);
  assert (r >= 0);

  if (r == 0) {
    // we got a frag to scrub, otherwise it would be ENOENT
    dout(25) << "looking up new frag " << next_frag << dendl;
    CDir *next_dir = in->get_dirfrag(next_frag);

    if (!next_dir) {
      if (in->is_auth()) {
	next_dir = in->get_dirfrag(next_frag);
	fragtree_t dft = in->dirfragtree;
	if (!next_dir) {
	  if (dft.contains(next_frag)) {
	    next_dir = in->get_or_open_dirfrag(mdcache, next_frag);
	    assert(next_dir);
	  } else {
	    dout(25) << "stale dirfrag " << next_frag << dendl;

	    list<frag_t> ls;
	    dft.get_leaves_under(next_frag, ls);
	    if (ls.empty()) {
	      frag_t branch = dft.get_branch(next_frag);
	      dft.get_leaves_under(branch, ls);
	    }

	    for (frag_t f : ls) {
	      in->scrub_info()->dirfrag_stamps[f];
	    }

	    return get_next_cdir(in, new_dir);
	  }
	}
      } else {
	mdcache->open_remote_dirfrag(in, next_frag, new C_KickOffScrubs(mdcache, this));
	return false;
      }
    }

    if (!next_dir->is_complete() && next_dir->is_auth()) {
      dout(20) << "fetching " << *next_dir << dendl;
      next_dir->fetch(new C_KickOffScrubs(mdcache, this));
      return false;
    } else if (!next_dir->is_auth()) {
      dout(20) << __func__ << " sending to " << next_dir->authority().first
	       << dendl;
      MMDSScrubPath *msg = new MMDSScrubPath(next_dir->dirfrag(),
					     in->scrub_info()->header);
      //pop_inode_if(in);
      next_dir->scrub_abort();
      mdcache->mds->send_message_mds(msg, next_dir->authority().first);
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

    void finish(int r) override
    {
      target->auth_unpin(this);
      stack->_validate_inode_done(target, r, result);
    }
};

class C_ValidateDiskState : public MDSInternalContext
{
  public:
    ScrubStack *stack;
    CInode *in;

    C_ValidateDiskState(MDSRank *mds, ScrubStack *stack_, CInode *in_)
      : MDSInternalContext(mds), stack(stack_), in(in_) {}

    void finish(int r)
    {
      C_InodeValidated *fin = new C_InodeValidated(mds, stack, in);
      in->auth_pin(fin);
      in->validate_disk_state(&fin->result, fin);
    }
};

void ScrubStack::scrub_dir_inode_final(CInode *in)
{
  dout(20) << __func__ << " " << *in << dendl;

  if (!in->is_auth()) {
    if (in->has_subtree_root_dirfrag(mdcache->mds->get_nodeid())) {
      MMDSScrubInode *msg = new MMDSScrubInode(in->inode.ino,
					       in->scrub_info()->header);
      mdcache->mds->send_message_mds(msg, in->authority().first);
    }
    return;
  }

  // Two passes through this function.  First one triggers inode validation,
  // second one sets finally_done
  // FIXME: kind of overloading scrub_in_progress here, using it while
  // dentry is still on stack to indicate that we have finished
  // doing our validate_disk_state on the inode
  // FIXME: the magic-constructing scrub_info() is going to leave
  // an unneeded scrub_infop lying around here
  if (!in->scrub_info()->children_scrubbed) {
    if (!in->scrub_info()->on_finish) {
      in->scrub_set_finisher(new C_KickOffScrubs(mdcache, this));
    }

    in->scrub_children_finished();

    CDir *dir = in->get_parent_dir();

    if (dir && (/*dir->is_freezing_tree() ||*/ dir->is_frozen_tree())) {
      dout(20) << __func__ << " " << *in
	       << " is frozen, waiting on validate" << dendl;

      C_ValidateDiskState *fin = new C_ValidateDiskState(mdcache->mds,
							 this, in);
      dir->add_waiter(CDir::WAIT_SINGLEAUTH, fin);
      return;
    }

    C_InodeValidated *fin = new C_InodeValidated(mdcache->mds, this, in);
    in->auth_pin(fin);
    in->validate_disk_state(&fin->result, fin);
  }

  return;
}

void ScrubStack::scrub_dirfrag(CDir *dir,
			       ScrubHeader& header,
			       bool *added_children, bool *is_terminal,
			       bool *done,
			       MDSInternalContext *onfinish)
{
  assert(dir != NULL);

  dout(20) << __func__ << " on " << *dir << " header " << header << dendl;
  *added_children = false;
  *is_terminal = false;
  *done = false;

  if (dir->is_ambiguous_auth()) {
    dout(20) << "ambiguous: " << *dir << dendl;
    delete onfinish;
    C_MDS_ScrubDirfrag *sdf = new C_MDS_ScrubDirfrag(this, dir->dirfrag(),
						     header);
    dir->scrub_remove_dentries();
    dir->scrub_reset();
    dir->add_waiter(CDir::WAIT_SCRUBQUEUE, sdf);
    pop_inode_if(dir->inode);
    return;
  }

  if (dir->is_frozen()) {
    dout(20) << "frozen: " << *dir << dendl;
    delete onfinish;
    C_MDS_ScrubDirfrag *sdf = new C_MDS_ScrubDirfrag(this, dir->dirfrag(),
						     header);
    dir->scrub_remove_dentries();
    dir->scrub_reset();
    dir->add_waiter(CDir::WAIT_UNFREEZE, sdf);
    pop_inode_if(dir->inode);
    return;
  }

  if (!dir->is_auth()) {
    dout(20) << __func__ << " subtree boundary at " << *dir
	     << " sending to " << dir->authority().first << dendl;

    MMDSScrubPath *msg = new MMDSScrubPath(dir->dirfrag(), header);
    dir->scrub_remove_dentries();
    mdcache->mds->send_message_mds(msg, dir->authority().first);
    delete onfinish;

    *done = true;
    *is_terminal = true;
    return;
  }

  // Get the frag complete before calling
  // scrub initialize, so that it can populate its lists
  // of dentries.
  if (!dir->is_complete()) {
    dout(20) << "need fetch on " << *dir << dendl;
    dir->fetch(onfinish);
    return;
  }

  if (!dir->scrub_info()->directory_scrubbing) {
    dir->scrub_initialize(header);
  }

  int r = 0;
  while(r == 0) {
    CDentry *dn = NULL;
    r = dir->scrub_dentry_next(onfinish, &dn);

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
	if (dir->inode->scrub_is_in_progress()) {
	  dir->inode->scrub_dirfrag_finished(dir->frag); /* XXX? */
	}
	dir->scrub_finished();
        *done = true;
        *is_terminal = true;
      } else {
        dout(20) << __func__ << " " << scrubbing.size()
		 << " dentries still scrubbing in " << *dir << dendl;
	dout(25) << "dentries follow:" << dendl;
	for (auto i : scrubbing) {
	  dout(25) << *i << dendl;
	}
	dout(25) << "end dentries" << dendl;
	/*
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
	*/
      }
      delete onfinish;
      return;
    }

    // scrub_dentry_next defined to only give EAGAIN, ENOENT, 0 -- we should
    // never get random IO errors here.
    assert(r == 0);

    CInode *in = dn->get_projected_inode();
    assert(mdcache->have_inode(in->vino()));
    in = mdcache->get_inode(in->vino());
    //if (mdcache->have_inode(in->vino()))
    ////_enqueue_inode(dn->get_projected_inode(), dn, header, NULL, true);
    _enqueue_inode(in, dn, header, NULL, true);

    *added_children = true;
  }
  delete onfinish;
}

void ScrubStack::scrub_complete(dirfrag_t df, nest_info_t rstat)
{
  CDir *dir = mdcache->get_dirfrag(df);
  if (!dir) {
    /* XXX need to complete children. If a parent, just ignore it? */
    CInode *in = mdcache->get_inode(df.ino);
    assert(in);
    list<CDir *> ls;
    in->get_dirfrags_under(df.frag, ls);
    for (CDir *d : ls) {
      scrub_complete(d->dirfrag(), rstat);
    }
    return;
  }

  CDir *parent_dir = dir->get_parent_dir();
  assert(parent_dir);

  if (dir->inode->scrub_is_in_progress()) {
    dir->inode->scrub_dirfrag_finished(dir->frag);
    dir->scrub_abort();
  }

  parent_dir->scrub_dentry_finished(dir->inode->get_parent_dn());
  if (!dir->inode->item_scrub.is_on_list()) {
    push_inode(dir->inode);
  }
  ScrubHeader header = dir->scrub_info()->header;

  if (!parent_dir->inode->scrub_is_in_progress()) {
    mdcache->enqueue_scrub_dirfrag(parent_dir->dirfrag(), header.get_tag(),
				   header.get_oi(), header.get_force(),
				   header.get_repair());
  }

  if (!parent_dir->inode->item_scrub.is_on_list()) {
    push_inode(parent_dir->inode);
  }
  kick_off_scrubs();
}

void ScrubStack::scrub_file_inode(CInode *in)
{
  CDir *dir = in->get_parent_dir();
  dout(20) << __func__ << " checking parent: " << *dir << dendl;
  if (dir && (dir->is_freezing_tree() || dir->is_frozen_tree())) {
    dout(20) << "frozen, waiting on validate" << dendl;
    C_ValidateDiskState *fin = new C_ValidateDiskState(mdcache->mds, this, in);
    dir->add_waiter(CDir::WAIT_UNFREEZE, fin);
    return;
  }

  C_InodeValidated *fin = new C_InodeValidated(mdcache->mds, this, in);
  in->auth_pin(fin);
  in->validate_disk_state(&fin->result, fin);
}

void ScrubStack::_validate_inode_done(CInode *in, int r,
				      const CInode::validated_data &result)
{
  LogChannelRef clog = mdcache->mds->clog;
  const ScrubHeader& header = in->scrub_info()->header;
  //clog->info() << __func__ << " " << *in << " r=" << r;
  dout(20) << __func__ << " " << *in << " r=" << r << dendl;;

  std::string path;
  if (!result.passed_validation) {
    // Build path string for use in messages
    in->make_path_string(path, true);
  }

  if (result.backtrace.checked && !result.backtrace.passed) {
    // Record backtrace fails as remote linkage damage, as
    // we may not be able to resolve hard links to this inode
    mdcache->mds->damage_table.notify_remote_damaged(in->inode.ino, path);
  } else if (result.inode.checked && !result.inode.passed) {
    // Record damaged inode structures as damaged dentries as
    // that is where they are stored
    auto parent = in->get_projected_parent_dn();
    if (parent) {
      auto dir = parent->get_dir();
      mdcache->mds->damage_table.notify_dentry(
          dir->inode->ino(), dir->frag, parent->last, parent->name, path);
    }
  }

  // Inform the cluster log if we found an error
  if (!result.passed_validation) {
    clog->warn() << "Scrub error on inode " << in->ino()
                 << " (" << path << ") see " << g_conf->name
                 << " log and `damage ls` output for details";

    // Put the verbose JSON output into the MDS log for later inspection
    JSONFormatter f;
    result.dump(&f);
    std::ostringstream out;
    f.flush(out);
    derr << __func__ << " scrub error on inode " << *in << ": " << out.str()
         << dendl;
  } else {
    dout(10) << __func__ << " scrub passed on inode " << *in << dendl;
  }

  MDSInternalContextBase *c = nullptr;
  if (in->scrub_is_in_progress() && r == 0) {
    in->scrub_finished(&c);
  } else {
    c = in->scrub_info()->on_finish;
  }

  if (header.get_formatter()) {
    if (!header.get_recursive() && in == header.get_origin()) {
      if (r >= 0) { // we got into the scrubbing dump it
	result.dump(header.get_formatter());
      } else { // we failed the lookup or something; dump ourselves
	header.get_formatter()->open_object_section("results");
	header.get_formatter()->dump_int("return_code", r);
	header.get_formatter()->close_section(); // results
      }
    }
  }

  if (c) {
    finisher->queue(new MDSIOContextWrapper(mdcache->mds, c), 0);
  }
}
