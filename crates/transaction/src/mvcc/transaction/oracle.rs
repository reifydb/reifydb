// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::conflict::Cm;
use crate::mvcc::watermark::{Closer, WaterMark};
use core::ops::AddAssign;
use smallvec_wrapper::TinyVec;
use std::borrow::Cow;
use std::sync::{Mutex, MutexGuard};

#[derive(Debug)]
pub(super) struct OracleInner<C> {
    pub next_txn_ts: u64,

    pub last_cleanup_ts: u64,

    /// Contains all committed writes (contains fingerprints
    /// of keys written and their latest commit counter).
    pub(super) committed_txns: TinyVec<CommittedTxn<C>>,
}

pub(super) enum CreateCommitTimestampResult<C> {
    Timestamp(u64),
    Conflict(Option<C>),
}

#[derive(Debug)]
pub(super) struct Oracle<C> {
    // write_serialize_lock is for ensuring that transactions go to the write
    // channel in the same order as their commit timestamps.
    pub(super) write_serialize_lock: Mutex<()>,

    pub(super) inner: Mutex<OracleInner<C>>,

    /// Used by DB
    pub(super) rx: WaterMark,

    /// Used to block new transaction, so all previous commits are visible to a new read.
    pub(super) tx: WaterMark,

    /// closer is used to stop watermarks.
    closer: Closer,
}

impl<C> Oracle<C>
where
    C: Cm,
{
    pub(super) fn new_commit_ts(
        &self,
        done_read: &mut bool,
        read_ts: u64,
        mut conflict_manager: Option<C>,
    ) -> CreateCommitTimestampResult<C> {
        let mut inner = self.inner.lock().unwrap();

        let conflict_manager = conflict_manager.take().unwrap();

        for committed_txn in inner.committed_txns.iter() {
            // If the committed_txn.ts is less than txn.read_ts that implies that the
            // committed_txn finished before the current transaction started.
            // We don't need to check for conflict in that case.
            // This change assumes linearizability. Lack of linearizability could
            // cause the read ts of a new txn to be lower than the commit ts of
            // a txn before it (@mrjn).
            if committed_txn.ts <= read_ts {
                continue;
            }

            if let Some(old_conflict_manager) = &committed_txn.conflict_manager {
                if conflict_manager.has_conflict(old_conflict_manager) {
                    return CreateCommitTimestampResult::Conflict(Some(conflict_manager));
                }
            }
        }

        let ts = {
            if !*done_read {
                self.rx.done(read_ts);
                *done_read = true;
            }

            self.cleanup_committed_transactions(true, &mut inner);

            // This is the general case, when user doesn't specify the read and commit ts.
            let ts = inner.next_txn_ts;
            inner.next_txn_ts += 1;
            self.tx.begin(ts);
            ts
        };

        assert!(ts >= inner.last_cleanup_ts);

        // We should ensure that txns are not added to o.committedTxns slice when
        // conflict detection is disabled otherwise this slice would keep growing.
        inner.committed_txns.push(CommittedTxn { ts, conflict_manager: Some(conflict_manager) });

        CreateCommitTimestampResult::Timestamp(ts)
    }

    fn cleanup_committed_transactions(
        &self,
        detect_conflicts: bool,
        inner: &mut MutexGuard<OracleInner<C>>,
    ) {
        if !detect_conflicts {
            // When detect_conflicts is set to false, we do not store any
            // committedTxns and so there's nothing to clean up.
            return;
        }

        let max_read_ts = self.rx.done_until();

        assert!(max_read_ts >= inner.last_cleanup_ts);

        // do not run clean up if the max_read_ts (read timestamp of the
        // oldest transaction that is still in flight) has not increased
        if max_read_ts == inner.last_cleanup_ts {
            return;
        }

        inner.last_cleanup_ts = max_read_ts;

        inner.committed_txns.retain(|txn| txn.ts > max_read_ts);
    }
}

impl<C> Oracle<C> {
    pub fn new(
        rx_mark_name: Cow<'static, str>,
        tx_mark_name: Cow<'static, str>,
        next_txn_ts: u64,
    ) -> Self {
        let closer = Closer::new(2);
        Self {
            write_serialize_lock: Mutex::new(()),
            inner: Mutex::new(OracleInner {
                next_txn_ts,
                last_cleanup_ts: 0,
                committed_txns: TinyVec::new(),
            }),
            rx: WaterMark::new(rx_mark_name, closer.clone()),
            tx: WaterMark::new(tx_mark_name, closer.clone()),
            closer,
        }
    }

    pub(super) fn read_ts(&self) -> u64 {
        let read_ts = {
            let inner = self.inner.lock().unwrap();

            let read_ts = inner.next_txn_ts - 1;
            self.rx.begin(read_ts);
            read_ts
        };

        // Wait for all txns which have no conflicts, have been assigned a commit
        // timestamp and are going through the write to value log and LSM tree
        // process. Not waiting here could mean that some txns which have been
        // committed would not be read.
        if let Err(e) = self.tx.wait_for_mark(read_ts) {
            panic!("{e}");
        }
        read_ts
    }

    pub(super) fn increment_next_ts(&self) {
        self.inner.lock().unwrap().next_txn_ts.add_assign(1);
    }

    pub(super) fn discard_at_or_below(&self) -> u64 {
        self.rx.done_until()
    }

    pub(super) fn done_read(&self, read_ts: u64) {
        self.rx.done(read_ts)
    }

    pub(super) fn done_commit(&self, cts: u64) {
        self.tx.done(cts)
    }

    fn stop(&self) {
        self.closer.signal_and_wait();
    }
}

impl<S> Drop for Oracle<S> {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Debug)]
pub(super) struct CommittedTxn<C> {
    ts: u64,
    /// Keeps track of the entries written at timestamp ts.
    conflict_manager: Option<C>,
}
