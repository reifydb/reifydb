// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::conflict::Conflict;
use crate::mvcc::watermark::{Closer, WaterMark};
use reifydb_storage::{LogicalClock, Version};
use std::borrow::Cow;
use std::sync::{Mutex, MutexGuard};

#[derive(Debug)]
pub(super) struct OracleInner<C, L>
where
    C: Conflict,
    L: LogicalClock,
{
    pub clock: L,

    pub last_cleanup: Version,

    pub(super) committed: Vec<CommittedTxn<C>>,
}

pub(super) enum CreateCommitResult<C> {
    Success(Version),
    Conflict(C),
}

#[derive(Debug)]
pub(super) struct Oracle<C, L>
where
    C: Conflict,
    L: LogicalClock,
{
    // write_serialize_lock is for ensuring that transactions go to the write
    // channel in the same order as their commit timestamps.
    pub(super) write_serialize_lock: Mutex<()>,

    pub(super) inner: Mutex<OracleInner<C, L>>,

    /// Used by DB
    pub(super) rx: WaterMark,
    /// Used to block new transaction, so all previous commits are visible to a new read.
    pub(super) tx: WaterMark,

    /// closer is used to stop watermarks.
    closer: Closer,
}

impl<C, L> Oracle<C, L>
where
    C: Conflict,
    L: LogicalClock,
{
    pub(super) fn new_commit(
        &self,
        done_read: &mut bool,
        version: Version,
        conflicts: C,
    ) -> CreateCommitResult<C> {
        let mut inner = self.inner.lock().unwrap();

        for committed_txn in inner.committed.iter() {
            // If the committed_txn.ts is less than txn.read_ts that implies that the
            // committed_txn finished before the current transaction started.
            // We don't need to check for conflict in that case.
            // This change assumes linearizability. Lack of linearizability could
            // cause the read ts of a new txn to be lower than the commit ts of
            // a txn before it (@mrjn).
            if committed_txn.version <= version {
                continue;
            }

            if let Some(old_conflicts) = &committed_txn.conflict_manager {
                if conflicts.has_conflict(old_conflicts) {
                    return CreateCommitResult::Conflict(conflicts);
                }
            }
        }

        let version = {
            if !*done_read {
                self.rx.done(version);
                *done_read = true;
            }

            self.cleanup_committed_transactions(true, &mut inner);

            // This is the general case, when user doesn't specify the read and commit ts.
            let version = inner.clock.next();
            self.tx.begin(version);
            version
        };

        assert!(version >= inner.last_cleanup);

        // We should ensure that txns are not added to o.committedTxns slice when
        // conflict detection is disabled otherwise this slice would keep growing.
        inner.committed.push(CommittedTxn { version, conflict_manager: Some(conflicts) });

        CreateCommitResult::Success(version)
    }

    fn cleanup_committed_transactions(
        &self,
        detect_conflicts: bool,
        inner: &mut MutexGuard<OracleInner<C, L>>,
    ) {
        if !detect_conflicts {
            // When detect_conflicts is set to false, we do not store any
            // committedTxns and so there's nothing to clean up.
            return;
        }

        let max_read_ts = self.rx.done_until();

        assert!(max_read_ts >= inner.last_cleanup);

        // do not run clean up if the max_read_ts (read timestamp of the
        // oldest transaction that is still in flight) has not increased
        if max_read_ts == inner.last_cleanup {
            return;
        }

        inner.last_cleanup = max_read_ts;

        inner.committed.retain(|txn| txn.version > max_read_ts);
    }
}

impl<C, L> Oracle<C, L>
where
    C: Conflict,
    L: LogicalClock,
{
    pub fn new(rx_mark_name: Cow<'static, str>, tx_mark_name: Cow<'static, str>, clock: L) -> Self {
        let closer = Closer::new(2);
        Self {
            write_serialize_lock: Mutex::new(()),
            inner: Mutex::new(OracleInner { clock, last_cleanup: 0, committed: Vec::new() }),
            rx: WaterMark::new(rx_mark_name, closer.clone()),
            tx: WaterMark::new(tx_mark_name, closer.clone()),
            closer,
        }
    }

    pub(super) fn version(&self) -> Version {
        let version = {
            let inner = self.inner.lock().unwrap();

            let version = inner.clock.current() - 1;
            self.rx.begin(version);
            version
        };

        // Wait for all txns which have no conflicts, have been assigned a commit
        // timestamp and are going through the write to value log and LSM tree
        // process. Not waiting here could mean that some txns which have been
        // committed would not be read.
        self.tx.wait_for_mark(version);
        version
    }

    pub(super) fn discard_at_or_below(&self) -> Version {
        self.rx.done_until()
    }

    pub(super) fn done_read(&self, version: Version) {
        self.rx.done(version)
    }

    pub(super) fn done_commit(&self, version: Version) {
        self.tx.done(version)
    }

    fn stop(&self) {
        self.closer.signal_and_wait();
    }
}

impl<C, L> Drop for Oracle<C, L>
where
    C: Conflict,
    L: LogicalClock,
{
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Debug)]
pub(super) struct CommittedTxn<C> {
    version: Version,
    conflict_manager: Option<C>,
}
