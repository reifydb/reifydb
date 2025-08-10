// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::conflict::Conflict;
use crate::mvcc::transaction::version::VersionProvider;
use crate::mvcc::watermark::{Closer, WaterMark};
use reifydb_core::Version;
use std::borrow::Cow;
use std::sync::{Mutex, MutexGuard};

#[derive(Debug)]
pub(super) struct OracleInner<C, L>
where
    C: Conflict,
    L: VersionProvider,
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
    L: VersionProvider,
{
    // LOCK ORDERING: To prevent deadlocks, always acquire locks in this order:
    // 1. command_serialize_lock (if needed)
    // 2. inner
    // Never acquire inner first if command_serialize_lock will also be needed.

    // command_serialize_lock is for ensuring that transactions go to the command
    // channel in the same order as their commit timestamps.
    pub(super) command_serialize_lock: Mutex<()>,

    pub(super) inner: Mutex<OracleInner<C, L>>,

    /// Used by DB
    pub(super) query: WaterMark,
    /// Used to block new transaction, so all previous commits are visible to a new query.
    pub(super) command: WaterMark,

    /// closer is used to stop watermarks.
    closer: Closer,
}

impl<C, L> Oracle<C, L>
where
    C: Conflict,
    L: VersionProvider,
{
    pub(super) fn new_commit(
        &self,
        done_read: &mut bool,
        version: Version,
        conflicts: C,
    ) -> crate::Result<CreateCommitResult<C>> {
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
                    return Ok(CreateCommitResult::Conflict(conflicts));
                }
            }
        }

        let version = {
            if !*done_read {
                self.query.done(version);
                *done_read = true;
            }

            self.cleanup_committed_transactions(true, &mut inner);

            // This is the general case, when user doesn't specify the read and commit ts.
            let version = inner.clock.next()?;
            self.command.begin(version);
            version
        };

        assert!(version >= inner.last_cleanup);

        // We should ensure that txns are not added to o.committedTxns slice when
        // conflict detection is disabled otherwise this slice would keep growing.
        inner.committed.push(CommittedTxn { version, conflict_manager: Some(conflicts) });

        // Limit the size of committed transactions to prevent unbounded growth
        const MAX_COMMITTED_TXNS: usize = 10000;
        if inner.committed.len() > MAX_COMMITTED_TXNS {
            // Force cleanup of old transactions
            let cutoff = inner.committed.len() / 2;
            inner.committed.drain(0..cutoff);
        }

        Ok(CreateCommitResult::Success(version))
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

        let max_read_ts = self.query.done_until();

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
    L: VersionProvider,
{
    pub fn new(rx_mark_name: Cow<'static, str>, tx_mark_name: Cow<'static, str>, clock: L) -> Self {
        let closer = Closer::new(2);
        Self {
            command_serialize_lock: Mutex::new(()),
            inner: Mutex::new(OracleInner { clock, last_cleanup: 0, committed: Vec::new() }),
            query: WaterMark::new(rx_mark_name, closer.clone()),
            command: WaterMark::new(tx_mark_name, closer.clone()),
            closer,
        }
    }

    pub(super) fn version(&self) -> crate::Result<Version> {
        let version = {
            let inner = self.inner.lock().unwrap();

            let version = inner.clock.current()?;
            self.query.begin(version);
            version
        };

        // Wait for all txns which have no conflicts, have been assigned a commit
        // timestamp and are going through the write to value log and LSM tree
        // process. Not waiting here could mean that some txns which have been
        // committed would not be read.
        self.command.wait_for_mark(version);
        Ok(version)
    }

    pub(super) fn discard_at_or_below(&self) -> Version {
        self.query.done_until()
    }

    pub(super) fn done_query(&self, version: Version) {
        self.query.done(version)
    }

    pub(super) fn done_commit(&self, version: Version) {
        self.command.done(version)
    }

    fn stop(&self) {
        self.closer.signal_and_wait();
    }
}

impl<C, L> Drop for Oracle<C, L>
where
    C: Conflict,
    L: VersionProvider,
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
