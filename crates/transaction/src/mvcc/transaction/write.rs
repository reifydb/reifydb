// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use crate::mvcc::marker::Marker;
use crate::mvcc::types::Pending;
use reifydb_core::clock::LogicalClock;
use reifydb_core::delta::Delta;
use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, Version};

pub struct TransactionManagerTx<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    pub(super) version: Version,
    pub(super) size: u64,
    pub(super) count: u64,
    pub(super) oracle: Arc<Oracle<C, L>>,
    pub(super) conflicts: C,
    // stores any writes done by tx
    pub(super) pending_writes: P,
    pub(super) duplicates: Vec<Pending>,

    pub(super) discarded: bool,
    pub(super) done_read: bool,
}

impl<C, L, P> Drop for TransactionManagerTx<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    fn drop(&mut self) {
        if !self.discarded {
            self.discard();
        }
    }
}

impl<C, L, P> TransactionManagerTx<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    /// Returns the version of the transaction.
    pub fn version(&self) -> Version {
        self.version
    }

    /// Sets the current version of the transaction manager.
    pub fn as_of_version(&mut self, version: Version) {
        self.version = version;
    }

    /// Returns the pending writes
    pub fn pending_writes(&self) -> &P {
        &self.pending_writes
    }

    /// Returns the conflict manager.
    pub fn conflicts(&self) -> &C {
        &self.conflicts
    }
}

impl<C, L, P> TransactionManagerTx<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    /// This method is used to create a marker for the keys that are operated.
    /// It must be used to mark keys when end user is implementing iterators to
    /// make sure the transaction manager works correctly.
    pub fn marker(&mut self) -> Marker<'_, C> {
        Marker::new(&mut self.conflicts)
    }

    /// Returns a marker for the keys that are operated and the pending writes manager.
    /// As Rust's borrow checker does not allow to borrow mutable marker and the immutable pending writes manager at the same
    pub fn marker_with_pending_writes(&mut self) -> (Marker<'_, C>, &P) {
        (Marker::new(&mut self.conflicts), &self.pending_writes)
    }

    /// Marks a key is read.
    pub fn mark_read(&mut self, k: &EncodedKey) {
        self.conflicts.mark_read(k);
    }

    /// Marks a key is conflict.
    pub fn mark_conflict(&mut self, k: &EncodedKey) {
        self.conflicts.mark_conflict(k);
    }
}

impl<C, L, P> TransactionManagerTx<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    /// Set a key-value pair to the transaction.
    pub fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<(), reifydb_core::Error> {
        if self.discarded {
            return Err(reifydb_core::Error(reifydb_core::error::diagnostic::transaction::transaction_discarded()));
        }

        self.set_internal(key, row)
    }

    /// Removes a key.
    ///
    /// This is done by adding a delete marker for the key at commit timestamp.  Any
    /// reads happening before this timestamp would be unaffected. Any reads after
    /// this commit would see the deletion.
    pub fn remove(&mut self, key: &EncodedKey) -> Result<(), reifydb_core::Error> {
        if self.discarded {
            return Err(reifydb_core::Error(reifydb_core::error::diagnostic::transaction::transaction_discarded()));
        }
        self.modify(Pending { delta: Delta::Remove { key: key.clone() }, version: 0 })
    }

    /// Rolls back the transaction.
    pub fn rollback(&mut self) -> Result<(), reifydb_core::Error> {
        if self.discarded {
            return Err(reifydb_core::Error(reifydb_core::error::diagnostic::transaction::transaction_discarded()));
        }

        self.pending_writes.rollback();
        self.conflicts.rollback();
        Ok(())
    }

    /// Returns `true` if the pending writes contains the key.
    pub fn contains_key(&mut self, key: &EncodedKey) -> Result<Option<bool>, reifydb_core::Error> {
        if self.discarded {
            return Err(reifydb_core::Error(reifydb_core::error::diagnostic::transaction::transaction_discarded()));
        }

        match self.pending_writes.get(key) {
            Some(pending) => {
                if pending.was_removed() {
                    return Ok(Some(false));
                }
                // Fulfill from buffer.
                Ok(Some(true))
            }
            None => {
                // track reads. No need to track read if txn serviced it
                // internally.
                self.conflicts.mark_read(key);
                Ok(None)
            }
        }
    }

    /// Looks for the key in the pending writes, if such key is not in the pending writes,
    /// the end user can read the key from the database.
    pub fn get<'a, 'b: 'a>(
        &'a mut self,
        key: &'b EncodedKey,
    ) -> Result<Option<Pending>, reifydb_core::Error> {
        if self.discarded {
            return Err(reifydb_core::Error(reifydb_core::error::diagnostic::transaction::transaction_discarded()));
        }

        if let Some(v) = self.pending_writes.get(key) {
            // If the value is None, it means that the key is removed.
            if v.was_removed() {
                return Ok(None);
            }

            // Fulfill from buffer.
            Ok(Some(Pending {
                delta: match v.row() {
                    Some(row) => Delta::Set { key: key.clone(), row: row.clone() },
                    None => Delta::Remove { key: key.clone() },
                },
                version: v.version,
            }))
        } else {
            // track reads. No need to track read if txn serviced it
            // internally.
            self.conflicts.mark_read(key);
            Ok(None)
        }
    }
}

impl<C, L, P> TransactionManagerTx<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    /// Commits the transaction, following these steps:
    ///
    /// 1. If there are no writes, return immediately.
    ///
    /// 2. Check if read rows were updated since txn started. If so, return `transaction_conflict()`.
    ///
    /// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
    ///
    /// 4. Batch up all writes, write them to database.
    ///
    /// 5. If callback is provided, Badger will return immediately after checking
    ///    for conflicts. Writes to the database will happen in the background.  If
    ///    there is a conflict, an error will be returned and the callback will not
    ///    run. If there are no conflicts, the callback will be called in the
    ///    background upon successful completion of writes or any error during write.
    pub fn commit<F>(&mut self, apply: F) -> Result<(), reifydb_core::Error>
    where
        F: FnOnce(Vec<Pending>) -> Result<(), Box<dyn std::error::Error>>,
    {
        if self.discarded {
            return Err(reifydb_core::Error(reifydb_core::error::diagnostic::transaction::transaction_discarded()));
        }

        if self.pending_writes.is_empty() {
            // Nothing to commit
            self.discard();
            return Ok(());
        }

        let (commit_ts, entries) = self.commit_pending().map_err(|e| {
            // Check if this is a conflict error by examining the error code
            if e.0.code == "TXN_001" {
                e // Don't discard on conflict, let caller handle retry
            } else {
                self.discard();
                e
            }
        })?;

        apply(entries)
            .map(|_| {
                self.oracle().done_commit(commit_ts);
                self.discard();
            })
            .map_err(|e| {
                self.oracle().done_commit(commit_ts);
                self.discard();
                reifydb_core::Error(reifydb_core::error::diagnostic::transaction::commit_failed(e.to_string()))
            })
    }
}

impl<C, L, P> TransactionManagerTx<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    fn set_internal(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<(), reifydb_core::Error> {
        if self.discarded {
            return Err(reifydb_core::Error(reifydb_core::error::diagnostic::transaction::transaction_discarded()));
        }

        self.modify(Pending { delta: Delta::Set { key: key.clone(), row }, version: self.version })
    }

    fn modify(&mut self, pending: Pending) -> Result<(), reifydb_core::Error> {
        if self.discarded {
            return Err(reifydb_core::Error(reifydb_core::error::diagnostic::transaction::transaction_discarded()));
        }

        let pending_writes = &mut self.pending_writes;

        let cnt = self.count + 1;
        // Extra row for the version in key.
        let size = self.size + pending_writes.estimate_size(&pending);
        if cnt >= pending_writes.max_batch_entries() || size >= pending_writes.max_batch_size() {
            return Err(reifydb_core::Error(reifydb_core::error::diagnostic::transaction::transaction_too_large()));
        }

        self.count = cnt;
        self.size = size;

        self.conflicts.mark_conflict(pending.key());

        // If a duplicate entry was inserted in managed mode, move it to the duplicate writes slice.
        // Add the entry to duplicateWrites only if both the entries have different versions. For
        // same versions, we will overwrite the existing entry.
        let key = pending.key();
        let row = pending.row();
        let version = pending.version;

        if let Some((old_key, old_value)) = pending_writes.remove_entry(key) {
            if old_value.version != version {
                self.duplicates.push(Pending {
                    delta: match row {
                        Some(row) => Delta::Set { key: old_key, row: row.clone() },
                        None => Delta::Remove { key: old_key },
                    },
                    version,
                })
            }
        }
        pending_writes.insert(key.clone(), pending);

        Ok(())
    }
}

impl<C, L, P> TransactionManagerTx<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    fn commit_pending(&mut self) -> Result<(Version, Vec<Pending>), reifydb_core::Error> {
        if self.discarded {
            return Err(reifydb_core::Error(reifydb_core::error::diagnostic::transaction::transaction_discarded()));
        }

        // Ensure that the order in which we get the commit timestamp is the same as
        // the order in which we push these updates to the write channel. So, we
        // acquire a writeChLock before getting a commit timestamp, and only release
        // it after pushing the entries to it.
        let _write_lock = self.oracle.write_serialize_lock.lock();

        let conflict_manager = mem::take(&mut self.conflicts);

        match self.oracle.new_commit(&mut self.done_read, self.version, conflict_manager) {
            CreateCommitResult::Conflict(conflicts) => {
                // If there is a conflict, we should not send the updates to the write channel.
                // Instead, we should return the conflict error to the user.
                self.conflicts = conflicts;
                Err(reifydb_core::Error(reifydb_core::error::diagnostic::transaction::transaction_conflict()))
            }
            CreateCommitResult::Success(version) => {
                let pending_writes = mem::take(&mut self.pending_writes);
                let duplicate_writes = mem::take(&mut self.duplicates);
                let mut all = Vec::with_capacity(pending_writes.len() + self.duplicates.len());

                let process = |entries: &mut Vec<Pending>, mut pending: Pending| {
                    pending.version = version;
                    entries.push(pending);
                };

                pending_writes.into_iter().for_each(|(k, v)| {
                    process(
                        &mut all,
                        Pending {
                            delta: match v.row() {
                                Some(row) => Delta::Set { key: k, row: row.clone() },
                                None => Delta::Remove { key: k },
                            },
                            version: v.version,
                        },
                    )
                });

                duplicate_writes.into_iter().for_each(|item| process(&mut all, item));

                // version should not be zero if we're inserting transaction markers.
                debug_assert_ne!(version, 0);

                Ok((version, all))
            }
        }
    }
}

impl<C, L, P> TransactionManagerTx<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    fn done_read(&mut self) {
        if !self.done_read {
            self.done_read = true;
            self.oracle().rx.done(self.version);
        }
    }

    fn oracle(&self) -> &Oracle<C, L> {
        &self.oracle
    }

    /// Discards a created transaction. This method is very important and must be called. `commit*`
    /// methods calls this internally, however, calling this multiple times doesn't cause any issues. So,
    /// this can safely be called via a defer right when transaction is created.
    pub fn discard(&mut self) {
        if self.discarded {
            return;
        }
        self.discarded = true;
        self.done_read();
    }

    /// Returns true if the transaction is discarded.

    pub fn is_discard(&self) -> bool {
        self.discarded
    }
}
