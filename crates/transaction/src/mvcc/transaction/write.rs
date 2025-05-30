// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use crate::Version;
use crate::mvcc::error::MvccError;
use crate::mvcc::marker::Marker;
use crate::mvcc::types::Pending;
use reifydb_persistence::{Action, Key, Value};

pub struct TransactionManagerTx<C, P> {
    pub(super) version: Version,
    pub(super) size: u64,
    pub(super) count: u64,
    pub(super) oracle: Arc<Oracle<C>>,
    pub(super) conflicts: C,
    // stores any writes done by tx
    pub(super) pending_writes: P,
    pub(super) duplicates: Vec<Pending>,

    pub(super) discarded: bool,
    pub(super) done_read: bool,
}

impl<C, P> Drop for TransactionManagerTx<C, P> {
    fn drop(&mut self) {
        if !self.discarded {
            self.discard();
        }
    }
}

impl<C, P> TransactionManagerTx<C, P> {
    /// Returns the version of the transaction.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Sets the current version of the transaction manager.
    /// This should be used only for testing purposes.
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

impl<C, P> TransactionManagerTx<C, P>
where
    C: Conflict,
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
    pub fn mark_read(&mut self, k: &Key) {
        self.conflicts.mark_read(k);
    }

    /// Marks a key is conflict.
    pub fn mark_conflict(&mut self, k: &Key) {
        self.conflicts.mark_conflict(k);
    }
}

impl<C, P> TransactionManagerTx<C, P>
where
    C: Conflict,
    P: PendingWrites,
{
    /// Set a key-value pair to the transaction.
    pub fn set(&mut self, key: Key, value: Value) -> Result<(), TransactionError> {
        if self.discarded {
            return Err(TransactionError::Discarded);
        }

        self.insert_with_in(key, value)
    }

    /// Removes a key.
    ///
    /// This is done by adding a delete marker for the key at commit timestamp.  Any
    /// reads happening before this timestamp would be unaffected. Any reads after
    /// this commit would see the deletion.
    pub fn remove(&mut self, key: Key) -> Result<(), TransactionError> {
        if self.discarded {
            return Err(TransactionError::Discarded);
        }
        self.modify(Pending { action: Action::Remove { key }, version: 0 })
    }

    /// Rolls back the transaction.
    pub fn rollback(&mut self) -> Result<(), TransactionError> {
        if self.discarded {
            return Err(TransactionError::Discarded);
        }

        self.pending_writes.rollback();
        self.conflicts.rollback();
        Ok(())
    }

    /// Returns `true` if the pending writes contains the key.
    pub fn contains_key(&mut self, key: &Key) -> Result<Option<bool>, TransactionError> {
        if self.discarded {
            return Err(TransactionError::Discarded);
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
        key: &'b Key,
    ) -> Result<Option<Pending>, TransactionError> {
        if self.discarded {
            return Err(TransactionError::Discarded);
        }

        if let Some(v) = self.pending_writes.get(key) {
            // If the value is None, it means that the key is removed.
            if v.was_removed() {
                return Ok(None);
            }

            // Fulfill from buffer.
            Ok(Some(Pending {
                action: match v.value() {
                    Some(value) => Action::Set { key: key.clone(), value: value.clone() },
                    None => Action::Remove { key: key.clone() },
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

impl<C, P> TransactionManagerTx<C, P>
where
    C: Conflict,
    P: PendingWrites,
{
    /// Commits the transaction, following these steps:
    ///
    /// 1. If there are no writes, return immediately.
    ///
    /// 2. Check if read rows were updated since txn started. If so, return `TransactionError::Conflict`.
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
    pub fn commit<F>(&mut self, apply: F) -> Result<(), MvccError>
    where
        F: FnOnce(Vec<Pending>) -> Result<(), Box<dyn std::error::Error>>,
    {
        if self.discarded {
            return Err(TransactionError::Discarded.into());
        }

        if self.pending_writes.is_empty() {
            // Nothing to commit
            self.discard();
            return Ok(());
        }

        let (commit_ts, entries) = self.commit_entries().map_err(|e| match e {
            TransactionError::Conflict => e,
            _ => {
                self.discard();
                e
            }
        })?;

        apply(entries)
            .map(|_| {
                self.orc().done_commit(commit_ts);
                self.discard();
            })
            .map_err(|e| {
                self.orc().done_commit(commit_ts);
                self.discard();
                MvccError::commit(e)
            })
    }
}

impl<C, P> TransactionManagerTx<C, P>
where
    C: Conflict,
    P: PendingWrites,
{
    fn insert_with_in(&mut self, key: Key, value: Value) -> Result<(), TransactionError> {
        if self.discarded {
            return Err(TransactionError::Discarded);
        }

        self.modify(Pending { action: Action::Set { key, value }, version: self.version })
    }

    fn modify(&mut self, pending: Pending) -> Result<(), TransactionError> {
        if self.discarded {
            return Err(TransactionError::Discarded);
        }

        let pending_writes = &mut self.pending_writes;

        let cnt = self.count + 1;
        // Extra bytes for the version in key.
        let size = self.size + pending_writes.estimate_size(&pending);
        if cnt >= pending_writes.max_batch_entries() || size >= pending_writes.max_batch_size() {
            return Err(TransactionError::LargeTxn);
        }

        self.count = cnt;
        self.size = size;

        self.conflicts.mark_conflict(pending.key());

        // If a duplicate entry was inserted in managed mode, move it to the duplicate writes slice.
        // Add the entry to duplicateWrites only if both the entries have different versions. For
        // same versions, we will overwrite the existing entry.
        let key = pending.key();
        let value = pending.value();
        let version = pending.version;

        if let Some((old_key, old_value)) = pending_writes.remove_entry(&key) {
            if old_value.version != version {
                self.duplicates.push(Pending {
                    action: match value {
                        Some(value) => Action::Set { key: old_key, value: value.clone() },
                        None => Action::Remove { key: old_key },
                    },
                    version,
                })
            }
        }
        pending_writes.insert(key.clone(), pending);

        Ok(())
    }
}

impl<C, P> TransactionManagerTx<C, P>
where
    C: Conflict,
    P: PendingWrites,
{
    fn commit_entries(&mut self) -> Result<(u64, Vec<Pending>), TransactionError> {
        if self.discarded {
            return Err(TransactionError::Discarded);
        }

        // Ensure that the order in which we get the commit timestamp is the same as
        // the order in which we push these updates to the write channel. So, we
        // acquire a writeChLock before getting a commit timestamp, and only release
        // it after pushing the entries to it.
        let _write_lock = self.oracle.write_serialize_lock.lock();

        let conflict_manager = mem::take(&mut self.conflicts);

        match self.oracle.new_commit_ts(&mut self.done_read, self.version, conflict_manager) {
            CreateCommitTimestampResult::Conflict(conflicts) => {
                // If there is a conflict, we should not send the updates to the write channel.
                // Instead, we should return the conflict error to the user.
                self.conflicts = conflicts;
                Err(TransactionError::Conflict)
            }
            CreateCommitTimestampResult::Timestamp(commit_ts) => {
                let pending_writes = mem::take(&mut self.pending_writes);
                let duplicate_writes = mem::take(&mut self.duplicates);
                let mut all = Vec::with_capacity(pending_writes.len() + self.duplicates.len());

                let process = |entries: &mut Vec<Pending>, mut pending: Pending| {
                    pending.version = commit_ts;
                    entries.push(pending);
                };

                pending_writes.into_iter().for_each(|(k, v)| {
                    process(
                        &mut all,
                        Pending {
                            action: match v.value() {
                                Some(value) => Action::Set { key: k, value: value.clone() },
                                None => Action::Remove { key: k },
                            },
                            version: v.version,
                        },
                    )
                });

                duplicate_writes.into_iter().for_each(|item| process(&mut all, item));

                // CommitTs should not be zero if we're inserting transaction markers.
                debug_assert_ne!(commit_ts, 0);

                Ok((commit_ts, all))
            }
        }
    }
}

impl<C, P> TransactionManagerTx<C, P> {
    fn done_read(&mut self) {
        if !self.done_read {
            self.done_read = true;
            self.orc().rx.done(self.version);
        }
    }

    fn orc(&self) -> &Oracle<C> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    #[ignore]
    fn test_transaction_manager_with_btree_pending_writes() {
        // let tm = TransactionManager::<
        //     Arc<u64>,
        //     u64,
        //     TestConflict<Arc<u64>>,
        //     BTreePendingWrites<Arc<u64>, u64>,
        // >::new("test", 0);
        // let mut wtm = tm.write().unwrap();
        // assert!(!wtm.is_discard());
        //
        // let mut marker = wtm.marker();
        //
        // let one = Arc::new(1);
        // let two = Arc::new(2);
        // let three = Arc::new(3);
        // let four = Arc::new(4);
        // let five = Arc::new(5);
        // marker.mark(&one);
        // marker.mark_conflict(&two);
        // wtm.mark_read(&two);
        // wtm.mark_conflict(&one);
        //
        // wtm.set(five.clone(), 5).unwrap();
    }

    struct TestConflict {
        conflict_keys: BTreeSet<Key>,
        reads: BTreeSet<Key>,
    }

    impl Default for TestConflict {
        fn default() -> Self {
            TestConflict::new()
        }
    }

    impl Conflict for TestConflict {
        fn new() -> Self {
            Self { conflict_keys: BTreeSet::new(), reads: BTreeSet::new() }
        }

        fn mark_read(&mut self, key: &Key) {
            self.reads.insert(key.clone());
        }

        fn mark_conflict(&mut self, key: &Key) {
            self.conflict_keys.insert(key.clone());
        }

        fn has_conflict(&self, other: &Self) -> bool {
            if self.reads.is_empty() {
                return false;
            }

            for ro in self.reads.iter() {
                if other.conflict_keys.contains(ro) {
                    return true;
                }
            }
            false
        }

        fn rollback(&mut self) {
            self.conflict_keys.clear();
            self.reads.clear();
        }
    }
}
