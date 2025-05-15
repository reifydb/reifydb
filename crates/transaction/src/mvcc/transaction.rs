// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes portions of code from https://github.com/erikgrinaker/toydb (Apache 2 License).
// Original Apache 2 License Copyright (c) erikgrinaker 2024.

use crate::mvcc::{Transaction, TransactionState, Version};
use crate::{errdata, errinput};

use std::collections::BTreeSet;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::mvcc::key::{Key, KeyPrefix};
use crate::mvcc::scan::ScanIterator;
use base::encoding::{Key as _, Value, bincode, keycode};
use serde::{Deserialize, Serialize};
use storage::EngineMut;
// FIXME remove this

impl<S: EngineMut> Transaction<S> {
    /// Begins a new transaction in read-write mode. This will allocate a new
    /// version that the transaction can write at, add it to the active set, and
    /// record its active snapshot for time-travel queries.
    pub(crate) fn begin(engine: Arc<Mutex<S>>) -> crate::mvcc::Result<Self> {
        // FIXME
        // let mut session = engine.lock()?;
        let mut session = engine.lock().unwrap();

        // Allocate a new version to write at.
        let version = match session.get(&Key::NextVersion.encode())? {
            Some(ref v) => Version::decode(v)?,
            None => Version(1),
        };
        session.set(&Key::NextVersion.encode(), (version + 1).encode())?;

        // Fetch the current set of active transactions, persist it for
        // time-travel queries if non-empty, then add this txn to it.
        let active = Self::scan_active(&mut session)?;
        if !active.is_empty() {
            session.set(&Key::TxActiveSnapshot(version).encode(), active.encode())?
        }
        session.set(&Key::TxActive(version).encode(), vec![])?;
        drop(session);

        Ok(Self { engine, state: TransactionState { version, read_only: false, active } })
    }

    /// Begins a new read-only transaction. If version is given it will see the
    /// state as of the beginning of that version (ignoring writes at that
    /// version). In other words, it sees the same state as the read-write
    /// transaction at that version saw when it began.
    pub(crate) fn begin_read_only(
        engine: Arc<Mutex<S>>,
        as_of: Option<Version>,
    ) -> crate::mvcc::Result<Self> {
        // FIXME
        // let mut session = engine.lock()?;
        let mut session = engine.lock().unwrap();

        // Fetch the latest version.
        let mut version = match session.get(&Key::NextVersion.encode())? {
            Some(ref v) => Version::decode(v)?,
            None => Version(1),
        };

        // If requested, create the transaction as of a past version, restoring
        // the active snapshot as of the beginning of that version. Otherwise,
        // use the latest version and get the current, real-time snapshot.
        let mut active = BTreeSet::new();
        if let Some(as_of) = as_of {
            if as_of >= version {
                return errinput!("version {as_of} does not exist");
            }
            version = as_of;
            if let Some(value) = session.get(&Key::TxActiveSnapshot(version).encode())? {
                active = BTreeSet::<Version>::decode(&value)?;
            }
        } else {
            active = Self::scan_active(&mut session)?;
        }

        drop(session);

        Ok(Self { engine, state: TransactionState { version, read_only: true, active } })
    }

    /// Resumes a transaction from the given state.
    // fn resume(engine: Arc<Mutex<S>>, s: TransactionState) -> Result<Self> {
    //     // For read-write transactions, verify that the transaction is still
    //     // active before making further writes.
    //     if !s.read_only && engine.lock()?.get(&Key::TxnActive(s.version).encode())?.is_none() {
    //         return errinput!("no active transaction at version {}", s.version);
    //     }
    //     Ok(Self { engine, state: s })
    // }

    /// Fetches the set of currently active transactions.
    fn scan_active(session: &mut MutexGuard<S>) -> crate::mvcc::Result<BTreeSet<Version>> {
        let mut active = BTreeSet::new();
        let mut scan = session.scan_prefix(&KeyPrefix::TxnActive.encode());
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxActive(version) => active.insert(version),
                key => return errdata!("expected TxnActive key, got {key:?}"),
            };
        }
        Ok(active)
    }

    /// Returns the version the transaction is running at.
    pub fn version(&self) -> Version {
        self.state.version
    }

    /// Returns whether the transaction is read-only.
    pub fn read_only(&self) -> bool {
        self.state.read_only
    }

    /// Returns the transaction's state. This can be used to instantiate a
    /// functionally equivalent transaction via resume().
    pub fn state(&self) -> &TransactionState {
        &self.state
    }

    /// Commits the transaction, by removing it from the active set. This will
    /// immediately make its writes visible to subsequent transactions. Also
    /// removes its TxnWrite records, which are no longer needed.
    ///
    /// NB: commit does not flush writes to durable storage, since we rely on
    /// the Raft log for persistence.
    pub fn commit(self) -> crate::mvcc::Result<()> {
        if self.state.read_only {
            return Ok(());
        }
        // FIXME
        // let mut engine = self.engine.lock()?;
        let mut engine = self.engine.lock().unwrap();

        let mut remove = Vec::new();
        for result in engine.scan_prefix(&KeyPrefix::TxnWrite(self.state.version).encode()) {
            let (k, _) = result?;
            remove.push(k);
        }
        for key in remove {
            engine.remove(&key)?;
        }

        // FIXME
        // engine.remove(&Key::TxnActive(self.state.version).encode())
        engine.remove(&Key::TxActive(self.state.version).encode()).unwrap();
        Ok(())
    }

    /// Rolls back the transaction, by undoing all written versions and removing
    /// it from the active set. The active set snapshot is left behind, since
    /// this is needed for time travel queries at this version.
    pub fn rollback(self) -> crate::mvcc::Result<()> {
        if self.state.read_only {
            return Ok(());
        }
        // FIXME
        // let mut engine = self.engine.lock()?;
        let mut engine = self.engine.lock().unwrap();
        let mut rollback = Vec::new();
        let mut scan = engine.scan_prefix(&KeyPrefix::TxnWrite(self.state.version).encode());
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxWrite(_, key) => {
                    rollback.push(Key::Version(key, self.state.version).encode())
                    // the version
                }
                key => return errdata!("expected TxnWrite, got {key:?}"),
            };
            rollback.push(key); // the TxnWrite record
        }
        drop(scan);
        for key in rollback.into_iter() {
            engine.remove(&key)?;
        }
        // FIXME
        // engine.remove(&Key::TxnActive(self.state.version).encode()) // remove from active set
        engine.remove(&Key::TxActive(self.state.version).encode()).unwrap();
        Ok(())
    }

    /// Deletes a key.
    pub fn delete(&self, key: &[u8]) -> crate::mvcc::Result<()> {
        self.write_version(key, None)
    }

    /// Sets a value for a key.
    pub fn set(&self, key: &[u8], value: Vec<u8>) -> crate::mvcc::Result<()> {
        self.write_version(key, Some(value))
    }

    /// Writes a new version for a key at the transaction's version. None writes
    /// a deletion tombstone. If a write conflict is found (either a newer or
    /// uncommitted version), a serialization error is returned.  Replacing our
    /// own uncommitted write is fine.
    fn write_version(&self, key: &[u8], value: Option<Vec<u8>>) -> crate::mvcc::Result<()> {
        if self.state.read_only {
            // FIXME
            todo!()
            // return Err(Error::ReadOnly);
        }
        // FIXME
        // let mut engine = self.engine.lock()?;
        let mut engine = self.engine.lock().unwrap();

        // Check for write conflicts, i.e. if the latest key is invisible to us
        // (either a newer version, or an uncommitted version in our past). We
        // can only conflict with the latest key, since all transactions enforce
        // the same invariant.
        let from = Key::Version(
            key.into(),
            self.state.active.iter().min().copied().unwrap_or(self.state.version + 1),
        )
        .encode();
        let to = Key::Version(key.into(), Version(u64::MAX)).encode();
        if let Some((key, _)) = engine.scan(from..=to).last().transpose()? {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    if !self.state.is_visible(version) {
                        // FIXME
                        // return Err(Error::Serialization);
                        todo!()
                    }
                }
                key => return errdata!("expected Key::Version got {key:?}"),
            }
        }

        // Write the new version and its write record.
        //
        // NB: TxnWrite contains the provided user key, not the encoded engine
        // key, since we can construct the engine key using the version.
        engine.set(&Key::TxWrite(self.state.version, key.into()).encode(), vec![])?;
        //FIXME
        // engine.set(&Key::Version(key.into(), self.state.version).encode(), bincode::serialize(&value))
        engine
            .set(&Key::Version(key.into(), self.state.version).encode(), bincode::serialize(&value))
            .unwrap();
        Ok(())
    }

    /// Fetches a key's value, or None if it does not exist.
    pub fn get(&self, key: &[u8]) -> crate::mvcc::Result<Option<Vec<u8>>> {
        // FIXME
        // let mut engine = self.engine.lock()?;
        let mut engine = self.engine.lock().unwrap();

        let from = Key::Version(key.into(), Version(0)).encode();
        let to = Key::Version(key.into(), self.state.version).encode();
        let mut scan = engine.scan(from..=to).rev();
        while let Some((key, value)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    if self.state.is_visible(version) {
                        // FIXME
                        // return bincode::deserialize(&value);
                        return Ok(bincode::deserialize(&value).unwrap());
                    }
                }
                key => return errdata!("expected Key::Version got {key:?}"),
            };
        }
        Ok(None)
    }

    /// Returns an iterator over the latest visible key/value pairs at the
    /// transaction's version.
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> ScanIterator<S> {
        let start = match range.start_bound() {
            Bound::Excluded(k) => {
                Bound::Excluded(Key::Version(k.into(), Version(u64::MAX)).encode())
            }
            Bound::Included(k) => Bound::Included(Key::Version(k.into(), Version(0)).encode()),
            Bound::Unbounded => Bound::Included(Key::Version(vec![].into(), Version(0)).encode()),
        };
        let end = match range.end_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Version(k.into(), Version(0)).encode()),
            Bound::Included(k) => {
                Bound::Included(Key::Version(k.into(), Version(u64::MAX)).encode())
            }
            Bound::Unbounded => Bound::Excluded(KeyPrefix::Unversioned.encode()),
        };
        ScanIterator::new(self.engine.clone(), self.state().clone(), (start, end))
    }

    /// Scans keys under a given prefix.
    pub fn scan_prefix(&self, prefix: &[u8]) -> ScanIterator<S> {
        // Normally, KeyPrefix::Version will only match all versions of the
        // exact given key. We want all keys maching the prefix, so we chop off
        // the Keycode byte slice terminator 0x0000 at the end.
        let mut prefix = KeyPrefix::Version(prefix.into()).encode();
        prefix.truncate(prefix.len() - 2);
        let range = keycode::prefix_range(&prefix);
        ScanIterator::new(self.engine.clone(), self.state().clone(), range)
    }
}
