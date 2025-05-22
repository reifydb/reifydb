// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::{Error, Transaction, TransactionState, Version};
use crate::{CatalogRx as _, CatalogTx, InsertResult};
use std::cell::UnsafeCell;

use std::collections::BTreeSet;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use crate::mvcc::catalog::Catalog;
use crate::mvcc::key::{Key, KeyPrefix};
use crate::mvcc::scan::ScanIterator;
use crate::mvcc::schema::Schema;
use base::encoding::{Key as _, Value, bincode, keycode};
use base::{Row, RowIter, key_prefix};
use storage::StorageEngine;
// FIXME remove this

impl<S: StorageEngine> crate::Rx for Transaction<S> {
    type Catalog = Catalog;
    type Schema = Schema;

    fn catalog(&self) -> crate::Result<&'static Self::Catalog> {
        // Ok(*CATALOG.get().expect("Catalog not initialized"))
        // todo!()
        // SAFETY: Caller guarantees exclusive access
        unsafe { Ok(*CATALOG.get().unwrap().0.get()) }
    }

    fn schema(&self, schema: &str) -> crate::Result<&Self::Schema> {
        Ok(self.catalog().unwrap().get(schema).unwrap())
    }

    fn get(&self, store: impl AsRef<str>, ids: &[base::Key]) -> crate::Result<Vec<Row>> {
        todo!()
    }

    fn scan(&self, store: &str) -> crate::Result<RowIter> {
        Ok(Box::new(
            self.engine
                .lock()
                .unwrap()
                .scan_prefix(key_prefix!("{}::row::", store))
                .map(|r| Row::decode(&r.unwrap().1).unwrap())
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }
}

#[derive(Debug)]
pub struct CatalogCell(UnsafeCell<&'static mut Catalog>);

unsafe impl Sync for CatalogCell {} // ⚠️ only safe in single-threaded context

static CATALOG: OnceLock<CatalogCell> = OnceLock::new();

pub fn init() {
    let boxed = Box::new(Catalog::new());
    let leaked = Box::leak(boxed);
    CATALOG.set(CatalogCell(UnsafeCell::new(leaked))).unwrap();
}

pub fn catalog_mut_singleton() -> &'static mut Catalog {
    // SAFETY: Caller guarantees exclusive access
    unsafe { *CATALOG.get().unwrap().0.get() }
}

impl<S: StorageEngine> crate::Tx for Transaction<S> {
    type CatalogMut = Catalog;
    type SchemaMut = Schema;

    fn catalog_mut(&mut self) -> crate::Result<&mut Self::CatalogMut> {
        // SAFETY: Caller guarantees exclusive access
        unsafe { Ok(*CATALOG.get().unwrap().0.get()) }
    }

    fn schema_mut(&mut self, schema: &str) -> crate::Result<&mut Self::SchemaMut> {
        let schema = self.catalog_mut().unwrap().get_mut(schema).unwrap();

        Ok(schema)
    }

    fn insert(&mut self, store: impl AsRef<str>, rows: Vec<Row>) -> crate::Result<InsertResult> {
        let store = store.as_ref();

        let last_id =
            self.engine.lock().unwrap().scan_prefix(&key_prefix!("{}::row::", store)).count();

        // FIXME assumes every row gets inserted - not updated etc..
        let inserted = rows.len();

        for (id, row) in rows.iter().enumerate() {
            self.engine
                .lock()
                .unwrap()
                .set(
                    // &encode_key(format!("{}::row::{}", store, (last_id + id + 1)).as_str()),
                    key_prefix!("{}::row::{}", store, (last_id + id + 1)),
                    bincode::serialize(row),
                )
                .unwrap();
        }

        Ok(InsertResult { inserted })
    }

    fn commit(self) -> crate::Result<()> {
        if self.state.read_only {
            return Ok(());
        }
        // FIXME
        // let mut engine = self.engine.lock()?;
        let mut engine = self.engine.lock().unwrap();

        let mut remove = Vec::new();
        for result in engine.scan_prefix(&KeyPrefix::TxWrite(self.state.version).encode()) {
            let (k, _) = result?;
            remove.push(k);
        }
        for key in remove {
            engine.remove(&key)?;
        }

        // FIXME
        // engine.remove(&Key::TxActive(self.state.version).encode())
        engine.remove(&Key::TxActive(self.state.version).encode()).unwrap();
        Ok(())
    }

    fn rollback(self) -> crate::Result<()> {
        todo!()
    }
}

impl<S: StorageEngine> Transaction<S> {
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
        // time-travel queries if non-empty, then add this tx to it.
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
                return Err(Error::VersionNotFound { version: as_of });
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

    /// Fetches the set of currently active transactions.
    fn scan_active(session: &mut MutexGuard<S>) -> crate::mvcc::Result<BTreeSet<Version>> {
        let mut active = BTreeSet::new();
        let mut scan = session.scan_prefix(&KeyPrefix::TxActive.encode());
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxActive(version) => active.insert(version),
                key => return Err(Error::unexpected_key("TxActive", key)),
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
    /// removes its TxWrite records, which are no longer needed.
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
        for result in engine.scan_prefix(&KeyPrefix::TxWrite(self.state.version).encode()) {
            let (k, _) = result?;
            remove.push(k);
        }
        for key in remove {
            engine.remove(&key)?;
        }

        // FIXME
        // engine.remove(&Key::TxActive(self.state.version).encode())
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
        let mut scan = engine.scan_prefix(&KeyPrefix::TxWrite(self.state.version).encode());
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxWrite(_, key) => {
                    rollback.push(Key::Version(key, self.state.version).encode())
                    // the version
                }
                // key => return errdata!("expected TxWrite, got {key:?}"),
                key => return Err(Error::unexpected_key("TxWrite", key)),
            };
            rollback.push(key); // the TxWrite record
        }
        drop(scan);
        for key in rollback.into_iter() {
            engine.remove(&key)?;
        }
        // FIXME
        // engine.remove(&Key::TxActive(self.state.version).encode()) // remove from active set
        engine.remove(&Key::TxActive(self.state.version).encode()).unwrap();
        Ok(())
    }

    /// Removes a key.
    pub fn remove(&self, key: &[u8]) -> crate::mvcc::Result<()> {
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
            return Err(Error::ReadOnly);
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
                        return Err(Error::Serialization);
                    }
                }
                key => return Err(Error::unexpected_key("Key::Version", key)),
            }
        }

        // Write the new version and its write record.
        //
        // NB: TxWrite contains the provided user key, not the encoded engine
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
                key => return Err(Error::unexpected_key("Key::Version", key)),
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
