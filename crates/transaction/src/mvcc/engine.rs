// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::key::{Key, KeyPrefix};
use crate::mvcc::transaction::init;
use crate::mvcc::{Status, Transaction, Version};
use base::encoding::{Key as _, Value};
use std::sync::{Arc, Mutex, OnceLock};
use storage::StorageEngine;

/// An MVCC-based transactional key-value engine. It wraps an underlying storage
/// engine that's used for raw key-value storage.
///
/// While it supports any number of concurrent transactions, individual read or
/// write operations are executed sequentially, serialized via a mutex. There
/// are two reasons for this: the storage engine itself is not thread-safe,
/// requiring serialized access.
pub struct Engine<S: StorageEngine> {
    // FIXME add concurrent safe MemPool module between Storage and transaction
    // idea - batch data and perform bulk insertions / update to underlying storage implementation
    // introduce ConfirmationLevel similar to Solana
    // Processed - a transaction was processed successful and the change is in the mempool
    // Confirmed - data written to file and synced
    // Finalized - majority of nodes accepted this data
    pub storage: Arc<Mutex<S>>,
}

impl<'a, S: StorageEngine + 'a> crate::TransactionEngine<'a, S> for Engine<S> {
    type Rx = Transaction<S>;
    type Tx = Transaction<S>;

    fn begin_read_only(&'a self) -> crate::Result<Self::Rx> {
        // let guard = self.inner.read().unwrap();
        // Ok(Transaction::new(guard))
        // unimplemented!()
        Ok(Transaction::begin_read_only(self.storage.clone(), None).unwrap())
    }

    fn begin(&'a self) -> crate::Result<Self::Tx> {
        // let guard = self.inner.write().unwrap();
        // Ok(TransactionMut::new(guard))
        Ok(Transaction::begin(self.storage.clone()).unwrap())
    }
}

static CATALOG: OnceLock<()> = OnceLock::new();

impl<S: StorageEngine> Engine<S> {
    /// Creates a new MVCC engine with the given storage engine.
    pub fn new(engine: S) -> Self {
        CATALOG.get_or_init(|| {
            init();
        });
        Self { storage: Arc::new(Mutex::new(engine)) }
    }

    /// Begins a new read-write transaction.
    pub fn begin(&self) -> crate::mvcc::Result<Transaction<S>> {
        Transaction::begin(self.storage.clone())
    }

    /// Begins a new read-only transaction at the latest version.
    pub fn begin_read_only(&self) -> crate::mvcc::Result<Transaction<S>> {
        Transaction::begin_read_only(self.storage.clone(), None)
    }

    /// Begins a new read-only transaction as of the given version.
    pub fn begin_read_only_as_of(&self, version: Version) -> crate::mvcc::Result<Transaction<S>> {
        Transaction::begin_read_only(self.storage.clone(), Some(version))
    }

    /// Fetches the value of an unversioned key.
    pub fn get_unversioned(&self, key: &[u8]) -> crate::mvcc::Result<Option<Vec<u8>>> {
        // self.engine.lock()?.get(&Key::Unversioned(key.into()).encode())
        // FIXME
        Ok(self.storage.lock().unwrap().get(&Key::Unversioned(key.into()).encode()).unwrap())
    }

    /// Sets the value of an unversioned key.
    pub fn set_unversioned(&self, key: &[u8], value: Vec<u8>) -> crate::mvcc::Result<()> {
        // self.engine.lock()?.set(&Key::Unversioned(key.into()).encode(), value)
        // FIXME
        Ok(self.storage.lock().unwrap().set(&Key::Unversioned(key.into()).encode(), value).unwrap())
    }

    /// Returns the status of the MVCC and storage engines.
    pub fn status(&self) -> crate::mvcc::Result<Status> {
        // FIXME
        // let mut engine = self.engine.lock()?;
        let mut engine = self.storage.lock().unwrap();
        let versions = match engine.get(&Key::NextVersion.encode())? {
            Some(ref v) => Version::decode(v)? - 1,
            None => Version(0),
        };
        let active_txs = engine.scan_prefix(&KeyPrefix::TxActive.encode()).count() as u64;
        Ok(Status { version: versions, active_txs })
        // Ok(Status { versions, active_txs, storage: engine.status()? })
    }
}
