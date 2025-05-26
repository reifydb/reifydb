// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::transaction::mvcc::key::{Key, KeyPrefix};
use crate::transaction::mvcc::transaction::init;
use crate::transaction::mvcc::{Status, Transaction, Version};
use base::encoding::{Key as _, Value};
use persistence::Persistence;
use std::sync::{Arc, Mutex, OnceLock};

/// An MVCC-based transactional key-value engine. It wraps an underlying store
/// engine that's used for raw key-value store.
///
/// While it supports any number of concurrent transactions, individual read or
/// write operations are executed sequentially, serialized via a mutex. There
/// are two reasons for this: the store engine itself is not thread-safe,
/// requiring serialized access.
pub struct Mvcc<P: Persistence> {
    // FIXME add concurrent safe MemPool module between Store and transaction
    // idea - batch data and perform bulk insertions / update to underlying store implementation
    // introduce ConfirmationLevel similar to Solana
    // Processed - a transaction was processed successful and the change is in the mempool
    // Confirmed - data written to file and synced
    // Finalized - majority of nodes accepted this data
    pub store: Arc<Mutex<P>>,
}

impl<P: Persistence> crate::Transaction<P> for Mvcc<P> {
    type Rx = Transaction<P>;
    type Tx = Transaction<P>;

    fn begin_read_only(&self) -> crate::Result<Self::Rx> {
        // let guard = self.inner.read().unwrap();
        // Ok(Transaction::new(guard))
        // unimplemented!()
        Ok(Transaction::begin_read_only(self.store.clone(), None).unwrap())
    }

    fn begin(&self) -> crate::Result<Self::Tx> {
        // let guard = self.inner.write().unwrap();
        // Ok(TransactionMut::new(guard))
        Ok(Transaction::begin(self.store.clone()).unwrap())
    }
}

static CATALOG: OnceLock<()> = OnceLock::new();

impl<P: Persistence> Mvcc<P> {
    /// Creates a new MVCC engine with the given store engine.
    pub fn new(engine: P) -> Self {
        CATALOG.get_or_init(|| {
            init();
        });
        Self { store: Arc::new(Mutex::new(engine)) }
    }

    /// Begins a new read-write transaction.
    pub fn begin(&self) -> crate::transaction::mvcc::Result<Transaction<P>> {
        Transaction::begin(self.store.clone())
    }

    /// Begins a new read-only transaction at the latest version.
    pub fn begin_read_only(&self) -> crate::transaction::mvcc::Result<Transaction<P>> {
        Transaction::begin_read_only(self.store.clone(), None)
    }

    /// Begins a new read-only transaction as of the given version.
    pub fn begin_read_only_as_of(
        &self,
        version: Version,
    ) -> crate::transaction::mvcc::Result<Transaction<P>> {
        Transaction::begin_read_only(self.store.clone(), Some(version))
    }

    /// Fetches the value of an unversioned key.
    pub fn get_unversioned(&self, key: &[u8]) -> crate::transaction::mvcc::Result<Option<Vec<u8>>> {
        // self.engine.lock()?.get(&Key::Unversioned(key.into()).encode())
        // FIXME
        Ok(self.store.lock().unwrap().get(&Key::Unversioned(key.into()).encode()).unwrap())
    }

    /// Sets the value of an unversioned key.
    pub fn set_unversioned(
        &self,
        key: &[u8],
        value: Vec<u8>,
    ) -> crate::transaction::mvcc::Result<()> {
        // self.engine.lock()?.set(&Key::Unversioned(key.into()).encode(), value)
        // FIXME
        Ok(self.store.lock().unwrap().set(&Key::Unversioned(key.into()).encode(), value).unwrap())
    }

    /// Returns the status of the MVCC and store engines.
    pub fn status(&self) -> crate::transaction::mvcc::Result<Status> {
        // FIXME
        // let mut engine = self.engine.lock()?;
        let mut engine = self.store.lock().unwrap();
        let versions = match engine.get(&Key::NextVersion.encode())? {
            Some(ref v) => Version::decode(v)? - 1,
            None => Version(0),
        };
        let active_txs = engine.scan_prefix(&KeyPrefix::TxActive.encode()).count() as u64;
        Ok(Status { version: versions, active_txs })
        // Ok(Status { versions, active_txs, store: engine.status()? })
    }
}
