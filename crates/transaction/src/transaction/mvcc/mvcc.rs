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
use crate::transaction::mvcc::{Status, Transaction, Version};
use reifydb_core::encoding::{Key as _, Value};
use reifydb_persistence::Persistence;
use std::sync::{Arc, Mutex};

/// An MVCC-based transactional key-value reifydb_engine. It wraps an underlying persistence that's used for raw key-value storage.
///
/// While it supports any number of concurrent transactions, individual read or
/// write operations are executed sequentially, serialized via a mutex
pub struct Mvcc<P: Persistence> {
    pub persistence: Arc<Mutex<P>>,
}

impl<P: Persistence> crate::Transaction<P> for Mvcc<P> {
    type Rx = Transaction<P>;
    type Tx = Transaction<P>;

    fn begin_read_only(&self) -> crate::Result<Self::Rx> {
        Ok(Transaction::begin_read_only(self.persistence.clone(), None)?)
    }

    fn begin(&self) -> crate::Result<Self::Tx> {
        Ok(Transaction::begin(self.persistence.clone())?)
    }
}

impl<P: Persistence> Mvcc<P> {
    /// Creates a new MVCC reifydb_engine with the given store reifydb_engine.
    pub fn new(persistence: P) -> Self {
        Self { persistence: Arc::new(Mutex::new(persistence)) }
    }

    /// Begins a new read-write transaction.
    pub fn begin(&self) -> crate::transaction::mvcc::Result<Transaction<P>> {
        Transaction::begin(self.persistence.clone())
    }

    /// Begins a new read-only transaction at the latest version.
    pub fn begin_read_only(&self) -> crate::transaction::mvcc::Result<Transaction<P>> {
        Transaction::begin_read_only(self.persistence.clone(), None)
    }

    /// Begins a new read-only transaction as of the given version.
    pub fn begin_read_only_as_of(
        &self,
        version: Version,
    ) -> crate::transaction::mvcc::Result<Transaction<P>> {
        Transaction::begin_read_only(self.persistence.clone(), Some(version))
    }

    /// Fetches the value of an unversioned key.
    pub fn get_unversioned(&self, key: &[u8]) -> crate::transaction::mvcc::Result<Option<Vec<u8>>> {
        // self.reifydb_engine.lock()?.get(&Key::Unversioned(key.into()).encode())
        // FIXME
        Ok(self.persistence.lock().unwrap().get(&Key::Unversioned(key.into()).encode()).unwrap())
    }

    /// Sets the value of an unversioned key.
    pub fn set_unversioned(
        &self,
        key: &[u8],
        value: Vec<u8>,
    ) -> crate::transaction::mvcc::Result<()> {
        // self.reifydb_engine.lock()?.set(&Key::Unversioned(key.into()).encode(), value)

        // FIXME
        Ok(self
            .persistence
            .lock()
            .unwrap()
            .set(&Key::Unversioned(key.into()).encode(), value)
            .unwrap())
    }

    /// Returns the status of the MVCC and store reifydb_engines.
    pub fn status(&self) -> crate::transaction::mvcc::Result<Status> {
        // FIXME
        // let mut reifydb_engine = self.reifydb_engine.lock()?;
        let mut reifydb_engine = self.persistence.lock().unwrap();
        let versions = match reifydb_engine.get(&Key::NextVersion.encode())? {
            Some(ref v) => Version::decode(v)? - 1,
            None => Version(0),
        };
        let active_txs = reifydb_engine.scan_prefix(&KeyPrefix::TxActive.encode()).count() as u64;
        Ok(Status { version: versions, active_txs })
        // Ok(Status { versions, active_txs, store: reifydb_engine.status()? })
    }
}
