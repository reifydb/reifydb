// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use reifydb_auth as auth;
pub use reifydb_core as core;
pub use reifydb_core::{Error, Result};
pub use reifydb_engine as engine;
#[cfg(any(feature = "server", feature = "client"))]
pub use reifydb_network as network;
pub use reifydb_rql as rql;
pub use reifydb_storage as storage;
pub use reifydb_transaction as transaction;

use std::path::Path;

#[cfg(feature = "embedded")]
use crate::embedded::Embedded;
#[cfg(feature = "server")]
use crate::server::Server;
use reifydb_core::frame::Frame;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{Principal, Transaction, UnversionedStorage, VersionedStorage};
use reifydb_engine::Engine;
#[cfg(feature = "client")]
pub use reifydb_network::grpc::client;
/// The underlying persistence responsible for data access.
use reifydb_storage::lmdb::Lmdb;
use reifydb_storage::memory::Memory;
use reifydb_storage::sqlite::Sqlite;
use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;
use reifydb_transaction::mvcc::transaction::serializable::Serializable;

#[cfg(feature = "embedded")]
pub mod embedded;

#[cfg(feature = "embedded_blocking")]
pub mod embedded_blocking;

#[cfg(feature = "server")]
pub mod server;
mod session;

pub struct ReifyDB {}

pub trait DB<'a>: Sized {
    fn tx_as(
        &self,
        principal: &Principal,
        rql: &str,
    ) -> impl Future<Output = Result<Vec<Frame>>> + Send;

    fn tx_as_root(&self, rql: &str) -> impl Future<Output = Result<Vec<Frame>>> + Send;

    fn rx_as(
        &self,
        principal: &Principal,
        rql: &str,
    ) -> impl Future<Output = Result<Vec<Frame>>> + Send;

    fn rx_as_root(&self, rql: &str) -> impl Future<Output = Result<Vec<Frame>>> + Send;
}

impl ReifyDB {
    #[cfg(feature = "embedded")]
    pub fn embedded() -> Embedded<Memory, Memory, Serializable<Memory, Memory>> {
        let (transaction, hooks) = serializable(memory());
        Embedded::new(transaction, hooks)
    }

    #[cfg(feature = "embedded_blocking")]
    pub fn embedded_blocking()
    -> embedded_blocking::Embedded<Memory, Memory, Serializable<Memory, Memory>> {
        let (transaction, hooks) = serializable(memory());
        embedded_blocking::Embedded::new(transaction, hooks).unwrap()
    }

    #[cfg(all(feature = "embedded_blocking", not(feature = "embedded")))]
    pub fn embedded() -> (
        embedded_blocking::Embedded<
            Memory,
            Memory,
            ::reifydb_transaction::mvcc::transaction::serializable::Serializable<Memory, Memory>,
        >,
        Principal,
    ) {
        Self::embedded_blocking()
    }

    #[cfg(feature = "embedded")]
    pub fn embedded_with<VS, US, T>(transaction: T, hooks: Hooks) -> Embedded<VS, US, T>
    where
        VS: VersionedStorage,
        US: UnversionedStorage,
        T: Transaction<VS, US>,
    {
        Embedded::new(transaction, hooks)
    }

    #[cfg(all(feature = "embedded_blocking", not(feature = "embedded")))]
    pub fn embedded_with<VS, US, T>(
        transaction: T,
        hooks: Hooks,
    ) -> embedded_blocking::Embedded<VS, US, T>
    where
        VS: VersionedStorage,
        US: UnversionedStorage,
        T: Transaction<VS, US>,
    {
        embedded_blocking::Embedded::new(transaction, hooks).unwrap()
    }

    #[cfg(feature = "embedded_blocking")]
    pub fn embedded_blocking_with<VS, US, T>(
        input: (T, Hooks),
    ) -> embedded_blocking::Embedded<VS, US, T>
    where
        VS: VersionedStorage,
        US: UnversionedStorage,
        T: Transaction<VS, US>,
    {
        let (transaction, hooks) = input;
        embedded_blocking::Embedded::new(transaction, hooks).unwrap()
    }

    #[cfg(feature = "server")]
    pub fn server() -> Server<
        Memory,
        Memory,
        Serializable<Memory, Memory>,
    > {
        let (transaction, hooks) = serializable(memory());
        let engine = Engine::new(transaction, hooks).unwrap();
        Server::new(engine)
    }

    #[cfg(feature = "server")]
    pub fn server_with<VS, US, T>(input: (T, Hooks)) -> Server<VS, US, T>
    where
        VS: VersionedStorage,
        US: UnversionedStorage,
        T: Transaction<VS, US>,
    {
        let (transaction, hooks) = input;
        let engine = Engine::new(transaction, hooks).unwrap();
        Server::new(engine)
    }
}

pub fn serializable<VS, US>(input: (VS, US, Hooks)) -> (Serializable<VS, US>, Hooks)
where
    VS: VersionedStorage,
    US: UnversionedStorage,
{
    (Serializable::new(input.0, input.1, input.2.clone()), input.2)
}

pub fn optimistic<VS, US>(input: (VS, US, Hooks)) -> (Optimistic<VS, US>, Hooks)
where
    VS: VersionedStorage,
    US: UnversionedStorage,
{
    (Optimistic::new(input.0, input.1, input.2.clone()), input.2)
}

pub fn memory() -> (Memory, Memory, Hooks) {
    (Memory::default(), Memory::default(), Hooks::default())
}

pub fn lmdb(path: &Path) -> (Lmdb, Lmdb, Hooks) {
    let result = Lmdb::new(path);
    (result.clone(), result, Hooks::default())
}

pub fn sqlite(path: &Path) -> (Sqlite, Sqlite, Hooks) {
    let result = Sqlite::new(path);
    (result.clone(), result, Hooks::default())
}
