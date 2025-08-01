// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

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

use reifydb_core::hook::Hooks;
#[cfg(any(feature = "embedded", feature = "embedded_blocking", feature = "server"))]
use reifydb_core::interface::Transaction;
use reifydb_core::interface::{Principal, UnversionedStorage, VersionedStorage};
use reifydb_core::result::Frame;
#[cfg(feature = "client")]
pub use reifydb_network::grpc::client;
/// The underlying persistence responsible for data access.
use reifydb_storage::lmdb::Lmdb;
use reifydb_storage::memory::Memory;
use reifydb_storage::sqlite::{Sqlite, SqliteConfig};
use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;
use reifydb_transaction::mvcc::transaction::serializable::Serializable;
use reifydb_transaction::svl::SingleVersionLock;
#[cfg(feature = "embedded")]
use variant::embedded::EmbeddedBuilder;
#[cfg(feature = "embedded_blocking")]
use variant::embedded_blocking::EmbeddedBlockingBuilder;
#[cfg(feature = "server")]
use variant::server::ServerBuilder;

pub mod hook;
mod session;
pub mod variant;

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
    pub fn embedded() -> EmbeddedBuilder<Memory, Memory, Serializable<Memory, Memory>, SingleVersionLock<Memory>> {
        let (transaction, hooks) = serializable(memory());
        let unversioned = SingleVersionLock::new(Memory::default());
        EmbeddedBuilder::new(transaction, unversioned, hooks)
    }

    #[cfg(feature = "embedded_blocking")]
    pub fn embedded_blocking()
    -> EmbeddedBlockingBuilder<Memory, Memory, Serializable<Memory, Memory>, SingleVersionLock<Memory>> {
        let (transaction, hooks) = serializable(memory());
        let unversioned = SingleVersionLock::new(Memory::default());
        EmbeddedBlockingBuilder::new(transaction, unversioned, hooks)
    }

    #[cfg(all(feature = "embedded_blocking", not(feature = "embedded")))]
    pub fn embedded() -> EmbeddedBlockingBuilder<Memory, Memory, Serializable<Memory, Memory>, SingleVersionLock<Memory>> {
        Self::embedded_blocking()
    }

    #[cfg(feature = "embedded")]
    pub fn embedded_with<VS, US, T>(transaction: T, unversioned: SingleVersionLock<US>, hooks: Hooks) -> EmbeddedBuilder<VS, US, T, SingleVersionLock<US>>
    where
        VS: VersionedStorage,
        US: UnversionedStorage,
        T: Transaction<VS, US>,
    {
        EmbeddedBuilder::new(transaction, unversioned, hooks)
    }

    #[cfg(all(feature = "embedded_blocking", not(feature = "embedded")))]
    pub fn embedded_with<VS, US, T>(
        transaction: T,
        unversioned: SingleVersionLock<US>,
        hooks: Hooks,
    ) -> EmbeddedBlockingBuilder<VS, US, T, SingleVersionLock<US>>
    where
        VS: VersionedStorage,
        US: UnversionedStorage,
        T: Transaction<VS, US>,
    {
        EmbeddedBlockingBuilder::new(transaction, unversioned, hooks)
    }

    #[cfg(feature = "embedded_blocking")]
    pub fn embedded_blocking_with<VS, US, T>(
        input: (T, SingleVersionLock<US>, Hooks),
    ) -> EmbeddedBlockingBuilder<VS, US, T, SingleVersionLock<US>>
    where
        VS: VersionedStorage,
        US: UnversionedStorage,
        T: Transaction<VS, US>,
    {
        let (transaction, unversioned, hooks) = input;
        EmbeddedBlockingBuilder::new(transaction, unversioned, hooks)
    }

    #[cfg(feature = "server")]
    pub fn server() -> ServerBuilder<Memory, Memory, Serializable<Memory, Memory>, SingleVersionLock<Memory>> {
        let (transaction, hooks) = serializable(memory());
        let unversioned = SingleVersionLock::new(Memory::default());
        ServerBuilder::new(transaction, unversioned, hooks)
    }

    #[cfg(feature = "server")]
    pub fn server_with<VS, US, T>(input: (T, SingleVersionLock<US>, Hooks)) -> ServerBuilder<VS, US, T, SingleVersionLock<US>>
    where
        VS: VersionedStorage,
        US: UnversionedStorage,
        T: Transaction<VS, US>,
    {
        let (transaction, unversioned, hooks) = input;
        ServerBuilder::new(transaction, unversioned, hooks)
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

pub fn sqlite(config: SqliteConfig) -> (Sqlite, Sqlite, Hooks) {
    let result = Sqlite::new(config);
    (result.clone(), result, Hooks::default())
}
