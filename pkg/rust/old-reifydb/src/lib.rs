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
pub use reifydb_system as system;
pub use reifydb_transaction as transaction;

use std::path::Path;

use reifydb_core::hook::Hooks;
use reifydb_core::interface::VersionedTransaction;
use reifydb_core::interface::{UnversionedTransaction, VersionedStorage};
#[cfg(feature = "client")]
pub use reifydb_network::grpc::client;
/// The underlying persistence responsible for data access.
use reifydb_storage::lmdb::Lmdb;
use reifydb_storage::memory::Memory;
use reifydb_storage::sqlite::{Sqlite, SqliteConfig};
use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;
use reifydb_transaction::mvcc::transaction::serializable::Serializable;
use reifydb_transaction::svl::SingleVersionLock;
#[cfg(feature = "embedded_async")]
use variant::embedded_async::EmbeddedAsyncBuilder;
#[cfg(feature = "embedded_sync")]
use variant::embedded_sync::EmbeddedSyncBuilder;
#[cfg(feature = "server")]
use variant::server::ServerBuilder;
use variant::system::SystemBuilder;

pub mod hook;
#[allow(unused_imports, unused_variables)]
pub mod session;
pub mod variant;

pub struct ReifyDB {}

impl ReifyDB {
    #[cfg(feature = "embedded_async")]
    pub fn embedded_async() -> EmbeddedAsyncBuilder<
        Serializable<Memory, SingleVersionLock<Memory>>,
        SingleVersionLock<Memory>,
    > {
        let (versioned, unversioned, hooks) = serializable(memory());
        EmbeddedAsyncBuilder::new(versioned, unversioned, hooks)
    }

    #[cfg(feature = "embedded_async")]
    pub fn embedded_async_with<VT, UT>(input: (VT, UT, Hooks)) -> EmbeddedAsyncBuilder<VT, UT>
    where
        VT: VersionedTransaction,
        UT: UnversionedTransaction,
    {
        let (versioned, unversioned, hooks) = input;
        EmbeddedAsyncBuilder::new(versioned, unversioned, hooks)
    }

    #[cfg(feature = "embedded_sync")]
    pub fn embedded_sync() -> EmbeddedSyncBuilder<
        Serializable<Memory, SingleVersionLock<Memory>>,
        SingleVersionLock<Memory>,
    > {
        let (versioned, unversioned, hooks) = serializable(memory());
        EmbeddedSyncBuilder::new(versioned, unversioned, hooks)
    }

    #[cfg(feature = "embedded_sync")]
    pub fn embedded_sync_with<VT, UT>(input: (VT, UT, Hooks)) -> EmbeddedSyncBuilder<VT, UT>
    where
        VT: VersionedTransaction,
        UT: UnversionedTransaction,
    {
        let (versioned, unversioned, hooks) = input;
        EmbeddedSyncBuilder::new(versioned, unversioned, hooks)
    }

    #[cfg(feature = "server")]
    pub fn server()
    -> ServerBuilder<Serializable<Memory, SingleVersionLock<Memory>>, SingleVersionLock<Memory>>
    {
        let (versioned, unversioned, hooks) = serializable(memory());
        ServerBuilder::new(versioned, unversioned, hooks)
    }

    #[cfg(feature = "server")]
    pub fn server_with<VT, UT>(input: (VT, UT, Hooks)) -> ServerBuilder<VT, UT>
    where
        VT: VersionedTransaction,
        UT: UnversionedTransaction,
    {
        let (versioned, unversioned, hooks) = input;
        ServerBuilder::new(versioned, unversioned, hooks)
    }

    /// Create a new system with default in-memory storage and serializable transactions
    pub fn system() -> SystemBuilder<
        Serializable<Memory, SingleVersionLock<Memory>>, 
        SingleVersionLock<Memory>
    > {
        let (versioned, unversioned, hooks) = serializable(memory());
        SystemBuilder::new(versioned, unversioned, hooks)
    }

    /// Create a new system with custom storage and transaction layers
    pub fn system_with<VT, UT>(input: (VT, UT, Hooks)) -> SystemBuilder<VT, UT>
    where
        VT: VersionedTransaction,
        UT: UnversionedTransaction,
    {
        let (versioned, unversioned, hooks) = input;
        SystemBuilder::new(versioned, unversioned, hooks)
    }
}

pub fn serializable<VS, UT>(input: (VS, UT, Hooks)) -> (Serializable<VS, UT>, UT, Hooks)
where
    VS: VersionedStorage,
    UT: UnversionedTransaction,
{
    (Serializable::new(input.0, input.1.clone(), input.2.clone()), input.1, input.2)
}

pub fn optimistic<VS, UT>(input: (VS, UT, Hooks)) -> (Optimistic<VS, UT>, UT, Hooks)
where
    VS: VersionedStorage,
    UT: UnversionedTransaction,
{
    (Optimistic::new(input.0, input.1.clone(), input.2.clone()), input.1, input.2)
}

pub fn memory() -> (Memory, SingleVersionLock<Memory>, Hooks) {
    let hooks = Hooks::new();
    (Memory::default(), SingleVersionLock::new(Memory::new(), hooks.clone()), hooks)
}

pub fn lmdb(path: &Path) -> (Lmdb, SingleVersionLock<Lmdb>, Hooks) {
    let hooks = Hooks::new();
    let result = Lmdb::new(path);
    (result.clone(), SingleVersionLock::new(result, hooks.clone()), hooks)
}

pub fn sqlite(config: SqliteConfig) -> (Sqlite, SingleVersionLock<Sqlite>, Hooks) {
    let hooks = Hooks::new();
    let result = Sqlite::new(config);
    (result.clone(), SingleVersionLock::new(result, hooks.clone()), hooks)
}
