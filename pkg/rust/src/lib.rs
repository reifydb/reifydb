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
use reifydb_core::interface::VersionedTransaction;
use reifydb_core::interface::{Principal, UnversionedTransaction, VersionedStorage};
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
// pub mod session;
pub mod variant;

// Re-export the params macro
// pub use crate::params;

pub struct ReifyDB {}

pub trait DB<'a>: Sized {
    fn command_as(
        &self,
        principal: &Principal,
        rql: &str,
    ) -> impl Future<Output = Result<Vec<Frame>>> + Send;

    fn command_as_root(&self, rql: &str) -> impl Future<Output = Result<Vec<Frame>>> + Send;

    fn query_as(
        &self,
        principal: &Principal,
        rql: &str,
    ) -> impl Future<Output = Result<Vec<Frame>>> + Send;

    fn query_as_root(&self, rql: &str) -> impl Future<Output = Result<Vec<Frame>>> + Send;
}

impl ReifyDB {
    #[cfg(feature = "embedded")]
    pub fn embedded()
    -> EmbeddedBuilder<Serializable<Memory, SingleVersionLock<Memory>>, SingleVersionLock<Memory>>
    {
        let (versioned, unversioned, hooks) = serializable(memory());
        EmbeddedBuilder::new(versioned, unversioned, hooks)
    }

    #[cfg(feature = "embedded_blocking")]
    pub fn embedded_blocking() -> EmbeddedBlockingBuilder<
        Serializable<Memory, SingleVersionLock<Memory>>,
        SingleVersionLock<Memory>,
    > {
        let (versioned, unversioned, hooks) = serializable(memory());
        EmbeddedBlockingBuilder::new(versioned, unversioned, hooks)
    }

    #[cfg(all(feature = "embedded_blocking", not(feature = "embedded")))]
    pub fn embedded() -> EmbeddedBlockingBuilder<
        Serializable<Memory, SingleVersionLock<Memory>>,
        SingleVersionLock<Memory>,
    > {
        Self::embedded_blocking()
    }

    #[cfg(feature = "embedded")]
    pub fn embedded_with<VT, UT>(input: (VT, UT, Hooks)) -> EmbeddedBuilder<VT, UT>
    where
        VT: VersionedTransaction,
        UT: UnversionedTransaction,
    {
        let (versioned, unversioned, hooks) = input;
        EmbeddedBuilder::new(versioned, unversioned, hooks)
    }

    #[cfg(all(feature = "embedded_blocking", not(feature = "embedded")))]
    pub fn embedded_with<VT, UT>(input: (VT, UT, Hooks)) -> EmbeddedBlockingBuilder<VT, UT>
    where
        VT: VersionedTransaction,
        UT: UnversionedTransaction,
    {
        let (versioned, unversioned, hooks) = input;
        EmbeddedBlockingBuilder::new(versioned, unversioned, hooks)
    }

    #[cfg(feature = "embedded_blocking")]
    pub fn embedded_blocking_with<VT, UT>(input: (VT, UT, Hooks)) -> EmbeddedBlockingBuilder<VT, UT>
    where
        VT: VersionedTransaction,
        UT: UnversionedTransaction,
    {
        let (versioned, unversioned, hooks) = input;
        EmbeddedBlockingBuilder::new(versioned, unversioned, hooks)
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
