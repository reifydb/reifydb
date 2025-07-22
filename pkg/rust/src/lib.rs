// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! # ReifyDB
//!
//! ReifyDB is an embeddable application backend engine that blends a high-level query language
//! (RQL – Reify Query Language) with a low-level key-value store system.
//!
//! It is designed for rapid prototyping, persistent data manipulation, and embedding powerful
//! logic directly into your app — without the need for a traditional database server.
//!
//! The system is composed of several submodules:
//!
//! - [`encoding`]: Handles serialization and deserialization of data types.
//! - [`rql`]: The high-level query language layer, responsible for parsing, planning, optimizing, and executing queries.
//! - [`store`]: The underlying key-value store responsible for persistence and data access.
//!
//! ReifyDB aims to be minimal, developer-first, and flexible enough to power backends, embedded analytics, and local-first systems.
//!
//! See also: [`testscript`] for running integration-style script tests.

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
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{Principal, Transaction, UnversionedStorage, VersionedStorage};
use reifydb_engine::frame::Frame;
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

    fn rx_as(
        &self,
        principal: &Principal,
        rql: &str,
    ) -> impl Future<Output = Result<Vec<Frame>>> + Send;
}

impl ReifyDB {
    #[cfg(feature = "embedded")]
    pub fn embedded() -> (Embedded<Memory, Memory, Serializable<Memory, Memory>>, Principal) {
        Embedded::new(serializable(memory()))
    }

    #[cfg(feature = "embedded_blocking")]
    pub fn embedded_blocking()
    -> (embedded_blocking::Embedded<Memory, Memory, Serializable<Memory, Memory>>, Principal) {
        embedded_blocking::Embedded::new(serializable(memory())).unwrap()
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
    pub fn embedded_with<VS, US, T>(transaction: T) -> (Embedded<VS, US, T>, Principal)
    where
        VS: VersionedStorage,
        US: UnversionedStorage,
        T: Transaction<VS, US>,
    {
        Embedded::new(transaction)
    }

    #[cfg(all(feature = "embedded_blocking", not(feature = "embedded")))]
    pub fn embedded_with<VS, US, T>(
        transaction: T,
    ) -> (embedded_blocking::Embedded<VS, US, T>, Principal)
    where
        VS: VersionedStorage,
        US: UnversionedStorage,
        T: Transaction<VS, US>,
    {
        embedded_blocking::Embedded::new(transaction).unwrap()
    }

    #[cfg(feature = "embedded_blocking")]
    pub fn embedded_blocking_with<VS, US, T>(
        transaction: T,
    ) -> (embedded_blocking::Embedded<VS, US, T>, Principal)
    where
        VS: VersionedStorage,
        US: UnversionedStorage,
        T: Transaction<VS, US>,
    {
        embedded_blocking::Embedded::new(transaction).unwrap()
    }

    #[cfg(feature = "server")]
    pub fn server() -> Server<Memory, Memory, Serializable<Memory, Memory>> {
        Server::new(serializable(memory()))
    }

    #[cfg(feature = "server")]
    pub fn server_with<VS, US, T>(transaction: T) -> Server<VS, US, T>
    where
        VS: VersionedStorage,
        US: UnversionedStorage,
        T: Transaction<VS, US>,
    {
        Server::new(transaction)
    }
}

pub fn serializable<VS, US>(storage: (VS, US)) -> Serializable<VS, US>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
{
    Serializable::new(storage.0, storage.1, Hooks::default())
}

pub fn optimistic<VS, US>(storage: (VS, US)) -> Optimistic<VS, US>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
{
    Optimistic::new(storage.0, storage.1, Hooks::default())
}

pub fn memory() -> (Memory, Memory) {
    (Memory::default(), Memory::default())
}

pub fn lmdb(path: &Path) -> (Lmdb, Lmdb) {
    let result = Lmdb::new(path);
    (result.clone(), result)
}

pub fn sqlite(path: &Path) -> (Sqlite, Sqlite) {
    let result = Sqlite::new(path);
    (result.clone(), result)
}
