// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

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
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use error::Error;
pub use reifydb_auth::Principal;
pub use reifydb_core::*;
/// The execution engine layer, responsible for evaluating query plans and orchestrating data flow between layers.
pub use reifydb_engine;
/// The high-level query language layer, responsible for parsing, planning, optimizing, and executing queries.
pub use reifydb_rql;
use std::path::Path;

use reifydb_engine::ExecutionResult;
/// The underlying persistence responsible for data access.
pub use reifydb_persistence;
#[cfg(any(feature = "server", feature = "client"))]
pub use tokio::*;

pub use reifydb_transaction;

#[cfg(feature = "embedded")]
use crate::embedded::Embedded;

#[cfg(feature = "server")]
use crate::server::Server;

use reifydb_persistence::{Lmdb, Memory, Persistence};
use reifydb_transaction::Transaction;

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "embedded")]
pub mod embedded;

#[cfg(feature = "embedded_blocking")]
pub mod embedded_blocking;

mod error;

#[cfg(feature = "server")]
pub mod server;
mod session;

pub struct ReifyDB {}

pub trait DB<'a>: Sized {
    /// runs tx
    fn tx_as(
        &self,
        principal: &Principal,
        rql: &str,
    ) -> impl Future<Output = Vec<ExecutionResult>> + Send;

    /// runs rx
    fn rx_as(
        &self,
        principal: &Principal,
        rql: &str,
    ) -> impl Future<Output = Vec<ExecutionResult>> + Send;

    // fn session_read_only(&self, into: impl IntoSessionRx<'a, Self>) -> Result<SessionRx<'a, Self>>;
    //
    // fn session(&self, into: impl IntoSessionTx<'a, Self>) -> Result<SessionTx<'a, Self>>;
}

impl ReifyDB {
    #[cfg(feature = "embedded")]
    pub fn embedded() -> (
        Embedded<Memory, ::reifydb_transaction::mvcc::transaction::serializable::Serializable>,
        Principal,
    ) {
        Embedded::new(serializable())
    }

    #[cfg(feature = "embedded_blocking")]
    pub fn embedded_blocking() -> (
        embedded_blocking::Embedded<
            Memory,
            ::reifydb_transaction::mvcc::transaction::serializable::Serializable,
        >,
        Principal,
    ) {
        embedded_blocking::Embedded::new(serializable())
    }

    #[cfg(all(feature = "embedded_blocking", not(feature = "embedded")))]
    pub fn embedded() -> (
        embedded_blocking::Embedded<
            Memory,
            ::reifydb_transaction::mvcc::transaction::serializable::Serializable,
        >,
        Principal,
    ) {
        Self::embedded_blocking()
    }

    #[cfg(feature = "embedded")]
    pub fn embedded_with<P: Persistence, T: Transaction<P>>(
        transaction: T,
    ) -> (Embedded<P, T>, Principal) {
        Embedded::new(transaction)
    }

    #[cfg(all(feature = "embedded_blocking", not(feature = "embedded")))]
    pub fn embedded_with<P: Persistence, T: Transaction<P>>(
        transaction: T,
    ) -> (embedded_blocking::Embedded<P, T>, Principal) {
        embedded_blocking::Embedded::new(transaction)
    }

    #[cfg(feature = "embedded_blocking")]
    pub fn embedded_blocking_with<P: Persistence, T: Transaction<P>>(
        transaction: T,
    ) -> (embedded_blocking::Embedded<P, T>, Principal) {
        embedded_blocking::Embedded::new(transaction)
    }

    #[cfg(feature = "server")]
    pub fn server()
    -> Server<Memory, ::reifydb_transaction::mvcc::transaction::serializable::Serializable> {
        Server::new(serializable())
    }

    #[cfg(feature = "server")]
    pub fn server_with<P: Persistence + 'static, T: Transaction<P> + 'static>(
        transaction: T,
    ) -> Server<P, T> {
        Server::new(transaction)
    }
}

pub fn svl<P: Persistence>(persistence: P) -> ::reifydb_transaction::svl::Svl<P> {
    ::reifydb_transaction::svl::Svl::new(persistence)
}

pub fn serializable() -> ::reifydb_transaction::mvcc::transaction::serializable::Serializable {
    ::reifydb_transaction::mvcc::transaction::serializable::Serializable::new()
}

pub fn optimistic() -> ::reifydb_transaction::mvcc::transaction::optimistic::Optimistic {
    ::reifydb_transaction::mvcc::transaction::optimistic::Optimistic::new()
}

pub fn memory() -> Memory {
    Memory::default()
}

pub fn lmdb(path: &Path) -> Lmdb {
    Lmdb::new(path).unwrap()
}
