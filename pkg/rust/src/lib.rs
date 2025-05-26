// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! # ReifyDB
//!
//! ReifyDB is an embeddable application backend engine that blends a high-level query language
//! (RQL – Reify Query Language) with a low-level key-value storage system.
//!
//! It is designed for rapid prototyping, persistent data manipulation, and embedding powerful
//! logic directly into your app — without the need for a traditional database server.
//!
//! The system is composed of several submodules:
//!
//! - [`encoding`]: Handles serialization and deserialization of data types.
//! - [`rql`]: The high-level query language layer, responsible for parsing, planning, optimizing, and executing queries.
//! - [`storage`]: The underlying key-value store responsible for persistence and data access.
//!
//! ReifyDB aims to be minimal, developer-first, and flexible enough to power backends, embedded analytics, and local-first systems.
//!
//! See also: [`testscript`] for running integration-style script tests.

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use auth::Principal;
pub use base::*;
/// The execution engine layer, responsible for evaluating query plans and orchestrating data flow between layers.
pub use engine;
use engine::old_execute::ExecutionResult;
pub use error::Error;
/// The high-level query language layer, responsible for parsing, planning, optimizing, and executing queries.
pub use rql;

#[cfg(any(feature = "server", feature = "client"))]
pub use tokio::*;

// pub use session::{IntoSessionRx, IntoSessionTx, SessionRx, SessionTx};

/// The underlying key-value store responsible for persistence and data access.
pub use storage;

pub use transaction;

#[cfg(feature = "embedded")]
use crate::embedded::Embedded;

#[cfg(feature = "server")]
use crate::server::Server;

use storage::{Memory, StorageEngine};
use transaction::{TransactionEngine, mvcc, svl};

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
    pub fn embedded() -> (Embedded<Memory, mvcc::Engine<Memory>>, Principal) {
        Embedded::new(mvcc(memory()))
    }

    #[cfg(feature = "embedded_blocking")]
    pub fn embedded_blocking()
    -> (embedded_blocking::Embedded<Memory, mvcc::Engine<Memory>>, Principal) {
        embedded_blocking::Embedded::new(mvcc(memory()))
    }

    #[cfg(all(feature = "embedded_blocking", not(feature = "embedded")))]
    pub fn embedded() -> (embedded_blocking::Embedded<Memory, mvcc::Engine<Memory>>, Principal) {
        Self::embedded_blocking()
    }

    #[cfg(feature = "embedded")]
    pub fn embedded_with<S: StorageEngine, T: TransactionEngine<S>>(
        transaction: T,
    ) -> (Embedded<S, T>, Principal) {
        Embedded::new(transaction)
    }

    #[cfg(all(feature = "embedded_blocking", not(feature = "embedded")))]
    pub fn embedded_with<S: StorageEngine, T: TransactionEngine<S>>(
        transaction: T,
    ) -> (embedded_blocking::Embedded<S, T>, Principal) {
        embedded_blocking::Embedded::new(transaction)
    }

    #[cfg(feature = "embedded_blocking")]
    pub fn embedded_blocking_with<S: StorageEngine, T: TransactionEngine<S>>(
        transaction: T,
    ) -> (embedded_blocking::Embedded<S, T>, Principal) {
        embedded_blocking::Embedded::new(transaction)
    }

    #[cfg(feature = "server")]
    pub fn server() -> Server<Memory, mvcc::Engine<Memory>> {
        Server::new(mvcc(memory()))
    }

    #[cfg(feature = "server")]
    pub fn server_with<S: StorageEngine + 'static, T: TransactionEngine<S> + 'static>(
        transaction: T,
    ) -> Server<S, T> {
        Server::new(transaction)
    }
}

pub fn svl<S: StorageEngine>(storage: S) -> svl::Engine<S> {
    svl::Engine::new(storage)
}

pub fn mvcc<S: StorageEngine>(storage: S) -> mvcc::Engine<S> {
    mvcc::Engine::new(storage)
}

pub fn memory() -> Memory {
    Memory::default()
}
