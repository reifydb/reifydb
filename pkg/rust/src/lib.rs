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

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

pub use auth::Principal;

pub use base::*;
// pub use embedded::Embedded;
/// The execution engine layer, responsible for evaluating query plans and orchestrating data flow between layers.
pub use engine;
use engine::execute::ExecutionResult;
pub use error::Error;
/// The high-level query language layer, responsible for parsing, planning, optimizing, and executing queries.
pub use rql;

pub use session::{IntoSessionRx, IntoSessionTx, SessionRx, SessionTx};

/// The underlying key-value store responsible for persistence and data access.
pub use storage;

pub use transaction;

use crate::embedded::Embedded;
use storage::{Memory, StorageEngine};
use transaction::{TransactionEngine, mvcc, svl};

pub mod client;
pub mod embedded;
mod error;
pub mod server;
mod session;

pub struct ReifyDB {}

pub trait DB<'a>: Sized {
    /// runs tx
    fn tx_execute_as(&self, principal: &Principal, rql: &str) -> Vec<ExecutionResult>;
    /// runs rx
    fn rx_execute_as(&self, principal: &Principal, rql: &str) -> Vec<ExecutionResult>;

    fn session_read_only(&self, into: impl IntoSessionRx<'a, Self>) -> Result<SessionRx<'a, Self>>;

    fn session(&self, into: impl IntoSessionTx<'a, Self>) -> Result<SessionTx<'a, Self>>;
}

impl ReifyDB {
    pub fn embedded<'a>() -> (Embedded<Memory, mvcc::Engine<Memory>>, Principal) {
        Embedded::new(mvcc(memory()))
    }

    pub fn embedded_with<'a, S: StorageEngine, T: TransactionEngine<S>>(
        transaction: T,
    ) -> (Embedded<S, T>, Principal) {
        Embedded::new(transaction)
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
