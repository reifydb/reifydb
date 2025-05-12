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

pub use base::*;
pub use error::Error;

mod embedded;
/// Handles serialization and deserialization of data types.
// pub mod encoding;
mod error;

/// The high-level query language layer, responsible for parsing, planning, optimizing, and executing queries.
pub use rql;

/// The execution engine layer, responsible for evaluating query plans and orchestrating data flow between layers.
pub use engine;
use engine::execute::{ExecutionResult, execute_plan, execute_plan_mut};
use engine::{Engine, TransactionMut};
use rql::ast;
use rql::plan::{plan, plan_mut};
/// The underlying key-value store responsible for persistence and data access.
pub use storage;
use storage::Memory;

pub struct ReifyDB {
    mode: Mode,
}

enum Mode {
    /// connect to server
    Client,
    /// embedd into application
    Embedded { engine: engine::svl::Engine<Memory> },
    /// run as server
    Server,
}

impl ReifyDB {
    pub fn in_memory() -> Self {
        Self { mode: Mode::Embedded { engine: engine::svl::Engine::new(Memory::default()) } }
    }
}

// FIXME RESULT
impl ReifyDB {
    /// runs tx
    pub fn tx(&self, rql: &str) -> Vec<ExecutionResult> {
        match &self.mode {
            Mode::Client => unimplemented!(),
            Mode::Embedded { engine } => {
                let mut result = vec![];
                let statements = ast::parse(rql);

                let mut tx = engine.begin().unwrap();

                for statement in statements {
                    let plan = plan_mut(statement).unwrap();
                    let er = execute_plan_mut(plan, &mut tx).unwrap();
                    result.push(er);
                }

                tx.commit().unwrap();

                result
            }
            Mode::Server => unimplemented!(),
        }
    }

    /// runs rx
    pub fn rx(&self, rql: &str) -> Vec<ExecutionResult> {
        match &self.mode {
            Mode::Client => unimplemented!(),
            Mode::Embedded { engine } => {
                let mut result = vec![];
                let statements = ast::parse(rql);

                let rx = engine.begin_read_only().unwrap();
                for statement in statements {
                    let plan = plan(statement).unwrap();
                    let er = execute_plan(plan, &rx).unwrap();
                    result.push(er);
                }

                result
            }
            Mode::Server => unimplemented!(),
        }
    }

    pub fn engine(&self) -> &engine::svl::Engine<Memory> {
        match &self.mode {
            Mode::Client => unimplemented!(),
            Mode::Embedded { engine } => &engine,
            Mode::Server => unimplemented!(),
        }
    }

    pub fn engine_mut(&mut self) -> &mut engine::svl::Engine<Memory> {
        match &mut self.mode {
            Mode::Client => unimplemented!(),
            Mode::Embedded { engine } => engine,
            Mode::Server => unimplemented!(),
        }
    }

    // runs rx
}
