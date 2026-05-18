// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Catalogue of long-lived background actors that run inside a ReifyDB instance.
//!
//! The actors themselves are implemented in the subsystems they coordinate; the types live here so
//! any crate can build messages or subscribe to events without pulling in a runtime dependency cycle.
//!
//! Invariant: every long-lived background actor in the workspace appears in exactly one submodule here. Adding an actor
//! in another crate without registering its message and event types in this module leaves it invisible to cross-crate
//! event subscribers and to the supervisor that wires the system together at startup.

pub mod admin;
pub mod cdc;
pub mod drop;
pub mod flow;
pub mod historical_gc;
pub mod metric;
pub mod operator_ttl;
pub mod pending;
pub mod replication;
pub mod server;
pub mod ttl;
pub mod watermark;
