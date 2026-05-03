// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Per-subsystem diagnostic catalogues.
//!
//! Each submodule (catalog, engine, flow, index, internal, operation, query, sequence, subscription, subsystem,
//! transaction) owns the diagnostics produced by one subsystem - their stable codes, default messages, and helper
//! constructors. Diagnostics are the user-facing failure objects sent over the wire; they must carry enough
//! source-fragment context that the client can highlight the offending RQL or configuration.
//!
//! Invariant: diagnostic codes are public API. Once shipped, a code's identity (e.g. `CA_087`) must continue to refer
//! to the same logical condition; renaming or recycling a code breaks tooling and documentation that pin on it.

pub mod catalog;
pub mod core_error;
pub mod engine;
pub mod flow;
pub mod index;
pub mod internal;
pub mod operation;
pub mod query;
pub mod sequence;
pub mod subscription;
pub mod subsystem;
pub mod transaction;
