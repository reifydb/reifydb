// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Streaming flow runtime: evaluates registered flow definitions over the change stream from the
//! transaction layer and writes the resulting deltas back into the catalog.
//!
//! Invariant: a flow's output is fully determined by its definition, so an operator carrying
//! hidden state (a clock, a random number, an external read) breaks replay.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod builder;
pub(crate) mod catalog;
pub mod connector;
pub mod context;
pub(crate) mod deferred;
pub mod engine;
pub mod error;
pub mod execution;
#[cfg(reifydb_target = "native")]
pub(crate) mod ffi;
pub(crate) mod lineage;
pub mod operator;
pub mod subsystem;
pub(crate) mod transactional;
