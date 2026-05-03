// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Streaming flow runtime: continuously evaluates registered flow definitions over the change stream coming out of
//! the transaction layer, applies the operator graph the planner produced, and writes the resulting deltas back into
//! the catalog so downstream queries observe a derived view that updates in step with its inputs.
//!
//! The runtime hosts both built-in operators and FFI-loaded ones from extensions, threading them through a shared
//! deferred-work queue so backpressure from a slow consumer does not block fast ones. Connectors at the edges of
//! the graph translate between the engine's internal column shape and external sources/sinks.
//!
//! Invariant: a flow's output for a given input set is fully determined by its definition - replaying the same input
//! deltas through the same flow definition produces the same output deltas. Operators that introduce hidden state
//! (a clock, a random number, an external read that may differ between runs) break this guarantee and break replay.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod builder;
pub(crate) mod catalog;
pub mod connector;
pub(crate) mod deferred;
pub mod engine;
#[cfg(reifydb_target = "native")]
pub mod ffi;
#[allow(dead_code)]
pub mod operator;
pub mod subsystem;
pub mod transaction;

pub(crate) use operator::Operator;
pub(crate) mod transactional;
