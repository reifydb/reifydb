// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! CDC-driven replication: a primary publishes its change stream over the wire, replicas tail it, and apply the
//! deltas locally so their committed state converges to the primary's. The crate owns both sides - the primary
//! actor that serialises CDC records into the replication protocol, and the replica actor that consumes and applies
//! them - plus the conversion between in-process delta types and the wire shape generated from protobuf.
//!
//! Replication is built on the same CDC stream that subscriptions and external sinks read from, so a replica is just
//! another consumer of the source-of-truth log. There is no separate replication log; if CDC has the record, the
//! replica can converge to it.
//!
//! Invariant: a replica's apply order matches the primary's commit order; CDC records are applied in transaction-id
//! order. Out-of-order application produces divergent state because writes can stomp on later writes.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod actor;
pub mod builder;
pub mod convert;
pub mod error;
#[cfg(not(reifydb_single_threaded))]
pub mod factory;
pub mod generated;
#[cfg(not(reifydb_single_threaded))]
pub mod primary;
pub mod replica;
#[cfg(not(reifydb_single_threaded))]
pub mod subsystem;
