// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! CDC-driven replication: a primary publishes its change stream, replicas tail it and apply the deltas locally.
//! A replica is just another consumer of the CDC log; there is no separate replication log.
//!
//! Invariant: a replica applies CDC records in strictly increasing commit version. The applier rejects anything
//! at or below its last applied version rather than applying it, because replaying out of order lets an older
//! write stomp a newer one.

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
