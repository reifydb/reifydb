// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Background-task scheduler for ReifyDB-internal jobs: registering recurring or deferred work, dispatching it on
//! the runtime's pools, and tracking handles so an admin can list, cancel, or inspect what is running. Used for
//! flushers, compactors, telemetry collection, and any subsystem-owned background loop that benefits from sharing
//! the workspace's scheduling discipline.
//!
//! The crate is not a user-facing job system - it does not run user-supplied procedures on a cron. That kind of
//! workload belongs in routines and flow.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod context;
#[cfg(not(reifydb_single_threaded))]
pub mod coordinator;
#[cfg(not(reifydb_single_threaded))]
pub mod factory;
#[cfg(not(reifydb_single_threaded))]
pub mod handle;
pub mod registry;
pub mod schedule;
#[cfg(not(reifydb_single_threaded))]
pub mod subsystem;
pub mod task;
