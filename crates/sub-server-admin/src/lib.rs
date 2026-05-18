// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Admin web UI and HTTP API hosted on top of Axum. Serves the bundled static assets, exposes management endpoints
//! that wrap RQL admin operations (create/drop catalog objects, inspect identities, manage policies), and threads
//! every request through `sub-server`'s shared dispatch so authentication and policy enforcement match the other
//! protocols.
//!
//! The admin endpoints are deliberately a small surface; anything beyond what they expose is meant to go through the
//! standard gRPC/HTTP/WebSocket protocols. The crate is gated on multi-threaded targets because Axum requires it.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod actor;
#[cfg(not(reifydb_single_threaded))]
pub mod assets;
pub mod config;
#[cfg(not(reifydb_single_threaded))]
pub mod factory;
#[cfg(not(reifydb_single_threaded))]
pub mod handlers;
#[cfg(not(reifydb_single_threaded))]
pub mod routes;
#[cfg(not(reifydb_single_threaded))]
pub mod state;
#[cfg(not(reifydb_single_threaded))]
pub mod subsystem;
