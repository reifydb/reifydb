// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Admin web UI and HTTP API on Axum: bundled static assets plus management endpoints wrapping RQL admin
//! operations, threaded through `sub-server`'s shared dispatch so auth and policy match the other protocols. The
//! surface is deliberately small, and the crate is gated on multi-threaded targets because Axum requires it.

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
