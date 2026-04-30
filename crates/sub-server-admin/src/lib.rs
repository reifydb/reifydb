// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
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
