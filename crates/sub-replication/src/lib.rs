// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
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
