// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod builders;
pub mod callbacks;
pub mod chaos;
pub mod context;
pub mod harness;
pub mod helpers;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
pub mod in_process;
pub mod registry;
pub mod state;
