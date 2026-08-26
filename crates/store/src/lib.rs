// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod coverage;
pub mod filter;
pub mod metrics;
pub mod row;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod sqlite;
