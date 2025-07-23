// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use reifydb_core::Result;

pub use engine::Engine;
pub use execute::{execute_rx, execute_tx};

mod engine;
mod evaluate;
pub(crate) mod execute;
pub mod frame;
#[allow(dead_code)]
mod function;
mod get;
mod system;
#[allow(dead_code)]
pub(crate) mod view;
