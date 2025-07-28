// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub use reifydb_core::Result;

pub use engine::Engine;
pub use execute::{execute_rx, execute_tx};

mod engine;
mod evaluate;
pub(crate) mod execute;
pub mod flow;

#[allow(dead_code)]
mod function;
mod system;
