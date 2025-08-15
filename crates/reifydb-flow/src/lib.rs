// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(warnings))] // FIXME

#[allow(dead_code, unused_variables)]
mod compile;
#[allow(dead_code, unused_variables)]
mod core;
#[allow(dead_code, unused_variables)]
pub mod legacy_processor;
#[allow(dead_code, unused_variables)]
mod operator;
#[allow(dead_code, unused_variables)]
mod process; // FIXME remove that

pub use core::*;

pub use compile::compile_flow;
pub use reifydb_core::Result;
