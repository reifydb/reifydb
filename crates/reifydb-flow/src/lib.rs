// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(warnings))] // FIXME

#[allow(dead_code)]
mod compile;
#[allow(dead_code)]
mod core;
#[allow(dead_code)]
mod operator;
#[allow(dead_code)]
mod process;

pub use core::*;

pub use compile::compile_flow;

pub mod legacy_processor; // FIXME remove that

pub use reifydb_core::Result;
