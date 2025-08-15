// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub use columnar::{GroupByView, GroupKey};
pub use reifydb_core::value::columnar;
mod engine;
mod evaluate;
mod execute;
#[allow(dead_code)]
mod function;

pub use engine::StandardEngine;
pub use reifydb_core::Result;
