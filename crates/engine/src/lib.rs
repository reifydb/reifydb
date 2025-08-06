// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::Value;
use std::collections::HashMap;

pub mod columnar;
mod engine;
mod evaluate;
pub(crate) mod execute;
pub mod flow;
#[allow(dead_code)]
mod function;
mod system;

pub use engine::Engine;
pub use execute::{execute_query, execute_command};
pub use reifydb_core::Result;

pub type GroupKey = Vec<Value>;
pub type GroupByView = HashMap<GroupKey, Vec<usize>>;
