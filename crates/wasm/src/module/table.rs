// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use crate::module::{function::Function, value::Value};

#[derive(Clone)]
pub struct Table {
	pub elements: Vec<Option<Value>>,
	pub limit: TableLimit,
	/// Resolved function references for cross-module call_indirect.
	/// Parallel to `elements`; when Some, call_indirect uses this instead of module-local lookup.
	pub func_refs: Vec<Option<Arc<Function>>>,
}

#[derive(Clone, PartialEq)]
pub struct TableLimit {
	pub min: u32,
	pub max: Option<u32>,
}
