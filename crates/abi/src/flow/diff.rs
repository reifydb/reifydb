// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::data::column::ColumnsFFI;

/// Type of flow diff operation
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowDiffType {
	/// Insert a new row
	Insert = 0,
	/// Update an existing row
	Update = 1,
	/// Remove a row
	Remove = 2,
}

/// FFI-safe flow diff (batch version using columnar format)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FlowDiffFFI {
	/// Type of the diff
	pub diff_type: FlowDiffType,
	/// Previous state (empty for Insert)
	pub pre: ColumnsFFI,
	/// New state (empty for Remove)
	pub post: ColumnsFFI,
}
