// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::data::column::ColumnsFFI;

/// Type of diff operation
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffType {
	/// Insert a new row
	Insert = 1,
	/// Update an existing row
	Update = 2,
	/// Remove a row
	Remove = 3,
}

/// FFI-safe diff (batch version using columnar format)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct DiffFFI {
	/// Type of the diff
	pub diff_type: DiffType,
	/// Previous state (empty for Insert)
	pub pre: ColumnsFFI,
	/// New state (empty for Remove)
	pub post: ColumnsFFI,
}
