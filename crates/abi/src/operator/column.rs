// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use core::ptr::null;

use crate::data::buffer::BufferFFI;

/// FFI-safe operator column definition
///
/// Describes a single column in an operator's input or output,
/// including name, type constraint, and description for documentation purposes.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct OperatorColumnDefFFI {
	/// Column name (UTF-8 encoded)
	pub name: BufferFFI,
	/// Base type code (use reifydb_type::value::r#type::Type::to_u8/from_u8)
	pub base_type: u8,
	/// Constraint type: 0=None, 1=MaxBytes, 2=PrecisionScale
	pub constraint_type: u8,
	/// First constraint parameter: MaxBytes value OR precision (as u32)
	pub constraint_param1: u32,
	/// Second constraint parameter: scale (only for PrecisionScale, 0 otherwise)
	pub constraint_param2: u32,
	/// Human-readable description (UTF-8 encoded)
	pub description: BufferFFI,
}

/// FFI-safe operator column definitions
///
/// Describes the input or output columns of an operator for documentation
/// and discovery purposes.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct OperatorColumnDefsFFI {
	/// Pointer to array of column definitions
	pub columns: *const OperatorColumnDefFFI,
	/// Number of columns
	pub column_count: usize,
}

impl OperatorColumnDefsFFI {
	/// Create empty column definitions (no columns)
	pub const fn empty() -> Self {
		Self {
			columns: null(),
			column_count: 0,
		}
	}
}
