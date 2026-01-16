// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::data::buffer::BufferFFI;

/// FFI-safe column definition
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ColumnDefFFI {
	/// Column ID (u64)
	pub id: u64,
	/// Column name (UTF-8 encoded)
	pub name: BufferFFI,
	/// Base type (Type::to_u8())
	pub base_type: u8,
	/// Constraint type (0=None, 1=MaxBytes, 2=PrecisionScale)
	pub constraint_type: u8,
	/// Constraint parameter 1 (MaxBytes value OR precision)
	pub constraint_param1: u32,
	/// Constraint parameter 2 (scale for PrecisionScale)
	pub constraint_param2: u32,
	/// Column position in table
	pub column_index: u8,
	/// Auto-increment flag (0=false, 1=true)
	pub auto_increment: u8,
}
