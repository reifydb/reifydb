// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use super::{column::ColumnDefFFI, primary_key::PrimaryKeyFFI};
use crate::data::buffer::BufferFFI;

/// FFI-safe table definition
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct TableFFI {
	/// Table ID (u64)
	pub id: u64,
	/// Namespace ID this table belongs to
	pub namespace_id: u64,
	/// Table name (UTF-8 encoded)
	pub name: BufferFFI,
	/// Array of column definitions
	pub columns: *const ColumnDefFFI,
	/// Number of columns in the array
	pub column_count: usize,
	/// Whether table has a primary key (0=no, 1=yes)
	pub has_primary_key: u8,
	/// Primary key definition (null if has_primary_key=0)
	pub primary_key: *const PrimaryKeyFFI,
}
