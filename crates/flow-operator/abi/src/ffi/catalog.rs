// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! FFI types for catalog access (NamespaceDef, TableDef, etc.)

use crate::BufferFFI;

/// FFI-safe namespace definition
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FFINamespaceDef {
	/// Namespace ID (u64)
	pub id: u64,
	/// Namespace name (UTF-8 encoded)
	pub name: BufferFFI,
}

/// FFI-safe table definition
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FFITableDef {
	/// Table ID (u64)
	pub id: u64,
	/// Namespace ID this table belongs to
	pub namespace_id: u64,
	/// Table name (UTF-8 encoded)
	pub name: BufferFFI,
	/// Array of column definitions
	pub columns: *const FFIColumnDef,
	/// Number of columns in the array
	pub column_count: usize,
	/// Whether table has a primary key (0=no, 1=yes)
	pub has_primary_key: u8,
	/// Primary key definition (null if has_primary_key=0)
	pub primary_key: *const FFIPrimaryKeyDef,
}

/// FFI-safe column definition
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FFIColumnDef {
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

/// FFI-safe primary key definition
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FFIPrimaryKeyDef {
	/// Primary key ID (u64)
	pub id: u64,
	/// Number of columns in primary key
	pub column_count: usize,
	/// Array of column IDs that compose the primary key
	pub column_ids: *const u64,
}
