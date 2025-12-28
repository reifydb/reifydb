// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Catalog access for FFI operators
//!
//! Provides read-only access to catalog metadata (namespaces, tables) with
//! version-based queries for time-travel support.

mod namespace;
mod table;

use std::slice::from_raw_parts;

use reifydb_abi::{ColumnDefFFI, PrimaryKeyFFI};
use reifydb_core::{
	CommitVersion,
	interface::{
		ColumnDef, ColumnId, ColumnIndex, NamespaceDef, NamespaceId, PrimaryKeyDef, PrimaryKeyId, TableDef,
		TableId,
	},
};
use reifydb_type::{
	Constraint, Type, TypeConstraint,
	value::constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
};

use crate::{FFIError, OperatorContext};

/// Read-only catalog access wrapper
///
/// Provides safe access to catalog metadata through FFI callbacks.
/// Mirrors the API of MaterializedCatalog.
pub struct Catalog<'a> {
	ctx: &'a mut OperatorContext,
}

impl<'a> Catalog<'a> {
	/// Create a new catalog accessor from an operator context
	pub(crate) fn new(ctx: &'a mut OperatorContext) -> Self {
		Self {
			ctx,
		}
	}

	/// Find a namespace by ID at a specific version
	///
	/// Mirrors MaterializedCatalog::find_namespace
	///
	/// # Parameters
	/// - `namespace`: The namespace ID to look up
	/// - `version`: The commit version for time-travel queries
	///
	/// # Returns
	/// - `Ok(Some(namespace))` if found
	/// - `Ok(None)` if not found
	/// - `Err(_)` on error
	pub fn find_namespace(
		&self,
		namespace: NamespaceId,
		version: CommitVersion,
	) -> Result<Option<NamespaceDef>, FFIError> {
		namespace::raw_catalog_find_namespace(self.ctx, namespace, version)
	}

	/// Find a namespace by name at a specific version
	///
	/// Mirrors MaterializedCatalog::find_namespace_by_name
	///
	/// # Parameters
	/// - `namespace`: The namespace name to look up
	/// - `version`: The commit version for time-travel queries
	///
	/// # Returns
	/// - `Ok(Some(namespace))` if found
	/// - `Ok(None)` if not found
	/// - `Err(_)` on error
	pub fn find_namespace_by_name(
		&self,
		namespace: &str,
		version: CommitVersion,
	) -> Result<Option<NamespaceDef>, FFIError> {
		namespace::raw_catalog_find_namespace_by_name(self.ctx, namespace, version)
	}

	/// Find a table by ID at a specific version
	///
	/// Mirrors MaterializedCatalog::find_table
	///
	/// # Parameters
	/// - `table`: The table ID to look up
	/// - `version`: The commit version for time-travel queries
	///
	/// # Returns
	/// - `Ok(Some(table))` if found
	/// - `Ok(None)` if not found
	/// - `Err(_)` on error
	pub fn find_table(&self, table: TableId, version: CommitVersion) -> Result<Option<TableDef>, FFIError> {
		table::raw_catalog_find_table(self.ctx, table, version)
	}

	/// Find a table by name in a namespace at a specific version
	///
	/// Mirrors MaterializedCatalog::find_table_by_name
	///
	/// # Parameters
	/// - `namespace`: The namespace ID containing the table
	/// - `name`: The table name to look up
	/// - `version`: The commit version for time-travel queries
	///
	/// # Returns
	/// - `Ok(Some(table))` if found
	/// - `Ok(None)` if not found
	/// - `Err(_)` on error
	pub fn find_table_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Result<Option<TableDef>, FFIError> {
		table::raw_catalog_find_table_by_name(self.ctx, namespace, name, version)
	}
}

// ============================================================================
// Shared unmarshal functions
// ============================================================================

/// Unmarshal ColumnDefFFI to ColumnDef
pub(crate) unsafe fn unmarshal_column(ffi_col: &ColumnDefFFI) -> Result<ColumnDef, FFIError> {
	// Convert name BufferFFI to String
	let name_bytes = if !ffi_col.name.ptr.is_null() && ffi_col.name.len > 0 {
		unsafe { from_raw_parts(ffi_col.name.ptr, ffi_col.name.len) }
	} else {
		&[]
	};

	let name = std::str::from_utf8(name_bytes)
		.map_err(|_| FFIError::Other("Invalid UTF-8 in column name".to_string()))?
		.to_string();

	// Decode type constraint
	let constraint = decode_type_constraint(
		ffi_col.base_type,
		ffi_col.constraint_type,
		ffi_col.constraint_param1,
		ffi_col.constraint_param2,
	)?;

	Ok(ColumnDef {
		id: ColumnId(ffi_col.id),
		name,
		constraint,
		policies: Vec::new(), // Simplified version - no policies
		index: ColumnIndex(ffi_col.column_index),
		auto_increment: ffi_col.auto_increment != 0,
		dictionary_id: None, // Simplified version - no dictionary
	})
}

/// Unmarshal PrimaryKeyFFI to PrimaryKeyDef
pub(crate) unsafe fn unmarshal_primary_key(ffi_pk: &PrimaryKeyFFI) -> Result<PrimaryKeyDef, FFIError> {
	// Get column IDs
	let column_ids = if !ffi_pk.column_ids.is_null() && ffi_pk.column_count > 0 {
		unsafe { from_raw_parts(ffi_pk.column_ids, ffi_pk.column_count).to_vec() }
	} else {
		Vec::new()
	};

	// Note: We can't fully reconstruct PrimaryKeyDef because it contains Vec<ColumnDef>,
	// but we only have column IDs. This is a limitation of the simplified FFI.
	// For now, we'll create placeholder ColumnDef entries.
	let columns = column_ids
		.into_iter()
		.enumerate()
		.map(|(idx, col_id)| ColumnDef {
			id: ColumnId(col_id),
			name: format!("col_{}", col_id),
			constraint: TypeConstraint::unconstrained(Type::Int4),
			policies: Vec::new(),
			index: ColumnIndex(idx as u8),
			auto_increment: false,
			dictionary_id: None,
		})
		.collect();

	Ok(PrimaryKeyDef {
		id: PrimaryKeyId(ffi_pk.id),
		columns,
	})
}

/// Decode TypeConstraint from FFI format
///
/// # Parameters
/// - `base_type`: Type::to_u8() value
/// - `constraint_type`: 0=None, 1=MaxBytes, 2=PrecisionScale
/// - `param1`: MaxBytes value OR precision
/// - `param2`: scale
fn decode_type_constraint(
	base_type: u8,
	constraint_type: u8,
	param1: u32,
	param2: u32,
) -> Result<TypeConstraint, FFIError> {
	let ty = Type::from_u8(base_type);

	match constraint_type {
		0 => Ok(TypeConstraint::unconstrained(ty)),
		1 => Ok(TypeConstraint::with_constraint(ty, Constraint::MaxBytes(MaxBytes::new(param1)))),
		2 => Ok(TypeConstraint::with_constraint(
			ty,
			Constraint::PrecisionScale(Precision::new(param1 as u8), Scale::new(param2 as u8)),
		)),
		_ => Err(FFIError::Other("Invalid constraint type".to_string())),
	}
}
