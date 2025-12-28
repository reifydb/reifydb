// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Raw FFI functions for catalog access
//!
//! These functions call the host-provided catalog callbacks and unmarshal
//! the results from FFI types to Rust types.

use std::{mem::MaybeUninit, slice::from_raw_parts};

use reifydb_abi::{ColumnDefFFI, FFI_NOT_FOUND, FFI_OK, NamespaceFFI, PrimaryKeyFFI, TableFFI};
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

/// Find namespace by ID
pub(super) fn raw_catalog_find_namespace(
	ctx: &OperatorContext,
	namespace_id: NamespaceId,
	version: CommitVersion,
) -> Result<Option<NamespaceDef>, FFIError> {
	unsafe {
		// Get callback function
		let callback = (*ctx.ctx).callbacks.catalog.find_namespace;

		// Allocate output buffer on stack
		let mut output = MaybeUninit::<NamespaceFFI>::uninit();

		// Call FFI callback
		let result = callback(ctx.ctx, namespace_id.0, version.0, output.as_mut_ptr());

		match result {
			FFI_OK => {
				// Success - unmarshal namespace
				let ffi_ns = output.assume_init();
				let namespace = unmarshal_namespace(&ffi_ns)?;

				// Free FFI-allocated memory
				let free_callback = (*ctx.ctx).callbacks.catalog.free_namespace;
				free_callback(&mut output.as_mut_ptr().read());

				Ok(Some(namespace))
			}
			FFI_NOT_FOUND => Ok(None), // Not found
			_ => Err(FFIError::Other("Failed to find namespace".to_string())),
		}
	}
}

/// Find namespace by name
pub(super) fn raw_catalog_find_namespace_by_name(
	ctx: &OperatorContext,
	name: &str,
	version: CommitVersion,
) -> Result<Option<NamespaceDef>, FFIError> {
	unsafe {
		// Get callback function
		let callback = (*ctx.ctx).callbacks.catalog.find_namespace_by_name;

		// Prepare name bytes
		let name_bytes = name.as_bytes();

		// Allocate output buffer on stack
		let mut output = MaybeUninit::<NamespaceFFI>::uninit();

		// Call FFI callback
		let result = callback(ctx.ctx, name_bytes.as_ptr(), name_bytes.len(), version.0, output.as_mut_ptr());

		match result {
			FFI_OK => {
				// Success - unmarshal namespace
				let ffi_ns = output.assume_init();
				let namespace = unmarshal_namespace(&ffi_ns)?;

				// Free FFI-allocated memory
				let free_callback = (*ctx.ctx).callbacks.catalog.free_namespace;
				free_callback(&mut output.as_mut_ptr().read());

				Ok(Some(namespace))
			}
			FFI_NOT_FOUND => Ok(None), // Not found
			_ => Err(FFIError::Other("Failed to find namespace by name".to_string())),
		}
	}
}

/// Find table by ID
pub(super) fn raw_catalog_find_table(
	ctx: &OperatorContext,
	table_id: TableId,
	version: CommitVersion,
) -> Result<Option<TableDef>, FFIError> {
	unsafe {
		// Get callback function
		let callback = (*ctx.ctx).callbacks.catalog.find_table;

		// Allocate output buffer on stack
		let mut output = MaybeUninit::<TableFFI>::uninit();

		// Call FFI callback
		let result = callback(ctx.ctx, table_id.0, version.0, output.as_mut_ptr());

		match result {
			FFI_OK => {
				// Success - unmarshal table
				let ffi_table = output.assume_init();
				let table = unmarshal_table(&ffi_table)?;

				// Free FFI-allocated memory
				let free_callback = (*ctx.ctx).callbacks.catalog.free_table;
				free_callback(&mut output.as_mut_ptr().read());

				Ok(Some(table))
			}
			FFI_NOT_FOUND => Ok(None), // Not found
			_ => Err(FFIError::Other("Failed to find table".to_string())),
		}
	}
}

/// Find table by name
pub(super) fn raw_catalog_find_table_by_name(
	ctx: &OperatorContext,
	namespace_id: NamespaceId,
	name: &str,
	version: CommitVersion,
) -> Result<Option<TableDef>, FFIError> {
	unsafe {
		// Get callback function
		let callback = (*ctx.ctx).callbacks.catalog.find_table_by_name;

		// Prepare name bytes
		let name_bytes = name.as_bytes();

		// Allocate output buffer on stack
		let mut output = MaybeUninit::<TableFFI>::uninit();

		// Call FFI callback
		let result = callback(
			ctx.ctx,
			namespace_id.0,
			name_bytes.as_ptr(),
			name_bytes.len(),
			version.0,
			output.as_mut_ptr(),
		);

		match result {
			FFI_OK => {
				// Success - unmarshal table
				let ffi_table = output.assume_init();
				let table = unmarshal_table(&ffi_table)?;

				// Free FFI-allocated memory
				let free_callback = (*ctx.ctx).callbacks.catalog.free_table;
				free_callback(&mut output.as_mut_ptr().read());

				Ok(Some(table))
			}
			FFI_NOT_FOUND => Ok(None), // Not found
			_ => Err(FFIError::Other("Failed to find table by name".to_string())),
		}
	}
}

/// Unmarshal NamespaceFFI to NamespaceDef
unsafe fn unmarshal_namespace(ffi_ns: &NamespaceFFI) -> Result<NamespaceDef, FFIError> {
	// Convert name BufferFFI to String
	let name_bytes = if !ffi_ns.name.ptr.is_null() && ffi_ns.name.len > 0 {
		unsafe { from_raw_parts(ffi_ns.name.ptr, ffi_ns.name.len) }
	} else {
		&[]
	};

	let name = std::str::from_utf8(name_bytes)
		.map_err(|_| FFIError::Other("Invalid UTF-8 in namespace name".to_string()))?
		.to_string();

	Ok(NamespaceDef {
		id: NamespaceId(ffi_ns.id),
		name,
	})
}

/// Unmarshal TableFFI to TableDef
unsafe fn unmarshal_table(ffi_table: &TableFFI) -> Result<TableDef, FFIError> {
	// Convert name BufferFFI to String
	let name_bytes = if !ffi_table.name.ptr.is_null() && ffi_table.name.len > 0 {
		unsafe { from_raw_parts(ffi_table.name.ptr, ffi_table.name.len) }
	} else {
		&[]
	};

	let name = std::str::from_utf8(name_bytes)
		.map_err(|_| FFIError::Other("Invalid UTF-8 in table name".to_string()))?
		.to_string();

	// Unmarshal columns
	let mut columns = Vec::with_capacity(ffi_table.column_count);
	if !ffi_table.columns.is_null() && ffi_table.column_count > 0 {
		let columns_slice = unsafe { from_raw_parts(ffi_table.columns, ffi_table.column_count) };
		for ffi_col in columns_slice {
			columns.push(unsafe { unmarshal_column(ffi_col)? });
		}
	}

	// Unmarshal primary key if present
	let primary_key = if ffi_table.has_primary_key != 0 && !ffi_table.primary_key.is_null() {
		unsafe { Some(unmarshal_primary_key(&*ffi_table.primary_key)?) }
	} else {
		None
	};

	Ok(TableDef {
		id: TableId(ffi_table.id),
		namespace: NamespaceId(ffi_table.namespace_id),
		name,
		columns,
		primary_key,
	})
}

/// Unmarshal ColumnDefFFI to ColumnDef
unsafe fn unmarshal_column(ffi_col: &ColumnDefFFI) -> Result<ColumnDef, FFIError> {
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
unsafe fn unmarshal_primary_key(ffi_pk: &PrimaryKeyFFI) -> Result<PrimaryKeyDef, FFIError> {
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
