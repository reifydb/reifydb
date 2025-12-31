// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Table FFI operations

use std::{mem::MaybeUninit, slice::from_raw_parts};

use reifydb_abi::{FFI_NOT_FOUND, FFI_OK, TableFFI};
use reifydb_core::{
	CommitVersion,
	interface::{NamespaceId, TableDef, TableId},
};

use super::{unmarshal_column, unmarshal_primary_key};
use crate::{FFIError, OperatorContext};

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
