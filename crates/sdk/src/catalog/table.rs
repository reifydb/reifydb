// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{mem::MaybeUninit, slice::from_raw_parts, str};

use reifydb_abi::{
	catalog::table::TableFFI,
	constants::{FFI_NOT_FOUND, FFI_OK},
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{NamespaceId, TableId},
		table::Table,
	},
};

use super::{unmarshal_column, unmarshal_primary_key};
use crate::{error::FFIError, operator::context::OperatorContext};

pub(super) fn raw_catalog_find_table(
	ctx: &OperatorContext,
	table_id: TableId,
	version: CommitVersion,
) -> Result<Option<Table>, FFIError> {
	unsafe {
		let callback = (*ctx.ctx).callbacks.catalog.find_table;

		let mut output = MaybeUninit::<TableFFI>::uninit();

		let result = callback(ctx.ctx, table_id.0, version.0, output.as_mut_ptr());

		match result {
			FFI_OK => {
				let ffi_table = output.assume_init();
				let table = unmarshal_table(&ffi_table)?;

				let free_callback = (*ctx.ctx).callbacks.catalog.free_table;
				free_callback(&mut output.as_mut_ptr().read());

				Ok(Some(table))
			}
			FFI_NOT_FOUND => Ok(None),
			_ => Err(FFIError::Other("Failed to find table".to_string())),
		}
	}
}

pub(super) fn raw_catalog_find_table_by_name(
	ctx: &OperatorContext,
	namespace_id: NamespaceId,
	name: &str,
	version: CommitVersion,
) -> Result<Option<Table>, FFIError> {
	unsafe {
		let callback = (*ctx.ctx).callbacks.catalog.find_table_by_name;

		let name_bytes = name.as_bytes();

		let mut output = MaybeUninit::<TableFFI>::uninit();

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
				let ffi_table = output.assume_init();
				let table = unmarshal_table(&ffi_table)?;

				let free_callback = (*ctx.ctx).callbacks.catalog.free_table;
				free_callback(&mut output.as_mut_ptr().read());

				Ok(Some(table))
			}
			FFI_NOT_FOUND => Ok(None),
			_ => Err(FFIError::Other("Failed to find table by name".to_string())),
		}
	}
}

unsafe fn unmarshal_table(ffi_table: &TableFFI) -> Result<Table, FFIError> {
	let name_bytes = if !ffi_table.name.ptr.is_null() && ffi_table.name.len > 0 {
		unsafe { from_raw_parts(ffi_table.name.ptr, ffi_table.name.len) }
	} else {
		&[]
	};

	let name = str::from_utf8(name_bytes)
		.map_err(|_| FFIError::Other("Invalid UTF-8 in table name".to_string()))?
		.to_string();

	let mut columns = Vec::with_capacity(ffi_table.column_count);
	if !ffi_table.columns.is_null() && ffi_table.column_count > 0 {
		let columns_slice = unsafe { from_raw_parts(ffi_table.columns, ffi_table.column_count) };
		for ffi_col in columns_slice {
			columns.push(unsafe { unmarshal_column(ffi_col)? });
		}
	}

	let primary_key = if ffi_table.has_primary_key != 0 && !ffi_table.primary_key.is_null() {
		unsafe { Some(unmarshal_primary_key(&*ffi_table.primary_key)?) }
	} else {
		None
	};

	Ok(Table {
		id: TableId(ffi_table.id),
		namespace: NamespaceId(ffi_table.namespace_id),
		name,
		columns,
		primary_key,
		underlying: false,
	})
}
