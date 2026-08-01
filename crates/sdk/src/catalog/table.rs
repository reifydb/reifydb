// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem::MaybeUninit, slice::from_raw_parts, str};

use reifydb_abi::{
	catalog::table::TableFFI,
	constants::{FFI_NOT_FOUND, FFI_OK},
};
use reifydb_core::{
	common::{CommitVersion, TimeSource},
	interface::catalog::{
		id::{NamespaceId, TableId},
		table::Table,
	},
};

use super::{unmarshal_column, unmarshal_primary_key};
use crate::{error::SdkError, operator::context::ffi::FFIOperatorContext};

pub(super) fn raw_catalog_find_table(
	ctx: &FFIOperatorContext,
	table_id: TableId,
	version: CommitVersion,
) -> Result<Option<Table>, SdkError> {
	// SAFETY: `FFIOperatorContext::new` asserts `ctx.ctx` is non-null and the host keeps the ContextFFI
	// alive for the call; on FFI_OK the host has written a fully initialised TableFFI into `output` whose
	// buffers stay live until `free_table`, discharging `unmarshal_table`.
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
			_ => Err(SdkError::Other("Failed to find table".to_string())),
		}
	}
}

pub(super) fn raw_catalog_find_table_by_name(
	ctx: &FFIOperatorContext,
	namespace_id: NamespaceId,
	name: &str,
	version: CommitVersion,
) -> Result<Option<Table>, SdkError> {
	// SAFETY: `FFIOperatorContext::new` asserts `ctx.ctx` is non-null and `name` outlives the call; on
	// FFI_OK the host has written a fully initialised TableFFI into `output` whose buffers stay live
	// until `free_table`, discharging `unmarshal_table`.
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
			_ => Err(SdkError::Other("Failed to find table by name".to_string())),
		}
	}
}

/// # Safety
///
/// `ffi_table.name.ptr` must be null or valid for reads of `ffi_table.name.len`
/// initialised bytes; `ffi_table.columns` must be null or valid for reads of
/// `ffi_table.column_count` initialised, aligned `ColumnFFI`; and when
/// `has_primary_key` is non-zero, `primary_key` must be null or point to one
/// initialised, aligned `PrimaryKeyFFI`. All must outlive the call, and each
/// pointed-to struct must itself satisfy the contract of its unmarshaller.
unsafe fn unmarshal_table(ffi_table: &TableFFI) -> Result<Table, SdkError> {
	let name_bytes = if !ffi_table.name.ptr.is_null() && ffi_table.name.len > 0 {
		// SAFETY: discharges this function's own contract; the branch above established that
		// `name.ptr` is non-null and `name.len` is non-zero.
		unsafe { from_raw_parts(ffi_table.name.ptr, ffi_table.name.len) }
	} else {
		&[]
	};

	let name = str::from_utf8(name_bytes)
		.map_err(|_| SdkError::Other("Invalid UTF-8 in table name".to_string()))?
		.to_string();

	let mut columns = Vec::with_capacity(ffi_table.column_count);
	if !ffi_table.columns.is_null() && ffi_table.column_count > 0 {
		// SAFETY: discharges this function's own contract; the branch above established that `columns`
		// is non-null and `column_count` is non-zero.
		let columns_slice = unsafe { from_raw_parts(ffi_table.columns, ffi_table.column_count) };
		for ffi_col in columns_slice {
			// SAFETY: this function's contract requires every element of `columns` to satisfy
			// `unmarshal_column`.
			columns.push(unsafe { unmarshal_column(ffi_col)? });
		}
	}

	let primary_key = if ffi_table.has_primary_key != 0 && !ffi_table.primary_key.is_null() {
		// SAFETY: discharges this function's own contract; the branch above established that
		// `has_primary_key` is set and `primary_key` is non-null.
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
		partition_by: vec![],
		underlying: false,
		time: TimeSource::Processing,
	})
}
