// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Namespace FFI operations

use std::{mem::MaybeUninit, slice::from_raw_parts};

use reifydb_abi::{FFI_NOT_FOUND, FFI_OK, NamespaceFFI};
use reifydb_core::{
	CommitVersion,
	interface::{NamespaceDef, NamespaceId},
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
