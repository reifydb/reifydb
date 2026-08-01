// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem::MaybeUninit, slice::from_raw_parts, str};

use reifydb_abi::{
	catalog::namespace::NamespaceFFI,
	constants::{FFI_NOT_FOUND, FFI_OK},
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{id::NamespaceId, namespace::Namespace},
};

use crate::{error::SdkError, operator::context::ffi::FFIOperatorContext};

pub(super) fn raw_catalog_find_namespace(
	ctx: &FFIOperatorContext,
	namespace_id: NamespaceId,
	version: CommitVersion,
) -> Result<Option<Namespace>, SdkError> {
	// SAFETY: `FFIOperatorContext::new` asserts `ctx.ctx` is non-null and the host keeps the ContextFFI
	// alive for the call; on FFI_OK the host has written a fully initialised NamespaceFFI into `output`
	// whose name buffer stays live until `free_namespace`, discharging `unmarshal_namespace`.
	unsafe {
		let callback = (*ctx.ctx).callbacks.catalog.find_namespace;

		let mut output = MaybeUninit::<NamespaceFFI>::uninit();

		let result = callback(ctx.ctx, namespace_id.0, version.0, output.as_mut_ptr());

		match result {
			FFI_OK => {
				let ffi_ns = output.assume_init();
				let namespace = unmarshal_namespace(&ffi_ns)?;

				let free_callback = (*ctx.ctx).callbacks.catalog.free_namespace;
				free_callback(&mut output.as_mut_ptr().read());

				Ok(Some(namespace))
			}
			FFI_NOT_FOUND => Ok(None),
			_ => Err(SdkError::Other("Failed to find namespace".to_string())),
		}
	}
}

pub(super) fn raw_catalog_find_namespace_by_name(
	ctx: &FFIOperatorContext,
	name: &str,
	version: CommitVersion,
) -> Result<Option<Namespace>, SdkError> {
	// SAFETY: `FFIOperatorContext::new` asserts `ctx.ctx` is non-null and `name` outlives the call; on
	// FFI_OK the host has written a fully initialised NamespaceFFI into `output` whose name buffer stays
	// live until `free_namespace`, discharging `unmarshal_namespace`.
	unsafe {
		let callback = (*ctx.ctx).callbacks.catalog.find_namespace_by_name;

		let name_bytes = name.as_bytes();

		let mut output = MaybeUninit::<NamespaceFFI>::uninit();

		let result = callback(ctx.ctx, name_bytes.as_ptr(), name_bytes.len(), version.0, output.as_mut_ptr());

		match result {
			FFI_OK => {
				let ffi_ns = output.assume_init();
				let namespace = unmarshal_namespace(&ffi_ns)?;

				let free_callback = (*ctx.ctx).callbacks.catalog.free_namespace;
				free_callback(&mut output.as_mut_ptr().read());

				Ok(Some(namespace))
			}
			FFI_NOT_FOUND => Ok(None),
			_ => Err(SdkError::Other("Failed to find namespace by name".to_string())),
		}
	}
}

/// # Safety
///
/// `ffi_ns.name.ptr` must be null or valid for reads of `ffi_ns.name.len`
/// initialised bytes for the duration of the call.
unsafe fn unmarshal_namespace(ffi_ns: &NamespaceFFI) -> Result<Namespace, SdkError> {
	let name_bytes = if !ffi_ns.name.ptr.is_null() && ffi_ns.name.len > 0 {
		// SAFETY: discharges this function's own contract; the branch above established that
		// `name.ptr` is non-null and `name.len` is non-zero.
		unsafe { from_raw_parts(ffi_ns.name.ptr, ffi_ns.name.len) }
	} else {
		&[]
	};

	let name = str::from_utf8(name_bytes)
		.map_err(|_| SdkError::Other("Invalid UTF-8 in namespace name".to_string()))?
		.to_string();

	let local_name = name.rsplit_once("::").map(|(_, s)| s).unwrap_or(&name).to_string();

	Ok(Namespace::Local {
		id: NamespaceId(ffi_ns.id),
		name,
		local_name,
		parent_id: NamespaceId(ffi_ns.parent_id),
	})
}
