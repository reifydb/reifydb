// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{slice, str};

use reifydb_sdk::common::{api::CURRENT_API, extern_c::wire::buffer::ExternCBuffer};

use crate::error::ExtensionError;

/// Invalid UTF-8 yields a placeholder rather than an error, so this never fails a load.
///
/// # Safety
/// `buffer.ptr` must be valid for reads of `buffer.len` bytes for the duration of the call.
pub unsafe fn buffer_to_string(buffer: &ExternCBuffer) -> String {
	if buffer.ptr.is_null() || buffer.len == 0 {
		return String::new();
	}
	// SAFETY: ptr is non-null with non-zero len here, and the caller guarantees the range is readable.
	let slice = unsafe { slice::from_raw_parts(buffer.ptr, buffer.len) };
	str::from_utf8(slice).unwrap_or("<invalid UTF-8>").to_string()
}

pub fn validate_api_version(api: u32) -> Result<(), ExtensionError> {
	if api != CURRENT_API {
		return Err(ExtensionError::ApiVersionMismatch {
			expected: CURRENT_API,
			actual: api,
		});
	}
	Ok(())
}
