// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use reifydb_codec::{frame::decode::decode_frames, value::encode_params};
use reifydb_value::{params::Params, value::frame::frame::Frame};

use crate::{
	common::extern_c::wire::{buffer::ExternCBuffer, callbacks::rql::RqlCallbacks, status::EXTERN_C_OK},
	error::{Result, SdkError},
};

pub(crate) fn raw_query(ctx: *mut c_void, callbacks: RqlCallbacks, query: &str, params: Params) -> Result<Vec<Frame>> {
	let params_bytes = encode_params(&params)
		.map_err(|e| SdkError::Serialization(format!("failed to serialize params: {}", e)))?;

	let mut output = ExternCBuffer::empty();

	// SAFETY: the caller passes the non-null context pointer the host keeps valid for the whole guest call;
	// query and params_bytes outlive the callback. Discharges ExternCBuffer::as_slice: the host leaves output
	// either empty or pointing at a live host allocation of output.len bytes that nothing here frees.
	unsafe {
		let result = (callbacks.rql)(
			ctx,
			query.as_ptr(),
			query.len(),
			params_bytes.as_ptr(),
			params_bytes.len(),
			&mut output,
		);

		if result == EXTERN_C_OK {
			let result_bytes = output.as_slice();
			let frames: Vec<Frame> = decode_frames(result_bytes)
				.map_err(|e| SdkError::Serialization(format!("failed to deserialize result: {}", e)))?;
			Ok(frames)
		} else {
			let msg = if !output.is_empty() {
				String::from_utf8_lossy(output.as_slice()).into_owned()
			} else {
				format!("host_rql failed with code {}", result)
			};
			Err(SdkError::Other(msg))
		}
	}
}
