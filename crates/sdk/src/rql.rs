// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::{constants::FFI_OK, data::buffer::BufferFFI};
use reifydb_codec::{frame::decode::decode_frames, value::encode_params};
use reifydb_value::{params::Params, value::frame::frame::Frame};

use crate::{
	error::{Result, SdkError},
	operator::context::ffi::FFIOperatorContext,
};

pub(crate) fn raw_query(ctx: &FFIOperatorContext, query: &str, params: Params) -> Result<Vec<Frame>> {
	let params_bytes = encode_params(&params)
		.map_err(|e| SdkError::Serialization(format!("failed to serialize params: {}", e)))?;

	let mut output = BufferFFI::empty();

	unsafe {
		let result = ((*ctx.ctx).callbacks.rql.rql)(
			ctx.ctx,
			query.as_ptr(),
			query.len(),
			params_bytes.as_ptr(),
			params_bytes.len(),
			&mut output,
		);

		if result == FFI_OK {
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
