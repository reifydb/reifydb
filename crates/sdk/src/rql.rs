// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Guest-side RQL execution via FFI callbacks

use reifydb_abi::{constants::FFI_OK, data::buffer::BufferFFI};
use reifydb_type::{params::Params, value::frame::frame::Frame};

use crate::{
	error::{FFIError, Result},
	operator::context::OperatorContext,
};

/// Execute an RQL statement through the host's RQL callback.
pub(crate) fn raw_rql(ctx: &OperatorContext, rql: &str, params: Params) -> Result<Vec<Frame>> {
	let params_bytes = postcard::to_stdvec(&params)
		.map_err(|e| FFIError::Serialization(format!("failed to serialize params: {}", e)))?;

	let mut output = BufferFFI::empty();

	unsafe {
		let result = ((*ctx.ctx).callbacks.rql.rql)(
			ctx.ctx,
			rql.as_ptr(),
			rql.len(),
			params_bytes.as_ptr(),
			params_bytes.len(),
			&mut output,
		);

		if result == FFI_OK {
			let result_bytes = output.as_slice();
			let frames: Vec<Frame> = postcard::from_bytes(result_bytes)
				.map_err(|e| FFIError::Serialization(format!("failed to deserialize result: {}", e)))?;
			Ok(frames)
		} else {
			Err(FFIError::Other(format!("host_rql failed with code {}", result)))
		}
	}
}
