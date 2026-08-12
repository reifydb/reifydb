// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use reifydb_codec::{frame::decode::decode_frames, value::encode_params};
use reifydb_value::{params::Params, value::frame::frame::Frame};

use crate::{
	common::extern_c::{
		binding::builder::ColumnsBuilder,
		wire::{buffer::ExternCBuffer, status::EXTERN_C_OK},
	},
	error::{Result, SdkError},
	procedure::extern_c::wire::context::ExternCContextRaw,
};

pub struct ExternCProcedureContext {
	pub(crate) ctx: *mut ExternCContextRaw,
}

impl ExternCProcedureContext {
	pub fn new(ctx: *mut ExternCContextRaw) -> Self {
		assert!(!ctx.is_null(), "ExternCContextRaw pointer must not be null");
		Self {
			ctx,
		}
	}

	pub fn query(&self, query: &str, params: Params) -> Result<Vec<Frame>> {
		raw_procedure_query(self, query, params)
	}

	pub fn builder(&mut self) -> ColumnsBuilder<'_> {
		// SAFETY: `new` asserts `self.ctx` is non-null and the host keeps the context alive for the whole
		// call it handed the pointer to.
		unsafe {
			ColumnsBuilder::new(
				self.ctx as *mut c_void,
				(*self.ctx).callbacks.builder,
				(*self.ctx).written_at_nanos,
			)
		}
	}
}

pub(crate) fn raw_procedure_query(ctx: &ExternCProcedureContext, query: &str, params: Params) -> Result<Vec<Frame>> {
	let params_bytes = encode_params(&params)
		.map_err(|e| SdkError::Serialization(format!("failed to serialize params: {}", e)))?;

	let mut output = ExternCBuffer::empty();

	// SAFETY: ExternCProcedureContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw
	// valid for the whole procedure call; query and params_bytes outlive the callback. Discharges
	// ExternCBuffer::as_slice: the host leaves output either empty or pointing at a live host allocation of
	// output.len bytes that nothing here frees.
	unsafe {
		let result = ((*ctx.ctx).callbacks.rql.rql)(
			ctx.ctx as *mut c_void,
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
			Err(SdkError::Other(format!("host_rql failed with code {}", result)))
		}
	}
}
