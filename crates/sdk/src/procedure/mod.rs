// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub mod exports;
pub mod wrapper;

use std::collections::HashMap;

use postcard::{from_bytes, to_stdvec};
use reifydb_abi::{constants::FFI_OK, context::context::ContextFFI, data::buffer::BufferFFI};
use reifydb_type::{
	params::Params,
	value::{Value, frame::frame::Frame},
};

use crate::{
	error::{Result, SdkError},
	operator::builder::ColumnsBuilder,
};

pub trait FFIProcedureMetadata {
	const NAME: &'static str;

	const API: u32;

	const VERSION: &'static str;

	const DESCRIPTION: &'static str;
}

pub trait FFIProcedure: 'static {
	fn new(config: &HashMap<String, Value>) -> Result<Self>
	where
		Self: Sized;

	fn call(&mut self, ctx: &mut FFIProcedureContext, params: Params) -> Result<()>;
}

pub trait FFIProcedureWithMetadata: FFIProcedure + FFIProcedureMetadata {}
impl<T> FFIProcedureWithMetadata for T where T: FFIProcedure + FFIProcedureMetadata {}

pub struct FFIProcedureContext {
	pub(crate) ctx: *mut ContextFFI,
}

impl FFIProcedureContext {
	pub fn new(ctx: *mut ContextFFI) -> Self {
		assert!(!ctx.is_null(), "ContextFFI pointer must not be null");
		Self {
			ctx,
		}
	}

	pub fn query(&self, query: &str, params: Params) -> Result<Vec<Frame>> {
		raw_procedure_query(self, query, params)
	}

	pub fn builder(&mut self) -> ColumnsBuilder<'_> {
		ColumnsBuilder::from_raw_ctx(self.ctx)
	}
}

pub(crate) fn raw_procedure_query(ctx: &FFIProcedureContext, query: &str, params: Params) -> Result<Vec<Frame>> {
	let params_bytes = to_stdvec(&params)
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
			let frames: Vec<Frame> = from_bytes(result_bytes)
				.map_err(|e| SdkError::Serialization(format!("failed to deserialize result: {}", e)))?;
			Ok(frames)
		} else {
			Err(SdkError::Other(format!("host_rql failed with code {}", result)))
		}
	}
}
