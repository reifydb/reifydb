// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Procedure traits and types for FFI procedure libraries

pub mod exports;
pub mod wrapper;

use std::collections::HashMap;

use reifydb_abi::{constants::FFI_OK, context::context::ContextFFI, data::buffer::BufferFFI};
use reifydb_core::value::column::columns::Columns;
use reifydb_type::{
	params::Params,
	value::{Value, frame::frame::Frame},
};

use crate::error::{FFIError, Result};

/// Static metadata about a procedure type
pub trait FFIProcedureMetadata {
	/// Procedure name (must be unique within a library)
	const NAME: &'static str;
	/// API version for FFI compatibility (must match host's CURRENT_API)
	const API: u32;
	/// Semantic version of the procedure (e.g., "1.0.0")
	const VERSION: &'static str;
	/// Human-readable description of the procedure
	const DESCRIPTION: &'static str;
}

/// Runtime procedure behavior
pub trait FFIProcedure: 'static {
	/// Create a new procedure instance with configuration
	fn new(config: &HashMap<String, Value>) -> Result<Self>
	where
		Self: Sized;

	/// Call the procedure with the given context and parameters
	fn call(&mut self, ctx: &FFIProcedureContext, params: Params) -> Result<Columns>;
}

pub trait FFIProcedureWithMetadata: FFIProcedure + FFIProcedureMetadata {}
impl<T> FFIProcedureWithMetadata for T where T: FFIProcedure + FFIProcedureMetadata {}

/// Context available to FFI procedures for executing RQL within the current transaction
pub struct FFIProcedureContext {
	pub(crate) ctx: *mut ContextFFI,
}

impl FFIProcedureContext {
	/// Create a new procedure context from an FFI context pointer
	pub fn new(ctx: *mut ContextFFI) -> Self {
		assert!(!ctx.is_null(), "ContextFFI pointer must not be null");
		Self {
			ctx,
		}
	}

	/// Execute an RQL statement within the current transaction
	pub fn rql(&self, rql: &str, params: Params) -> Result<Vec<Frame>> {
		raw_procedure_rql(self, rql, params)
	}
}

/// Execute an RQL statement through the host's RQL callback (procedure variant).
pub(crate) fn raw_procedure_rql(ctx: &FFIProcedureContext, rql: &str, params: Params) -> Result<Vec<Frame>> {
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
