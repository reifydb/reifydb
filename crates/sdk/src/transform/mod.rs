// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod context;
pub mod exports;
pub mod wrapper;

use std::collections::HashMap;

use reifydb_type::value::Value;

use crate::{error::Result, operator::change::BorrowedColumns, transform::context::FFITransformContext};

pub trait FFITransformMetadata {
	/// Transform name (must be unique within a library)
	const NAME: &'static str;
	/// API version for FFI compatibility (must match host's CURRENT_API)
	const API: u32;
	/// Semantic version of the transform (e.g., "1.0.0")
	const VERSION: &'static str;
	/// Human-readable description of the transform
	const DESCRIPTION: &'static str;
}

pub trait FFITransform: 'static {
	fn new(config: &HashMap<String, Value>) -> Result<Self>
	where
		Self: Sized;

	/// Apply the transform.
	///
	/// `input` borrows native column storage; do not retain pointers past
	/// return. Emit output via `ctx.builder()` -- typically a single
	/// `emit_insert`, mirroring `FFIOperator::pull`.
	fn transform(&mut self, ctx: &mut FFITransformContext, input: BorrowedColumns<'_>) -> Result<()>;
}

pub trait FFITransformWithMetadata: FFITransform + FFITransformMetadata {}
impl<T> FFITransformWithMetadata for T where T: FFITransform + FFITransformMetadata {}
