// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Transform traits and types for FFI transform libraries

pub mod exports;
pub mod wrapper;

use std::collections::HashMap;

use reifydb_core::value::column::columns::Columns;
use reifydb_type::value::Value;

use crate::error::Result;

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

	fn transform(&mut self, input: Columns) -> Result<Columns>;
}

pub trait FFITransformWithMetadata: FFITransform + FFITransformMetadata {}
impl<T> FFITransformWithMetadata for T where T: FFITransform + FFITransformMetadata {}
