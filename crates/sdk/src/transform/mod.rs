// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod context;
pub mod exports;
pub mod wrapper;

use std::collections::HashMap;

use reifydb_type::value::Value;

use crate::{error::Result, operator::change::BorrowedColumns, transform::context::FFITransformContext};

pub trait FFITransformMetadata {
	const NAME: &'static str;

	const API: u32;

	const VERSION: &'static str;

	const DESCRIPTION: &'static str;
}

pub trait FFITransform: 'static {
	fn new(config: &HashMap<String, Value>) -> Result<Self>
	where
		Self: Sized;

	fn transform(&mut self, ctx: &mut FFITransformContext, input: BorrowedColumns<'_>) -> Result<()>;
}

pub trait FFITransformWithMetadata: FFITransform + FFITransformMetadata {}
impl<T> FFITransformWithMetadata for T where T: FFITransform + FFITransformMetadata {}
