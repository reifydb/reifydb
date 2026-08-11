// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::config::Config;

use crate::{
	error::Result, flow::operator::change::BorrowedColumns,
	transform::extern_c::binding::context::ExternCTransformContext,
};

pub trait ExternCTransformMetadata {
	const NAME: &'static str;

	const API: u32;

	const VERSION: &'static str;

	const DESCRIPTION: &'static str;
}

pub trait ExternCTransform: 'static {
	fn new(config: &Config) -> Result<Self>
	where
		Self: Sized;

	fn transform(&mut self, ctx: &mut ExternCTransformContext, input: BorrowedColumns<'_>) -> Result<()>;
}

pub trait ExternCTransformWithMetadata: ExternCTransform + ExternCTransformMetadata {}
impl<T> ExternCTransformWithMetadata for T where T: ExternCTransform + ExternCTransformMetadata {}
