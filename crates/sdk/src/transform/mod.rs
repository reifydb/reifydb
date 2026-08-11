// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod context;
pub mod exports;
pub mod wrapper;

use crate::{
	config::Config, error::Result, operator::change::BorrowedColumns, transform::context::ExternCTransformContext,
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
