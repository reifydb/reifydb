// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::config::ExtensionParams;

use crate::{
	error::SdkError, flow::operator::change::BorrowedColumns,
	transform::extern_c::binding::context::ExternCTransformContext,
};

pub trait ExternCTransformMetadata {
	const NAME: &'static str;

	const VERSION: &'static str;

	const DESCRIPTION: &'static str;
}

pub trait ExternCTransform: 'static {
	fn new(params: &ExtensionParams) -> Result<Self, SdkError>
	where
		Self: Sized;

	fn transform(&mut self, ctx: &mut ExternCTransformContext, input: BorrowedColumns<'_>) -> Result<(), SdkError>;
}

pub trait ExternCTransformWithMetadata: ExternCTransform + ExternCTransformMetadata {}
impl<T> ExternCTransformWithMetadata for T where T: ExternCTransform + ExternCTransformMetadata {}
