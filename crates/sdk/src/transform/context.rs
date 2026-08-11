// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::context::context::ExternCContext;

use crate::operator::builder::ColumnsBuilder;

pub struct ExternCTransformContext {
	pub(crate) ctx: *mut ExternCContext,
}

impl ExternCTransformContext {
	pub fn new(ctx: *mut ExternCContext) -> Self {
		assert!(!ctx.is_null(), "ExternCContext pointer must not be null");
		Self {
			ctx,
		}
	}

	pub fn builder(&mut self) -> ColumnsBuilder<'_> {
		ColumnsBuilder::from_raw_ctx(self.ctx)
	}
}
