// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_abi::context::context::ContextFFI;

use crate::operator::builder::ColumnsBuilder;

pub struct FFITransformContext {
	pub(crate) ctx: *mut ContextFFI,
}

impl FFITransformContext {
	pub fn new(ctx: *mut ContextFFI) -> Self {
		assert!(!ctx.is_null(), "ContextFFI pointer must not be null");
		Self {
			ctx,
		}
	}

	pub fn builder(&mut self) -> ColumnsBuilder<'_> {
		ColumnsBuilder::from_raw_ctx(self.ctx)
	}
}
