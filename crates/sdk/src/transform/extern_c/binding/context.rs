// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use crate::{
	common::extern_c::binding::builder::ColumnsBuilder, transform::extern_c::wire::context::ExternCContextRaw,
};

pub struct ExternCTransformContext {
	pub(crate) ctx: *mut ExternCContextRaw,
}

impl ExternCTransformContext {
	pub fn new(ctx: *mut ExternCContextRaw) -> Self {
		assert!(!ctx.is_null(), "ExternCContextRaw pointer must not be null");
		Self {
			ctx,
		}
	}

	pub fn builder(&mut self) -> ColumnsBuilder<'_> {
		// SAFETY: `self.ctx` is non-null, checked in `new`, and the host keeps the context alive for
		// the whole call it handed the pointer to.
		unsafe {
			ColumnsBuilder::new(
				self.ctx as *mut c_void,
				(*self.ctx).callbacks.builder,
				(*self.ctx).written_at_nanos,
			)
		}
	}
}
