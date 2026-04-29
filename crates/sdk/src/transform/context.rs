// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Guest-side transform context.
//!
//! Mirrors `OperatorContext` but for the simpler transform call path:
//! holds a raw `*mut ContextFFI`, exposes a `builder()` accessor for
//! zero-copy output emission via the host's `BuilderCallbacks`.

use reifydb_abi::context::context::ContextFFI;

use crate::operator::builder::ColumnsBuilder;

/// Context passed to `FFITransform::transform`. Pinned to the call frame.
pub struct FFITransformContext {
	pub(crate) ctx: *mut ContextFFI,
}

impl FFITransformContext {
	/// # Safety
	/// `ctx` must be non-null and valid for the duration of the FFI call.
	pub fn new(ctx: *mut ContextFFI) -> Self {
		assert!(!ctx.is_null(), "ContextFFI pointer must not be null");
		Self {
			ctx,
		}
	}

	/// Acquire a `ColumnsBuilder` for emitting output columns directly into
	/// host-pool-owned buffers. The builder borrows this context for the
	/// duration of the FFI call.
	pub fn builder(&mut self) -> ColumnsBuilder<'_> {
		ColumnsBuilder::from_raw_ctx(self.ctx)
	}
}
