// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{catalog::row_shape::ExternCRowShape, context::context::ExternCContext};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RowShapeCallbacks {
	pub find_row_shape: extern "C" fn(ctx: *mut ExternCContext, fingerprint: u64, output: *mut ExternCRowShape) -> i32,

	pub free_row_shape: extern "C" fn(row_shape: *mut ExternCRowShape),
}
