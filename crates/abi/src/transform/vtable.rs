// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use crate::{context::context::ExternCContext, data::column::ExternCColumns};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExternCTransformVTable {
	pub transform: unsafe extern "C" fn(
		instance: *mut c_void,
		ctx: *mut ExternCContext,
		input: *const ExternCColumns,
	) -> i32,

	pub destroy: unsafe extern "C" fn(instance: *mut c_void),
}
