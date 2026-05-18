// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

use crate::{context::context::ContextFFI, data::column::ColumnsFFI};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct TransformVTableFFI {
	pub transform:
		unsafe extern "C" fn(instance: *mut c_void, ctx: *mut ContextFFI, input: *const ColumnsFFI) -> i32,

	pub destroy: unsafe extern "C" fn(instance: *mut c_void),
}
