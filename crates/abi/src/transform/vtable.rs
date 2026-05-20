// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use crate::{context::context::ContextFFI, data::column::ColumnsFFI};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct TransformVTableFFI {
	pub transform:
		unsafe extern "C" fn(instance: *mut c_void, ctx: *mut ContextFFI, input: *const ColumnsFFI) -> i32,

	pub destroy: unsafe extern "C" fn(instance: *mut c_void),
}
