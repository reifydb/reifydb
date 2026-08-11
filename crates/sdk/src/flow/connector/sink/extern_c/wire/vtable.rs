// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use crate::flow::connector::sink::extern_c::wire::record::ExternCSinkRecord;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExternCSinkVTable {
	pub write: extern "C" fn(instance: *mut c_void, records: *const ExternCSinkRecord, count: usize) -> i32,

	pub destroy: extern "C" fn(instance: *mut c_void),
}
