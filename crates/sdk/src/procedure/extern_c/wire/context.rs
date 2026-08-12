// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use crate::procedure::extern_c::wire::callbacks::ProcedureCallbacks;

#[repr(C)]
pub struct ExternCContextRaw {
	pub txn_ptr: *mut c_void,

	pub executor_ptr: *const c_void,

	pub written_at_nanos: u64,

	pub callbacks: ProcedureCallbacks,
}
