// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use crate::callbacks::host::HostCallbacks;

#[repr(C)]
pub struct ContextFFI {
	pub txn_ptr: *mut c_void,

	pub executor_ptr: *const c_void,

	pub operator_id: u64,

	pub clock_now_nanos: u64,

	pub callbacks: HostCallbacks,
}
