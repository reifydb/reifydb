// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
