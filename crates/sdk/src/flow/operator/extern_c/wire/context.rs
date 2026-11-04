// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use crate::flow::operator::extern_c::wire::callbacks::OperatorCallbacks;

#[repr(C)]
pub struct ExternCContextRaw {
	pub txn_ptr: *mut c_void,

	pub batch_time_nanos: i64,

	pub operator_id: u64,

	pub callbacks: OperatorCallbacks,
}
