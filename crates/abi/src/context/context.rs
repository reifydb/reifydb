// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

use crate::callbacks::host::HostCallbacks;

/// FFI context passed to operators containing transaction, operator ID, and callbacks
/// This struct is shared between the host and operators to provide complete execution context
#[repr(C)]
pub struct ContextFFI {
	/// Opaque pointer to the host's transaction data
	pub txn_ptr: *mut c_void,
	/// Operator ID for this operation
	pub operator_id: u64,
	/// Host callbacks for state and other operations
	pub callbacks: HostCallbacks,
}
