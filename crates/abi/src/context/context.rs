// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

use crate::callbacks::host::HostCallbacks;

/// FFI context passed to operators containing transaction, operator ID, and callbacks
/// This struct is shared between the host and operators to provide complete execution context
#[repr(C)]
pub struct ContextFFI {
	/// Opaque pointer to the host's transaction data
	pub txn_ptr: *mut c_void,
	/// Opaque pointer to the host's Executor (for RQL execution)
	pub executor_ptr: *const c_void,
	/// Operator ID for this operation
	pub operator_id: u64,
	/// Clock nanoseconds-since-epoch captured by the host before the vtable
	/// call. Used by builder callbacks to stamp system columns on emitted
	/// diffs. Stable for the duration of one vtable invocation; the host
	/// refreshes it per call (or per txn for cached-ctx call sites where
	/// the txn's logical clock is stable across calls within the txn).
	pub clock_now_nanos: u64,
	/// Host callbacks for state and other operations
	pub callbacks: HostCallbacks,
}
