// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

use crate::{context::context::ContextFFI, flow::change::ChangeFFI};

/// Virtual function table for FFI operators.
///
/// **Zero-copy ABI.** The host hands the guest a `*const ChangeFFI` whose
/// `BufferFFI` fields point directly at native column storage (`cap == 0`
/// borrow sentinel). The guest must read input only during the call and
/// must not retain pointers, write through them, or free them. Output is
/// emitted via `ctx.callbacks.builder` - the guest acquires column buffers
/// from the host pool, fills them in place, and emits diffs via
/// `emit_diff`. There is no host-owned `output: ChangeFFI` parameter.
///
/// The `pull` path returns its result via the same builder mechanism (one
/// emitted diff whose `post` columns are the fetched rows).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct OperatorVTableFFI {
	/// Apply the operator to a borrowed input `Change`. Output is written
	/// via the builder callbacks on `ctx`.
	///
	/// # Parameters
	/// - `instance`: The operator instance pointer
	/// - `ctx`: FFI context (provides `callbacks.builder` for output)
	/// - `input`: Borrowed input change. Pointers inside are borrowed from native storage and valid only for the
	///   duration of the call.
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub apply: unsafe extern "C" fn(instance: *mut c_void, ctx: *mut ContextFFI, input: *const ChangeFFI) -> i32,

	/// Pull specific rows by their row numbers. Output columns are
	/// emitted via the builder callbacks - the host reads the first
	/// emitted diff's `post` columns as the result.
	///
	/// # Parameters
	/// - `instance`: The operator instance pointer
	/// - `ctx`: FFI context
	/// - `row_numbers`: Array of row numbers to fetch
	/// - `count`: Number of row numbers
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub pull: unsafe extern "C" fn(
		instance: *mut c_void,
		ctx: *mut ContextFFI,
		row_numbers: *const u64,
		count: usize,
	) -> i32,

	/// Periodic tick for time-based maintenance.
	///
	/// # Parameters
	/// - `instance`: The operator instance pointer
	/// - `ctx`: FFI context (provides `callbacks.builder` for output)
	/// - `timestamp_nanos`: Current timestamp as nanoseconds since Unix epoch
	///
	/// # Returns
	/// - 0 on success with output emitted via builder callbacks
	/// - 1 on success without output (no-op)
	/// - negative on error
	pub tick: unsafe extern "C" fn(instance: *mut c_void, ctx: *mut ContextFFI, timestamp_nanos: u64) -> i32,

	/// Destroy an operator instance and free its resources
	///
	/// # Parameters
	/// - `instance`: The operator instance pointer to destroy
	///
	/// # Safety
	/// - The instance pointer must have been created by this operator's create function
	/// - The instance must not be used after calling destroy
	/// - This function must be called exactly once per instance
	pub destroy: unsafe extern "C" fn(instance: *mut c_void),

	/// Flush any state mutations the operator buffered during this txn.
	///
	/// Called once per txn at commit time, after the last `apply`/`pull`/
	/// `tick` call. The guest is expected to drain its `StateCache` dirty
	/// list and write each entry through the host's state callbacks
	/// (`ctx.callbacks.state.set` / `.remove`). If the operator has no
	/// state, this is a no-op.
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub flush_state: unsafe extern "C" fn(instance: *mut c_void, ctx: *mut ContextFFI) -> i32,
}
