// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Output column-buffer builder callbacks.
//!
//! Guests use these to allocate output column buffers from the host's pool,
//! write data directly into the buffers' raw storage, and commit ownership
//! back to the host - all without copying.
//!
//! Lifecycle:
//! 1. `acquire(ctx, type_code, capacity)` -> opaque `*mut ColumnBufferHandle`.
//! 2. `data_ptr(handle)` returns a writable byte pointer; for var-len types, `offsets_ptr(handle)` returns a writable
//!    u64 pointer.
//! 3. `grow(handle, additional)` resizes underlying storage. Pointers may be invalidated; the guest must re-fetch them.
//! 4. `bitvec_ptr(handle)` returns a writable byte pointer for the defined bitmap; lazily allocated on first access.
//! 5. `commit(handle, written_count)` transfers ownership to the host. The handle is invalid after this call.
//! 6. `release(handle)` discards an unused builder (e.g. on guest error).
//!
//! Generation counters live on the host side: every callback verifies the
//! handle's generation matches before dereferencing. Use-after-commit aborts
//! in debug builds.

use core::ffi::c_void;

use crate::{context::context::ContextFFI, data::column::ColumnTypeCode};

/// Opaque handle representing a host-pool-acquired column builder.
/// The guest treats this as `*mut c_void`; only the host knows the layout.
pub type ColumnBufferHandle = c_void;

/// Diff variant for `emit_diff`: matches `Diff::{Insert, Update, Remove}`.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmitDiffKind {
	Insert = 0,
	Update = 1,
	Remove = 2,
}

/// Output builder callbacks. Each entry is a non-null function pointer.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BuilderCallbacks {
	/// Acquire a column buffer of the given type from the host pool.
	///
	/// `capacity` is in elements for fixed-size types and a byte hint for
	/// var-len types' data buffer.
	///
	/// Returns a non-null handle on success, null on failure.
	pub acquire: unsafe extern "C" fn(
		ctx: *mut ContextFFI,
		type_code: ColumnTypeCode,
		capacity: usize,
	) -> *mut ColumnBufferHandle,

	/// Get a writable byte pointer into the buffer's data region.
	/// For fixed-size types: `capacity * sizeof(T)` bytes available.
	/// For var-len types: returns the data byte buffer.
	///
	/// Returns null if the handle is invalid.
	pub data_ptr: unsafe extern "C" fn(handle: *mut ColumnBufferHandle) -> *mut u8,

	/// Get a writable u64 pointer into the offsets region (var-len only).
	/// Returns null for fixed-size types.
	pub offsets_ptr: unsafe extern "C" fn(handle: *mut ColumnBufferHandle) -> *mut u64,

	/// Get a writable byte pointer into the defined-bitvec region. The
	/// bitmap is lazily allocated on first call. Subsequent calls return
	/// the same pointer.
	///
	/// Returns null if the handle is invalid.
	pub bitvec_ptr: unsafe extern "C" fn(handle: *mut ColumnBufferHandle) -> *mut u8,

	/// Grow the buffer by `additional` elements (or bytes for var-len data).
	/// May relocate underlying storage; subsequent `data_ptr` /
	/// `offsets_ptr` / `bitvec_ptr` calls must be re-fetched.
	///
	/// Returns 0 on success, negative on failure.
	pub grow: unsafe extern "C" fn(handle: *mut ColumnBufferHandle, additional: usize) -> i32,

	/// Commit a built buffer; `written_count` is the final element count
	/// (or byte count for var-len data). Host adopts the buffer as a
	/// native ColumnBuffer. Handle is invalid after this call.
	///
	/// Returns 0 on success, negative on failure.
	pub commit: unsafe extern "C" fn(handle: *mut ColumnBufferHandle, written_count: usize) -> i32,

	/// Release a builder without committing (e.g. on guest error). Handle
	/// is invalid after this call.
	pub release: unsafe extern "C" fn(handle: *mut ColumnBufferHandle),

	/// Emit a diff into the host's per-call accumulator. The guest passes
	/// the (already-committed) `pre` and/or `post` column handles
	/// alongside their column names.
	///
	/// - `kind`: Insert / Update / Remove.
	/// - `pre_handles_ptr`/`pre_count`: array of committed handles for the diff's `pre` columns. Empty for Insert.
	/// - `pre_name_ptrs` / `pre_name_lens`: arrays of length `pre_count` pointing at the UTF-8 name bytes
	///   (borrowed; valid for the call).
	/// - `pre_row_count`: number of rows in the `pre` columns.
	/// - `pre_row_numbers_ptr` / `pre_row_numbers_len`: borrowed array of u64 row numbers, one per row in
	///   `pre_row_count`. `pre_row_numbers_len` MUST equal `pre_row_count`. The same row number on a subsequent
	///   emit is the operator's signal to the materialiser to upsert the existing row. Borrowed for the duration
	///   of this call only (`cap == 0` borrow sentinel; the host copies the values immediately).
	/// - `post_handles_ptr`/`post_count`/`post_name_ptrs`/`post_name_lens`/ `post_row_count`: same for `post`.
	///   Empty for Remove.
	/// - `post_row_numbers_ptr` / `post_row_numbers_len`: borrowed array of u64 row numbers, one per row in
	///   `post_row_count`. `post_row_numbers_len` MUST equal `post_row_count`.
	///
	/// Handles passed here must already have been `commit`-ed. Reusing a
	/// committed handle anywhere else after this call aborts in debug.
	///
	/// Returns 0 on success, negative on failure.
	pub emit_diff: unsafe extern "C" fn(
		ctx: *mut ContextFFI,
		kind: EmitDiffKind,
		pre_handles_ptr: *const *mut ColumnBufferHandle,
		pre_name_ptrs: *const *const u8,
		pre_name_lens: *const usize,
		pre_count: usize,
		pre_row_count: usize,
		pre_row_numbers_ptr: *const u64,
		pre_row_numbers_len: usize,
		post_handles_ptr: *const *mut ColumnBufferHandle,
		post_name_ptrs: *const *const u8,
		post_name_lens: *const usize,
		post_count: usize,
		post_row_count: usize,
		post_row_numbers_ptr: *const u64,
		post_row_numbers_len: usize,
	) -> i32,
}
