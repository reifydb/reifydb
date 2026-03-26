// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! FFI-safe ABI types for source connectors

use core::ffi::c_void;

use crate::{
	data::{buffer::BufferFFI, column::ColumnsFFI},
	operator::column::OperatorColumnDefsFFI,
};

/// Function signature for the source magic number export
pub type SourceMagicFnFFI = extern "C" fn() -> u32;

/// Factory function type for creating source instances
pub type SourceCreateFnFFI = extern "C" fn(config: *const u8, config_len: usize) -> *mut c_void;

/// Descriptor for an FFI source connector
#[repr(C)]
pub struct SourceDescriptorFFI {
	/// API version
	pub api: u32,
	/// Connector name (UTF-8)
	pub name: BufferFFI,
	/// Semantic version (UTF-8)
	pub version: BufferFFI,
	/// Description (UTF-8)
	pub description: BufferFFI,
	/// 0 = Pull, 1 = Push
	pub mode: u8,
	/// Schema of records this source produces
	pub output_columns: OperatorColumnDefsFFI,
	/// Virtual function table
	pub vtable: SourceVTableFFI,
}

unsafe impl Send for SourceDescriptorFFI {}
unsafe impl Sync for SourceDescriptorFFI {}

/// Virtual function table for FFI source connectors
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SourceVTableFFI {
	/// Pull mode: fetch next batch of records
	///
	/// # Parameters
	/// - `instance`: Source instance pointer
	/// - `checkpoint`: Last checkpoint bytes (may be null)
	/// - `checkpoint_len`: Checkpoint length (0 if null)
	/// - `output`: Output columns (to be filled)
	/// - `out_checkpoint`: Output checkpoint buffer (to be filled)
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub poll: extern "C" fn(
		instance: *mut c_void,
		checkpoint: *const u8,
		checkpoint_len: usize,
		output: *mut ColumnsFFI,
		out_checkpoint: *mut BufferFFI,
	) -> i32,

	/// Push mode: run continuously
	///
	/// # Parameters
	/// - `instance`: Source instance pointer
	/// - `checkpoint`: Last checkpoint bytes (may be null)
	/// - `checkpoint_len`: Checkpoint length (0 if null)
	/// - `emit_ctx`: Opaque context for emit callback
	/// - `emit_fn`: Callback to emit a batch
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub run: extern "C" fn(
		instance: *mut c_void,
		checkpoint: *const u8,
		checkpoint_len: usize,
		emit_ctx: *mut c_void,
		emit_fn: extern "C" fn(
			ctx: *mut c_void,
			columns: *const ColumnsFFI,
			checkpoint: *const BufferFFI,
		) -> i32,
	) -> i32,

	/// Destroy the source instance
	pub destroy: extern "C" fn(instance: *mut c_void),
}
