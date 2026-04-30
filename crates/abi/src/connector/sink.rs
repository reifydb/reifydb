// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

use crate::{
	data::{buffer::BufferFFI, column::ColumnsFFI},
	operator::column::OperatorColumnsFFI,
};

/// Function signature for the sink magic number export
pub type SinkMagicFnFFI = extern "C" fn() -> u32;

/// Factory function type for creating sink instances
pub type SinkCreateFnFFI = extern "C" fn(config: *const u8, config_len: usize) -> *mut c_void;

/// Descriptor for an FFI sink connector
#[repr(C)]
pub struct SinkDescriptorFFI {
	/// API version
	pub api: u32,
	/// Connector name (UTF-8)
	pub name: BufferFFI,
	/// Semantic version (UTF-8)
	pub version: BufferFFI,
	/// Description (UTF-8)
	pub description: BufferFFI,
	/// Shape of records this sink accepts
	pub input_columns: OperatorColumnsFFI,
	/// Virtual function table
	pub vtable: SinkVTableFFI,
}

unsafe impl Send for SinkDescriptorFFI {}
unsafe impl Sync for SinkDescriptorFFI {}

/// Virtual function table for FFI sink connectors
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SinkVTableFFI {
	/// Write a batch of records to the external system
	///
	/// # Parameters
	/// - `instance`: Sink instance pointer
	/// - `records`: Array of record FFI structs
	/// - `count`: Number of records
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub write: extern "C" fn(instance: *mut c_void, records: *const SinkRecordFFI, count: usize) -> i32,

	/// Destroy the sink instance
	pub destroy: extern "C" fn(instance: *mut c_void),
}

/// FFI-safe representation of a sink record
#[repr(C)]
pub struct SinkRecordFFI {
	/// 1 = Insert, 2 = Update, 3 = Remove
	pub op: u8,
	/// The columnar data
	pub columns: ColumnsFFI,
}
