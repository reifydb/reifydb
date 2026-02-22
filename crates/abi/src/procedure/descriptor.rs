// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use super::vtable::ProcedureVTableFFI;
use crate::data::buffer::BufferFFI;

/// Descriptor for an FFI procedure
///
/// This structure describes a procedure's metadata and provides
/// its virtual function table.
#[repr(C)]
pub struct ProcedureDescriptorFFI {
	/// API version (must match CURRENT_API)
	pub api: u32,

	/// Procedure name (UTF-8 encoded)
	pub name: BufferFFI,

	/// Semantic version (UTF-8 encoded, e.g., "1.0.0")
	pub version: BufferFFI,

	/// Description (UTF-8 encoded)
	pub description: BufferFFI,

	/// Virtual function table with procedure methods
	pub vtable: ProcedureVTableFFI,
}

// SAFETY: ProcedureDescriptorFFI contains pointers to static strings and functions
// which are safe to share across threads
unsafe impl Send for ProcedureDescriptorFFI {}
unsafe impl Sync for ProcedureDescriptorFFI {}
