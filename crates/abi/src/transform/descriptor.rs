// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use super::vtable::TransformVTableFFI;
use crate::data::buffer::BufferFFI;

/// Descriptor for an FFI transform
///
/// This structure describes a transform's metadata and provides
/// its virtual function table.
#[repr(C)]
pub struct TransformDescriptorFFI {
	/// API version (must match CURRENT_API)
	pub api: u32,

	/// Transform name (UTF-8 encoded)
	pub name: BufferFFI,

	/// Semantic version (UTF-8 encoded, e.g., "1.0.0")
	pub version: BufferFFI,

	/// Description (UTF-8 encoded)
	pub description: BufferFFI,

	/// Virtual function table with transform methods
	pub vtable: TransformVTableFFI,
}

// SAFETY: TransformDescriptorFFI contains pointers to static strings and functions
// which are safe to share across threads
unsafe impl Send for TransformDescriptorFFI {}
unsafe impl Sync for TransformDescriptorFFI {}
