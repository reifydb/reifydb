// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::data::buffer::BufferFFI;

/// FFI-safe namespace definition
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct NamespaceFFI {
	/// Namespace ID (u64)
	pub id: u64,
	/// Namespace name (UTF-8 encoded)
	pub name: BufferFFI,
}
