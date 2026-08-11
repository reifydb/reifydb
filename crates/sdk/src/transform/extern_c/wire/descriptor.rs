// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::vtable::ExternCTransformVTable;
use crate::common::extern_c::wire::buffer::ExternCBuffer;

#[repr(C)]
pub struct ExternCTransformDescriptor {
	pub api: u32,

	pub name: ExternCBuffer,

	pub version: ExternCBuffer,

	pub description: ExternCBuffer,

	pub vtable: ExternCTransformVTable,
}

// SAFETY: every pointer in the descriptor addresses immutable module-static data (strings, symbols).
unsafe impl Send for ExternCTransformDescriptor {}
unsafe impl Sync for ExternCTransformDescriptor {}
