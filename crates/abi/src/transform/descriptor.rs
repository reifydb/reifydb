// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::vtable::TransformVTableFFI;
use crate::data::buffer::BufferFFI;

#[repr(C)]
pub struct TransformDescriptorFFI {
	pub api: u32,

	pub name: BufferFFI,

	pub version: BufferFFI,

	pub description: BufferFFI,

	pub vtable: TransformVTableFFI,
}

// SAFETY: TransformDescriptorFFI contains pointers to static strings and functions

unsafe impl Send for TransformDescriptorFFI {}
unsafe impl Sync for TransformDescriptorFFI {}
