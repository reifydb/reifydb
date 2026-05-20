// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
