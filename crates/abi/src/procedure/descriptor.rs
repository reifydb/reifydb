// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::vtable::ProcedureVTableFFI;
use crate::data::buffer::BufferFFI;

#[repr(C)]
pub struct ProcedureDescriptorFFI {
	pub api: u32,

	pub name: BufferFFI,

	pub version: BufferFFI,

	pub description: BufferFFI,

	pub vtable: ProcedureVTableFFI,
}

// SAFETY: ProcedureDescriptorFFI contains pointers to static strings and functions

unsafe impl Send for ProcedureDescriptorFFI {}
unsafe impl Sync for ProcedureDescriptorFFI {}
