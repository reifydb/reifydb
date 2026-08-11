// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::vtable::ExternCProcedureVTable;
use crate::common::extern_c::wire::buffer::ExternCBuffer;

#[repr(C)]
pub struct ExternCProcedureDescriptor {
	pub api: u32,

	pub name: ExternCBuffer,

	pub version: ExternCBuffer,

	pub description: ExternCBuffer,

	pub vtable: ExternCProcedureVTable,
}

// SAFETY: every pointer in the descriptor addresses immutable module-static data (strings, symbols).
unsafe impl Send for ExternCProcedureDescriptor {}
unsafe impl Sync for ExternCProcedureDescriptor {}
