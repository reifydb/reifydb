// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::vtable::ExternCOperatorVTable;
use crate::{common::extern_c::wire::buffer::ExternCBuffer, flow::extern_c::wire::schema::ExternCOperatorColumns};

#[repr(C)]
pub struct ExternCOperatorDescriptor {
	pub api: u32,

	pub abi_tag: u32,

	pub operator: ExternCBuffer,

	pub version: ExternCBuffer,

	pub description: ExternCBuffer,

	pub input_columns: ExternCOperatorColumns,

	pub output_columns: ExternCOperatorColumns,

	pub capabilities: u32,

	pub vtable: ExternCOperatorVTable,
}

// SAFETY: every pointer in the descriptor addresses immutable module-static data (strings, symbols).
unsafe impl Send for ExternCOperatorDescriptor {}
unsafe impl Sync for ExternCOperatorDescriptor {}
