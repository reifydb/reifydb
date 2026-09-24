// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::vtable::ExternCOperatorVTable;
use crate::{common::extern_c::wire::buffer::ExternCBuffer, flow::extern_c::wire::schema::ExternCOperatorColumns};

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ExternCWindowRequirements {
	pub takes_window: u8,

	pub kinds: u32,

	pub domain: u8,

	pub needs_pane: u8,

	pub throttles: u8,
}

#[repr(C)]
pub struct ExternCOperatorDescriptor {
	pub abi_tag: u32,

	pub operator: ExternCBuffer,

	pub version: ExternCBuffer,

	pub description: ExternCBuffer,

	pub input_columns: ExternCOperatorColumns,

	pub output_columns: ExternCOperatorColumns,

	pub capabilities: u32,

	pub class: u8,

	pub unmanaged_because: ExternCBuffer,

	pub window: ExternCWindowRequirements,

	pub vtable: ExternCOperatorVTable,
}

// SAFETY: every pointer in the descriptor addresses immutable module-static data (strings, symbols).
unsafe impl Send for ExternCOperatorDescriptor {}
unsafe impl Sync for ExternCOperatorDescriptor {}
