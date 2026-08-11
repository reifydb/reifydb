// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	common::extern_c::wire::buffer::ExternCBuffer,
	flow::{
		connector::source::extern_c::wire::vtable::ExternCSourceVTable,
		extern_c::wire::schema::ExternCOperatorColumns,
	},
};

#[repr(C)]
pub struct ExternCSourceDescriptor {
	pub api: u32,

	pub name: ExternCBuffer,

	pub version: ExternCBuffer,

	pub description: ExternCBuffer,

	pub mode: u8,

	pub output_columns: ExternCOperatorColumns,

	pub vtable: ExternCSourceVTable,
}

// SAFETY: every pointer in the descriptor addresses immutable module-static data (strings, symbols).
unsafe impl Send for ExternCSourceDescriptor {}
unsafe impl Sync for ExternCSourceDescriptor {}
