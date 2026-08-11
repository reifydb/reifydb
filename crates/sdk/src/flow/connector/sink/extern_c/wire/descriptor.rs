// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	common::extern_c::wire::buffer::ExternCBuffer,
	flow::{
		connector::sink::extern_c::wire::vtable::ExternCSinkVTable,
		extern_c::wire::schema::ExternCOperatorColumns,
	},
};

#[repr(C)]
pub struct ExternCSinkDescriptor {
	pub api: u32,

	pub name: ExternCBuffer,

	pub version: ExternCBuffer,

	pub description: ExternCBuffer,

	pub input_columns: ExternCOperatorColumns,

	pub vtable: ExternCSinkVTable,
}

// SAFETY: every pointer in the descriptor addresses immutable module-static data (strings, symbols).
unsafe impl Send for ExternCSinkDescriptor {}
unsafe impl Sync for ExternCSinkDescriptor {}
