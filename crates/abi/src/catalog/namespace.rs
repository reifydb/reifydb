// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::data::buffer::BufferFFI;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct NamespaceFFI {
	pub id: u64,

	pub name: BufferFFI,

	pub parent_id: u64,
}
