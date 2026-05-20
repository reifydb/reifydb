// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use crate::data::buffer::BufferFFI;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct NamespaceFFI {
	pub id: u64,

	pub name: BufferFFI,

	pub parent_id: u64,
}
