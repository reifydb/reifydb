// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use super::{column::ColumnFFI, primary_key::PrimaryKeyFFI};
use crate::data::buffer::BufferFFI;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct TableFFI {
	pub id: u64,

	pub namespace_id: u64,

	pub name: BufferFFI,

	pub columns: *const ColumnFFI,

	pub column_count: usize,

	pub has_primary_key: u8,

	pub primary_key: *const PrimaryKeyFFI,
}
