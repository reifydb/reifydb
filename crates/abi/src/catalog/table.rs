// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
