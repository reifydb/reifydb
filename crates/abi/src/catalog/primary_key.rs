// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PrimaryKeyFFI {
	pub id: u64,

	pub column_count: usize,

	pub column_ids: *const u64,
}
