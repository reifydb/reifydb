// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PrimaryKeyFFI {
	pub id: u64,

	pub column_count: usize,

	pub column_ids: *const u64,
}
