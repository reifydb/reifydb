// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::diff::DiffFFI;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct OriginFFI {
	pub origin: u8,
	pub id: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ChangeFFI {
	pub origin: OriginFFI,

	pub diff_count: usize,

	pub diffs: *const DiffFFI,

	pub version: u64,

	pub changed_at: u64,
}

impl ChangeFFI {
	pub const fn empty() -> Self {
		Self {
			origin: OriginFFI {
				origin: 0,
				id: 0,
			},
			diff_count: 0,
			diffs: core::ptr::null(),
			version: 0,
			changed_at: 0,
		}
	}
}
