// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::diff::ExternCDiff;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ExternCOrigin {
	pub origin: u8,
	pub id: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ExternCChange {
	pub origin: ExternCOrigin,

	pub diff_count: usize,

	pub diffs: *const ExternCDiff,

	pub version: u64,

	pub changed_at: u64,
}

impl ExternCChange {
	pub const fn empty() -> Self {
		Self {
			origin: ExternCOrigin {
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
