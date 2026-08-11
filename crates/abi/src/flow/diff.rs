// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::data::column::ExternCColumns;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffType {
	Insert = 1,

	Update = 2,

	Remove = 3,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ExternCDiff {
	pub diff_type: DiffType,

	pub pre: ExternCColumns,

	pub post: ExternCColumns,
}
