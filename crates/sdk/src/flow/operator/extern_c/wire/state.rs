// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub const EXTERN_C_GROUP_WIDTH: usize = 24;

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ExternCGroupId {
	pub bytes: [u8; EXTERN_C_GROUP_WIDTH],
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExternCStateSlice {
	pub ptr: *const u8,
	pub len: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExternCStateEntry {
	pub key: ExternCStateSlice,
	pub value: ExternCStateSlice,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct ExternCStateUsage {
	pub state_entries: u64,
	pub state_bytes: u64,
	pub row_number_entries: u64,
	pub row_number_bytes: u64,
}
