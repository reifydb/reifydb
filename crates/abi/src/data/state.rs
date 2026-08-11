// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
