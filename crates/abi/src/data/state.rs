// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[repr(C)]
#[derive(Clone, Copy)]
pub struct StateSliceFFI {
	pub ptr: *const u8,
	pub len: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct StateEntryFFI {
	pub key: StateSliceFFI,
	pub value: StateSliceFFI,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct StateUsageFFI {
	pub state_entries: u64,
	pub state_bytes: u64,
	pub row_number_entries: u64,
	pub row_number_bytes: u64,
}
