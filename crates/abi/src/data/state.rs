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
	pub has_membership: u64,
	pub membership_entries: u64,
	pub membership_bytes: u64,
	pub has_completeness: u64,
	pub values_complete: u64,
	pub membership_complete: u64,
	pub absences_served: u64,
	pub false_positives: u64,
	pub revocations: u64,
	pub has_pool: u64,
	pub pool_budget: u64,
	pub pool_evictions: u64,
}
