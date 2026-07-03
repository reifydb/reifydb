// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FFITypeConstraint {
	pub base_type: u8,

	pub constraint_type: u8,

	pub constraint_param1: u32,

	pub constraint_param2: u32,
}
