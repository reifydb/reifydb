// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ExternCKeyRef {
	pub ptr: *const u8,

	pub len: usize,
}
