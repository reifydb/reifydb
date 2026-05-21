// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct KeyRefFFI {
	pub ptr: *const u8,

	pub len: usize,
}
