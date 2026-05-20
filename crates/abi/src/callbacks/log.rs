// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#[repr(C)]
#[derive(Clone, Copy)]
pub struct LogCallbacks {
	pub message: unsafe extern "C" fn(operator_id: u64, level: u32, message: *const u8, message_len: usize),
}
