// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// Logging callbacks
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LogCallbacks {
	pub message: unsafe extern "C" fn(operator_id: u64, level: u32, message: *const u8, message_len: usize),
}
