// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::slice;

use tracing::{debug, error, info, trace, warn};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn host_log_message(operator_id: u64, level: u32, message: *const u8, message_len: usize) {
	if message.is_null() {
		return;
	}

	let msg_str = unsafe {
		let bytes = slice::from_raw_parts(message, message_len);
		String::from_utf8_lossy(bytes)
	};

	match level {
		0 => trace!("FFI Operator[{}]: {}", operator_id, msg_str),
		1 => debug!("FFI Operator[{}]: {}", operator_id, msg_str),
		2 => info!("FFI Operator[{}]: {}", operator_id, msg_str),
		3 => warn!("FFI Operator[{}]: {}", operator_id, msg_str),
		4 => error!("FFI Operator[{}]: {}", operator_id, msg_str),
		_ => info!("FFI Operator[{}]: {}", operator_id, msg_str),
	}
}
