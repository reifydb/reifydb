// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Logging callbacks for FFI operators
//!
//! Allows FFI operators to emit log messages at various severity levels.

use tracing::{debug, error, info, trace, warn};

/// Log a message from an FFI operator
///
/// # Parameters
/// - `operator_id`: The operator ID for identifying the source
/// - `level`: Log level (0=trace, 1=debug, 2=info, 3=warn, 4=error)
/// - `message`: Message bytes (not null-terminated)
/// - `message_len`: Length of message in bytes
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_log_message(operator_id: u64, level: u32, message: *const u8, message_len: usize) {
	if message.is_null() {
		return;
	}

	// Convert message to string using provided length
	let msg_str = unsafe {
		let bytes = std::slice::from_raw_parts(message, message_len);
		String::from_utf8_lossy(bytes)
	};

	// Log based on level
	match level {
		0 => trace!("FFI Operator[{}]: {}", operator_id, msg_str),
		1 => debug!("FFI Operator[{}]: {}", operator_id, msg_str),
		2 => info!("FFI Operator[{}]: {}", operator_id, msg_str),
		3 => warn!("FFI Operator[{}]: {}", operator_id, msg_str),
		4 => error!("FFI Operator[{}]: {}", operator_id, msg_str),
		_ => info!("FFI Operator[{}]: {}", operator_id, msg_str),
	}
}
