// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::atomic::{AtomicU64, Ordering};

/// Generate a unique request ID
pub(crate) fn generate_request_id() -> String {
	static COUNTER: AtomicU64 = AtomicU64::new(0);

	let count = COUNTER.fetch_add(1, Ordering::Relaxed);
	let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();

	format!("{}-{}", timestamp, count)
}
