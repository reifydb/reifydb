// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_value::value::duration::Duration;

/// Invoke a reconnection lifecycle hook if one is configured.
pub(crate) fn fire(callback: &Option<Arc<dyn Fn() + Send + Sync>>) {
	if let Some(callback) = callback {
		callback();
	}
}

/// Exponential backoff in milliseconds for a 1-based `attempt`, capped at 30s.
pub(crate) fn backoff_millis(base_ms: u64, attempt: u32) -> u64 {
	let factor = 1u64.checked_shl(attempt.saturating_sub(1).min(20)).unwrap_or(u64::MAX);
	base_ms.saturating_mul(factor).min(30_000)
}

#[allow(clippy::disallowed_types)]
pub(crate) fn millis_to_std(ms: u64) -> std::time::Duration {
	Duration::from_milliseconds(ms.min(i64::MAX as u64) as i64).unwrap().to_std()
}
