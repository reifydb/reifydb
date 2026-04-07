// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ops::Deref;

use reifydb_type::value::frame::frame::Frame;

use crate::metric::ExecutionMetrics;

/// Result of executing one or more RQL statements.
///
/// Derefs to `[Frame]` so callers can index, iterate, or check `.is_empty()`
/// directly while still accessing `.metrics` for telemetry.
#[derive(Debug)]
pub struct ExecutionResult {
	pub frames: Vec<Frame>,
	pub metrics: ExecutionMetrics,
}

impl Deref for ExecutionResult {
	type Target = [Frame];

	fn deref(&self) -> &[Frame] {
		&self.frames
	}
}
