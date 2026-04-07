// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ops::Deref;

use reifydb_type::{error::Error, value::frame::frame::Frame};

use crate::metric::ExecutionMetrics;

/// Result of executing one or more RQL statements.
///
/// Metrics and frames are **always** present — even when execution fails.
/// On failure, `error` holds the cause while `frames` contains partial output
/// (whatever succeeded before the failure) and `metrics` holds partial telemetry.
///
/// Derefs to `[Frame]` so callers can index, iterate, or check `.is_empty()`
/// directly while still accessing `.metrics` for telemetry.
#[derive(Debug)]
pub struct ExecutionResult {
	pub frames: Vec<Frame>,
	pub error: Option<Error>,
	pub metrics: ExecutionMetrics,
}

impl ExecutionResult {
	/// Returns `true` if the execution completed without error.
	pub fn is_ok(&self) -> bool {
		self.error.is_none()
	}

	/// Returns `true` if the execution failed.
	pub fn is_err(&self) -> bool {
		self.error.is_some()
	}

	/// Convert to a `Result`, enabling the `?` operator.
	///
	/// Returns `Ok(self)` when there is no error, `Err(e)` otherwise.
	/// The `Ok` variant retains full access to frames and metrics.
	pub fn check(self) -> Result<Self, Error> {
		match self.error {
			Some(e) => Err(e),
			None => Ok(self),
		}
	}
}

impl Deref for ExecutionResult {
	type Target = [Frame];

	fn deref(&self) -> &[Frame] {
		&self.frames
	}
}
