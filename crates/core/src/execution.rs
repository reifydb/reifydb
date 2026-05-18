// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ops::Deref;

use reifydb_type::{error::Error, value::frame::frame::Frame};

use crate::metric::ExecutionMetrics;

#[derive(Debug)]
pub struct ExecutionResult {
	pub frames: Vec<Frame>,
	pub error: Option<Error>,
	pub metrics: ExecutionMetrics,
}

impl ExecutionResult {
	pub fn is_ok(&self) -> bool {
		self.error.is_none()
	}

	pub fn is_err(&self) -> bool {
		self.error.is_some()
	}

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
