// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Deref;

use reifydb_value::{
	error::Error,
	value::{frame::frame::Frame, system_columns::SystemColumn},
};

use crate::{internal_err, metrics::execution::ExecutionMetrics};

#[derive(Debug)]
pub struct ExecutionResult {
	pub frames: Vec<Frame>,
	pub named_system_columns: Vec<Vec<SystemColumn>>,
	pub error: Option<Error>,
	pub metrics: ExecutionMetrics,
}

impl ExecutionResult {
	pub fn from_error(error: Error) -> Self {
		Self {
			frames: vec![],
			named_system_columns: vec![],
			error: Some(error),
			metrics: ExecutionMetrics::default(),
		}
	}

	pub fn strip_unnamed_system_columns(&mut self) -> Result<(), Error> {
		if self.frames.len() != self.named_system_columns.len() {
			return internal_err!(
				"{} frames but {} named system column sets; every frame needs exactly one",
				self.frames.len(),
				self.named_system_columns.len()
			);
		}
		for (frame, named) in self.frames.iter_mut().zip(&self.named_system_columns) {
			frame.system.keep_row_numbers_and(named);
		}
		Ok(())
	}

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
