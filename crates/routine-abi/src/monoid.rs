// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{Value, value_type::input_types::InputTypes};
use serde::{Deserialize, Serialize};

use crate::{RoutineInfo, error::RoutineError};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MonoidState {
	pub value: Value,
	pub count: u64,
	pub compensation: f64,
}

impl MonoidState {
	pub fn identity() -> Self {
		Self {
			value: Value::none(),
			count: 0,
			compensation: 0.0,
		}
	}

	pub fn is_identity(&self) -> bool {
		self.count == 0
	}
}

pub trait Monoid: Send + Sync {
	fn info(&self) -> &RoutineInfo;

	fn accepted_types(&self) -> InputTypes;

	fn lift(&self, value: &Value) -> MonoidState;

	fn combine(&self, a: &MonoidState, b: &MonoidState) -> Result<MonoidState, RoutineError>;

	fn invert(&self, total: &MonoidState, part: &MonoidState) -> Option<MonoidState>;

	fn finalize(&self, state: &MonoidState) -> Value;
}
