// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	Value,
	value_type::{ValueType, input_types::InputTypes},
};

use crate::{
	monoid::{Monoid, MonoidState},
	routine::{RoutineInfo, error::RoutineError},
};

pub struct Count {
	info: RoutineInfo,
}

impl Default for Count {
	fn default() -> Self {
		Self::new()
	}
}

impl Count {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::count"),
		}
	}
}

impl Monoid for Count {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn accepted_types(&self) -> InputTypes {
		InputTypes::any()
	}

	fn state_type(&self, _input: ValueType) -> ValueType {
		ValueType::Uint8
	}

	fn lift(&self, _value: &Value) -> MonoidState {
		MonoidState {
			value: Value::Uint8(1),
			count: 1,
			compensation: 0.0,
		}
	}

	fn combine(&self, a: &MonoidState, b: &MonoidState) -> Result<MonoidState, RoutineError> {
		if a.is_identity() {
			return Ok(b.clone());
		}
		if b.is_identity() {
			return Ok(a.clone());
		}
		let count = a.count + b.count;
		Ok(MonoidState {
			value: Value::Uint8(count),
			count,
			compensation: 0.0,
		})
	}

	fn invert(&self, total: &MonoidState, part: &MonoidState) -> Option<MonoidState> {
		if part.is_identity() {
			return Some(total.clone());
		}
		let count = total.count.saturating_sub(part.count);
		if count == 0 {
			return Some(MonoidState::identity());
		}
		Some(MonoidState {
			value: Value::Uint8(count),
			count,
			compensation: 0.0,
		})
	}

	fn finalize(&self, state: &MonoidState) -> Value {
		Value::Uint8(state.count)
	}
}
