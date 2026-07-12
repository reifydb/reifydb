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

pub struct Max {
	info: RoutineInfo,
}

impl Default for Max {
	fn default() -> Self {
		Self::new()
	}
}

impl Max {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::max"),
		}
	}
}

impl Monoid for Max {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn accepted_types(&self) -> InputTypes {
		InputTypes::single(vec![
			ValueType::Int1,
			ValueType::Int2,
			ValueType::Int4,
			ValueType::Int8,
			ValueType::Int16,
			ValueType::Uint1,
			ValueType::Uint2,
			ValueType::Uint4,
			ValueType::Uint8,
			ValueType::Uint16,
			ValueType::Float4,
			ValueType::Float8,
			ValueType::Int,
			ValueType::Uint,
			ValueType::Decimal,
			ValueType::Date,
			ValueType::DateTime,
			ValueType::Time,
			ValueType::Duration,
		])
	}

	fn state_type(&self, input: ValueType) -> ValueType {
		input
	}

	fn lift(&self, value: &Value) -> MonoidState {
		MonoidState {
			value: value.clone(),
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
		let value = if a.value >= b.value {
			a.value.clone()
		} else {
			b.value.clone()
		};
		Ok(MonoidState {
			value,
			count: a.count + b.count,
			compensation: 0.0,
		})
	}

	fn invert(&self, total: &MonoidState, part: &MonoidState) -> Option<MonoidState> {
		if part.is_identity() {
			return Some(total.clone());
		}
		let count = total.count.checked_sub(part.count)?;
		if count == 0 {
			return Some(MonoidState::identity());
		}
		if part.value != total.value {
			Some(MonoidState {
				value: total.value.clone(),
				count,
				compensation: 0.0,
			})
		} else {
			None
		}
	}

	fn finalize(&self, state: &MonoidState) -> Value {
		if state.is_identity() {
			Value::none()
		} else {
			state.value.clone()
		}
	}
}
