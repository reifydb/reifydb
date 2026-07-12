// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	fragment::Fragment,
	value::{
		Value,
		number::safe::{add::SafeAdd, sub::SafeSub},
		value_type::{ValueType, input_types::InputTypes},
	},
};

use crate::{
	monoid::{Monoid, MonoidState},
	routine::{RoutineInfo, error::RoutineError},
};

pub struct Sum {
	info: RoutineInfo,
}

impl Default for Sum {
	fn default() -> Self {
		Self::new()
	}
}

impl Sum {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::sum"),
		}
	}
}

impl Monoid for Sum {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn accepted_types(&self) -> InputTypes {
		InputTypes::numeric()
	}

	fn state_type(&self, input: ValueType) -> ValueType {
		match input {
			ValueType::Int1 | ValueType::Int2 | ValueType::Int4 | ValueType::Int8 | ValueType::Int16 => {
				ValueType::Int16
			}
			ValueType::Uint1
			| ValueType::Uint2
			| ValueType::Uint4
			| ValueType::Uint8
			| ValueType::Uint16 => ValueType::Uint16,
			ValueType::Float4 | ValueType::Float8 => ValueType::Float8,
			other => other,
		}
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

		if let (Value::Float8(av), Value::Float8(bv)) = (&a.value, &b.value) {
			let mut compensation = a.compensation;
			let mut sum = neumaier(av.value(), &mut compensation, bv.value());
			sum = neumaier(sum, &mut compensation, b.compensation);
			if !sum.is_finite() {
				return Err(overflow_error(&self.info, &a.value, &b.value));
			}
			return Ok(MonoidState {
				value: Value::float8(sum),
				count: a.count + b.count,
				compensation,
			});
		}

		let value =
			a.value.checked_add(&b.value).ok_or_else(|| overflow_error(&self.info, &a.value, &b.value))?;
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

		if let (Value::Float8(tv), Value::Float8(pv)) = (&total.value, &part.value) {
			let mut compensation = total.compensation;
			let mut sum = neumaier(tv.value(), &mut compensation, -pv.value());
			sum = neumaier(sum, &mut compensation, -part.compensation);
			if !sum.is_finite() {
				return None;
			}
			return Some(MonoidState {
				value: Value::float8(sum),
				count,
				compensation,
			});
		}

		let value = total.value.checked_sub(&part.value)?;
		Some(MonoidState {
			value,
			count,
			compensation: 0.0,
		})
	}

	fn finalize(&self, state: &MonoidState) -> Value {
		if state.is_identity() {
			return Value::none();
		}
		match &state.value {
			Value::Float8(v) => Value::float8(v.value() + state.compensation),
			other => other.clone(),
		}
	}
}

fn overflow_error(info: &RoutineInfo, a: &Value, b: &Value) -> RoutineError {
	RoutineError::FunctionExecutionFailed {
		function: Fragment::internal(info.name.clone()),
		reason: format!("math::sum overflow combining {:?} and {:?}", a, b),
	}
}

fn neumaier(sum: f64, compensation: &mut f64, x: f64) -> f64 {
	let t = sum + x;
	if sum.abs() >= x.abs() {
		*compensation += (sum - t) + x;
	} else {
		*compensation += (x - t) + sum;
	}
	t
}
