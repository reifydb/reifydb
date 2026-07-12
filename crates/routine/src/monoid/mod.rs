// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Built-in monoids: associative, commutative combine operations with an identity element,
//! registered alongside functions and procedures. Monoids power segment-tree summary nodes -
//! range aggregates are computed by folding `MonoidState` across storage order, range
//! decomposition pieces, and partitions, with no guaranteed global fold order. New aggregation
//! functions over new types can register their own monoid without touching this module.

pub mod math;

use std::sync::Arc;

use reifydb_value::value::{
	Value,
	value_type::{ValueType, input_types::InputTypes},
};
use serde::{Deserialize, Serialize};

use crate::routine::{RoutineInfo, error::RoutineError, registry::RoutinesConfigurator};

/// Folded state of a monoid over zero or more lifted values.
///
/// `count` distinguishes "no rows folded in" (the identity, count 0) from "rows folded in that
/// happen to combine to a zero-like value" - required for correct invert-to-empty and none
/// semantics. `compensation` is the running Neumaier compensation term; it is only meaningful
/// for `math::sum` over `Float8` and stays `0.0` (untouched) for every other monoid/type.
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

/// An associative, commutative combine operation with an identity element.
///
/// `combine` must be associative AND commutative, with `MonoidState::identity()` as the unit:
/// callers fold states in storage order (descending keys), across range decomposition pieces,
/// and across partitions in registry order - there is no global fold order.
///
/// `invert(total, part)` is the inverse of `combine` where cheaply possible; returning `None`
/// tells the caller to recompute from children instead (e.g. min/max when the removed value
/// equals the current extreme, or sum on arithmetic failure).
pub trait Monoid: Send + Sync {
	fn info(&self) -> &RoutineInfo;

	/// Accepted input value types; validated at CREATE.
	fn accepted_types(&self) -> InputTypes;

	/// The value type `finalize` produces for a given input column type.
	fn state_type(&self, input: ValueType) -> ValueType;

	/// Lift a single defined value into a one-element state. Callers must not call this
	/// with an undefined (`Value::None`) value - skip those before lifting.
	fn lift(&self, value: &Value) -> MonoidState;

	fn combine(&self, a: &MonoidState, b: &MonoidState) -> Result<MonoidState, RoutineError>;

	fn invert(&self, total: &MonoidState, part: &MonoidState) -> Option<MonoidState>;

	/// Map a folded state to its output value. Identity maps to `Value::none()` unless a
	/// monoid documents otherwise (e.g. `math::count` maps identity to `Uint8(0)`).
	fn finalize(&self, state: &MonoidState) -> Value;
}

pub fn default_native_monoids(builder: RoutinesConfigurator) -> RoutinesConfigurator {
	builder.register_builtin_monoid(Arc::new(math::sum::Sum::new()))
		.register_builtin_monoid(Arc::new(math::min::Min::new()))
		.register_builtin_monoid(Arc::new(math::max::Max::new()))
		.register_builtin_monoid(Arc::new(math::count::Count::new()))
}

#[cfg(test)]
mod tests;
