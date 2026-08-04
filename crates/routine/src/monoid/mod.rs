// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Built-in monoids, registered alongside functions and procedures, backing segment-tree summary nodes.
//!
//! Range aggregates fold `MonoidState` across storage order, range decomposition pieces and partitions, with no
//! guaranteed global fold order.

pub mod math;

use std::sync::Arc;

use reifydb_value::value::{Value, value_type::input_types::InputTypes};
use serde::{Deserialize, Serialize};

use crate::routine::{RoutineInfo, error::RoutineError, registry::RoutinesConfigurator};

/// `count` separates "no rows folded in" (the identity) from "rows that combine to a zero-like value", which
/// invert-to-empty and none semantics depend on. `compensation` is the Neumaier term, meaningful only for
/// `math::sum` over `Float8` and left at `0.0` everywhere else.
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

/// `combine` must be associative AND commutative with `MonoidState::identity()` as the unit, because callers fold
/// in storage order, across range decomposition pieces and across partitions with no global order.
/// `invert` returning `None` tells the caller to recompute from children instead.
pub trait Monoid: Send + Sync {
	fn info(&self) -> &RoutineInfo;

	/// Accepted input value types; validated at CREATE.
	fn accepted_types(&self) -> InputTypes;

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
