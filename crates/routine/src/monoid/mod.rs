// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Built-in monoids, registered alongside functions and procedures, backing segment-tree summary nodes.

pub mod math;

use std::sync::Arc;

use reifydb_routine_abi::registry::RoutinesConfigurator;

pub fn default_native_monoids(builder: RoutinesConfigurator) -> RoutinesConfigurator {
	builder.register_builtin_monoid(Arc::new(math::sum::Sum::new()))
		.register_builtin_monoid(Arc::new(math::min::Min::new()))
		.register_builtin_monoid(Arc::new(math::max::Max::new()))
		.register_builtin_monoid(Arc::new(math::count::Count::new()))
}

#[cfg(test)]
mod tests;
