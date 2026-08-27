// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod math;

use std::sync::Arc;

use reifydb_routine_abi::registry::RoutinesConfigurator;

pub fn default_in_process_monoids(builder: RoutinesConfigurator) -> RoutinesConfigurator {
	builder.register_builtin_monoid(Arc::new(math::sum::Sum::new()))
		.register_builtin_monoid(Arc::new(math::min::Min::new()))
		.register_builtin_monoid(Arc::new(math::max::Max::new()))
		.register_builtin_monoid(Arc::new(math::count::Count::new()))
}

#[cfg(test)]
mod tests;
