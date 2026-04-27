// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod identity;
pub mod subscription;
pub mod testing;

pub mod clock;
pub mod set;

use std::sync::Arc;

use crate::routine::registry::RoutinesConfigurator;

/// Register all built-in native procedures directly into a `Routines` builder.
pub fn default_native_procedures(builder: RoutinesConfigurator) -> RoutinesConfigurator {
	let builder = builder
		.register_procedure(Arc::new(set::config::SetConfigProcedure::new()))
		.register_procedure(Arc::new(clock::set::ClockSetProcedure::new()))
		.register_procedure(Arc::new(clock::advance::ClockAdvanceProcedure::new()))
		.register_procedure(Arc::new(identity::inject::IdentityInject::new()))
		.register_procedure(Arc::new(subscription::inspect::InspectSubscription::new()));
	testing::register_testing_native_procedures(builder)
}
