// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Built-in procedures: imperative routines users invoke as named statements. They may mutate catalog or storage
//! state, take typed argument lists, and return zero or more result columns. Identity management, subscription
//! control, set operations, clock manipulation in tests - anything that does not fit cleanly into a function
//! goes here.
//!
//! `default_native_procedures` is the registration entry point boot uses to install the workspace's built-ins;
//! extensions add their own through the same `RoutinesConfigurator`.

pub mod identity;
pub mod subscription;
pub mod testing;

pub mod clock;
pub mod set;

use std::sync::Arc;

use crate::routine::registry::RoutinesConfigurator;

pub fn default_native_procedures(builder: RoutinesConfigurator) -> RoutinesConfigurator {
	let builder = builder
		.register_builtin_procedure(Arc::new(set::config::SetConfigProcedure::new()))
		.register_builtin_procedure(Arc::new(clock::set::ClockSetProcedure::new()))
		.register_builtin_procedure(Arc::new(clock::advance::ClockAdvanceProcedure::new()))
		.register_builtin_procedure(Arc::new(identity::inject::IdentityInject::new()))
		.register_builtin_procedure(Arc::new(subscription::inspect::InspectSubscription::new()));
	testing::register_testing_native_procedures(builder)
}
