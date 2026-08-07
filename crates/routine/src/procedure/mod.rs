// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Built-in procedures: imperative routines invoked as named statements, which may mutate catalog or storage state.
//! Anything that does not fit cleanly into a pure function goes here.
//!
//! `default_native_procedures` installs the workspace's built-ins; extensions add their own through the same
//! `RoutinesConfigurator`.

pub mod identity;
pub mod subscription;
pub mod testing;

pub mod clock;
pub mod graphql;
pub mod rql;
pub mod set;
pub mod source;

use std::sync::Arc;

use reifydb_routine_abi::registry::RoutinesConfigurator;

pub fn default_native_procedures(builder: RoutinesConfigurator) -> RoutinesConfigurator {
	let builder = builder
		.register_builtin_procedure(Arc::new(set::config::SetConfigProcedure::new()))
		.register_builtin_procedure(Arc::new(source::complete_through::CompleteThroughProcedure::new()))
		.register_builtin_procedure(Arc::new(clock::set::ClockSetProcedure::new()))
		.register_builtin_procedure(Arc::new(clock::advance::ClockAdvanceProcedure::new()))
		.register_builtin_procedure(Arc::new(identity::inject::IdentityInject::new()))
		.register_builtin_procedure(Arc::new(identity::set_attribute::SetIdentityAttribute::new()))
		.register_builtin_procedure(Arc::new(identity::remove_attribute::RemoveIdentityAttribute::new()))
		.register_builtin_procedure(Arc::new(subscription::inspect::InspectSubscription::new()))
		.register_builtin_procedure(Arc::new(rql::tokenize::RqlTokenize::new()))
		.register_builtin_procedure(Arc::new(rql::ast::RqlAst::new()))
		.register_builtin_procedure(Arc::new(rql::logical::RqlLogical::new()))
		.register_builtin_procedure(Arc::new(rql::explain::RqlExplain::new()))
		.register_builtin_procedure(Arc::new(graphql::explain::GraphqlExplain::new()));
	testing::register_testing_native_procedures(builder)
}
