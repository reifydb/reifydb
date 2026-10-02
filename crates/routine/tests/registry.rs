// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_routine::{
	function::default_in_process_functions,
	procedure::{clock::set::ClockSetProcedure, default_in_process_procedures},
};
use reifydb_routine_abi::{Procedure, registry::Routines};

fn registry() -> Routines {
	default_in_process_procedures(default_in_process_functions(Routines::builder())).configure()
}

#[test]
fn function_fallback_returns_same_arc_as_canonical() {
	let r = registry();
	let direct = r.get_function("math::abs").unwrap();
	let canonical = r.get_function("system::builtin::functions::math::abs").unwrap();
	assert!(Arc::ptr_eq(&direct, &canonical));
}

#[test]
fn procedure_fallback_returns_same_arc_as_canonical() {
	let r = registry();
	let direct = r.get_procedure("clock::set").unwrap();
	let canonical = r.get_procedure("system::builtin::procedures::clock::set").unwrap();
	assert!(Arc::ptr_eq(&direct, &canonical));
}

#[test]
fn procedure_multi_segment_fallback() {
	let r = registry();
	let direct = r.get_procedure("testing::events::dispatched").unwrap();
	let canonical = r.get_procedure("system::builtin::procedures::testing::events::dispatched").unwrap();
	assert!(Arc::ptr_eq(&direct, &canonical));
}

#[test]
fn unknown_namespace_returns_none() {
	let r = registry();
	assert!(r.get_function("nonexistent::foo").is_none());
	assert!(r.get_procedure("nonexistent::bar").is_none());
}

#[test]
fn unqualified_name_returns_none() {
	let r = registry();
	assert!(r.get_function("abs").is_none());
	assert!(r.get_procedure("set").is_none());
}

#[test]
fn alias_returns_same_arc_as_canonical() {
	let r = registry();
	let day = r.get_function("duration::day").unwrap();
	let days = r.get_function("duration::days").unwrap();
	assert!(Arc::ptr_eq(&day, &days));
}

#[test]
fn raw_registration_shadows_builtin() {
	let user_proc: Arc<dyn Procedure> = Arc::new(ClockSetProcedure::new());
	let r = default_in_process_procedures(Routines::builder()).register_procedure(user_proc.clone()).configure();
	let resolved = r.get_procedure("clock::set").unwrap();
	assert!(Arc::ptr_eq(&resolved, &user_proc));
}

#[test]
fn name_listings_strip_canonical_prefix() {
	let r = registry();
	let function_names = r.function_names();
	assert!(function_names.iter().any(|n| n == "math::abs"));
	assert!(!function_names.iter().any(|n| n.starts_with("system::builtin::functions::")));
	let procedure_names = r.procedure_names();
	assert!(procedure_names.iter().any(|n| n == "clock::set"));
	assert!(!procedure_names.iter().any(|n| n.starts_with("system::builtin::procedures::")));
}
