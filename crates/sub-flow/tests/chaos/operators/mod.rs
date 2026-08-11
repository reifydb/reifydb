// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod aggregate;
pub mod append;
pub mod distinct;
pub mod gate;
pub mod join;
pub mod pipeline;
pub mod rowwise;
pub mod sink;
pub mod source;
pub mod take;
pub mod window;

use reifydb_routine::{
	function::default_in_process_functions, monoid::default_in_process_monoids,
	procedure::default_in_process_procedures,
};
use reifydb_routine_abi::registry::Routines;

/// The registry every operator that evaluates an expression is built against. Shared so two suites
/// cannot end up driving their operators against different sets of monoids and disagreeing for a
/// reason that has nothing to do with the operator under test.
pub fn routines() -> Routines {
	let b = Routines::builder();
	let b = default_in_process_functions(b);
	let b = default_in_process_procedures(b);
	default_in_process_monoids(b).configure()
}
