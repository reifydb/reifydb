// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::utf8;
use reifydb_routine::function::text::{
	concat::TextConcat, contains::TextContains, ends_with::TextEndsWith, starts_with::TextStartsWith,
};
use reifydb_routine_abi::{Routine, context::FunctionContext, error::RoutineError};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{fragment::Fragment, value::identity::IdentityId};

fn ctx(name: &str, row_count: usize) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal(name),
		identity: IdentityId::root(),
		row_count,
		runtime_context: &RUNTIME,
	}
}

fn ragged(long: &[&str], short: &[&str]) -> Vec<(FieldRef, ArrayRef)> {
	vec![utf8("arg0", long.to_vec()), utf8("arg1", short.to_vec())]
}

fn outcome(routine: &dyn Routine<FunctionContext<'static>>, name: &str) -> Result<(FieldRef, ArrayRef), RoutineError> {
	routine.execute(&mut ctx(name, 3), &ragged(&["ab", "cd", "ef"], &["a"]))
}

#[test]
fn concat_of_arguments_of_different_lengths_is_an_error_not_a_silently_padded_row() {
	// Padding the missing rows with an empty string answers a value that no argument ever held.
	let result = outcome(&TextConcat::new(), "text::concat");

	match result {
		Err(RoutineError::FunctionExecutionFailed {
			..
		}) => {}
		other => panic!("unequal argument lengths must fail execution, got {other:?}"),
	}
}

#[test]
fn every_text_predicate_of_arguments_of_different_lengths_is_an_error_not_a_panic() {
	// Indexing the short argument with the long argument's row count reads past its end.
	let cases: [(Box<dyn Routine<FunctionContext<'static>>>, &str); 3] = [
		(Box::new(TextContains::new()), "text::contains"),
		(Box::new(TextStartsWith::new()), "text::starts_with"),
		(Box::new(TextEndsWith::new()), "text::ends_with"),
	];

	for (routine, name) in cases {
		match outcome(routine.as_ref(), name) {
			Err(RoutineError::FunctionExecutionFailed {
				..
			}) => {}
			other => panic!("{name} with unequal argument lengths must fail execution, got {other:?}"),
		}
	}
}
