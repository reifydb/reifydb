// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_buffer::{BooleanBuffer, NullBuffer};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine::function::is::{none::IsNone, some::IsSome};
use reifydb_routine_abi::{Routine, context::FunctionContext, error::RoutineError};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{identity::IdentityId, value_type::ValueType},
};

fn ctx(name: &str, row_count: usize) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal(name),
		identity: IdentityId::root(),
		row_count,
		runtime_context: &RUNTIME,
	}
}

fn call(
	routine: &dyn Routine<FunctionContext<'static>>,
	name: &str,
	arg: ColumnBuffer,
) -> Result<Columns, RoutineError> {
	let row_count = arg.len();
	let columns = Columns::new(vec![ColumnWithName::new(Fragment::internal("arg0"), arg)]);
	routine.call(&mut ctx(name, row_count), &columns)
}

fn shapes() -> Vec<(&'static str, ColumnBuffer, Vec<bool>)> {
	vec![
		("non nullable", ColumnBuffer::int4([1, 2, 3]), vec![false, false, false]),
		(
			"nullable with no none rows",
			ColumnBuffer::int4([1, 2, 3]).with_nulls(NullBuffer::new(BooleanBuffer::new_set(3))),
			vec![false, false, false],
		),
		("some none rows", ColumnBuffer::int4_optional([Some(1), None, Some(3)]), vec![false, true, false]),
		("all none rows", ColumnBuffer::int4_optional([None, None, None]), vec![true, true, true]),
		("empty", ColumnBuffer::int4(Vec::<i32>::new()), vec![]),
	]
}

#[test]
fn is_none_and_is_some_answer_a_non_nullable_boolean_for_every_input_shape() {
	// Reading the null buffer must never hand its mask on to the answer, which is always defined.
	for (label, input, _) in shapes() {
		let none = call(&IsNone::new(), "is::none", input.clone()).unwrap();
		let some = call(&IsSome::new(), "is::some", input).unwrap();

		assert_eq!(none[0].get_type(), ValueType::Boolean, "is::none over a {label} column");
		assert_eq!(some[0].get_type(), ValueType::Boolean, "is::some over a {label} column");
	}
}

#[test]
fn is_none_marks_exactly_the_none_rows_and_is_some_is_its_complement() {
	// Reading the mask the wrong way round, or ignoring an all valid buffer, flips whole columns silently.
	for (label, input, expected) in shapes() {
		let none = call(&IsNone::new(), "is::none", input.clone()).unwrap();
		let some = call(&IsSome::new(), "is::some", input).unwrap();

		let none_answers: Vec<String> = (0..none[0].len()).map(|i| none[0].get_value(i).to_string()).collect();
		let some_answers: Vec<String> = (0..some[0].len()).map(|i| some[0].get_value(i).to_string()).collect();

		assert_eq!(
			none_answers,
			expected.iter().map(|v| v.to_string()).collect::<Vec<_>>(),
			"is::none over a {label} column"
		);
		assert_eq!(
			some_answers,
			expected.iter().map(|v| (!v).to_string()).collect::<Vec<_>>(),
			"is::some over a {label} column"
		);
	}
}
