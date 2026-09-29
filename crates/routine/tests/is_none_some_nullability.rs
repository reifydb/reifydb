// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_array::{Array, ArrayRef};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::FieldRef;
use reifydb_core::value::column::{
	factory::{int4, int4_optional},
	nulls::with_nulls,
};
use reifydb_routine::function::is::{none::IsNone, some::IsSome};
use reifydb_routine_abi::{Routine, context::FunctionContext, error::RoutineError};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{column_view::ColumnView, identity::IdentityId, value_type::ValueType},
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
	arg: (FieldRef, ArrayRef),
) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let row_count = arg.1.len();
	routine.call(&mut ctx(name, row_count), &[arg])
}

fn shapes() -> Vec<(&'static str, (FieldRef, ArrayRef), Vec<bool>)> {
	vec![
		("non nullable", int4("arg0", [1, 2, 3]), vec![false, false, false]),
		(
			"nullable with no none rows",
			with_nulls(int4("arg0", [1, 2, 3]), NullBuffer::new(BooleanBuffer::new_set(3))).unwrap(),
			vec![false, false, false],
		),
		("some none rows", int4_optional("arg0", [Some(1), None, Some(3)]), vec![false, true, false]),
		("all none rows", int4_optional("arg0", [None, None, None]), vec![true, true, true]),
		("empty", int4("arg0", Vec::<i32>::new()), vec![]),
	]
}

#[test]
fn is_none_and_is_some_answer_a_non_nullable_boolean_for_every_input_shape() {
	// Reading the null buffer must never hand its mask on to the answer, which is always defined.
	for (label, input, _) in shapes() {
		let none = call(&IsNone::new(), "is::none", input.clone()).unwrap();
		let some = call(&IsSome::new(), "is::some", input).unwrap();

		let none = ColumnView::try_from(&none).unwrap();
		let some = ColumnView::try_from(&some).unwrap();

		assert_eq!(none.get_type(), ValueType::Boolean, "is::none over a {label} column");
		assert_eq!(some.get_type(), ValueType::Boolean, "is::some over a {label} column");
	}
}

#[test]
fn is_none_marks_exactly_the_none_rows_and_is_some_is_its_complement() {
	// Reading the mask the wrong way round, or ignoring an all valid buffer, flips whole columns silently.
	for (label, input, expected) in shapes() {
		let none = call(&IsNone::new(), "is::none", input.clone()).unwrap();
		let some = call(&IsSome::new(), "is::some", input).unwrap();

		let none = ColumnView::try_from(&none).unwrap();
		let some = ColumnView::try_from(&some).unwrap();

		let none_answers: Vec<String> = (0..none.len()).map(|i| none.get_value(i).to_string()).collect();
		let some_answers: Vec<String> = (0..some.len()).map(|i| some.get_value(i).to_string()).collect();

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
