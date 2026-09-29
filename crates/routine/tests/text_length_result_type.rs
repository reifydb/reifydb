// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::utf8;
use reifydb_routine::function::text::length::TextLength;
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
	args: Vec<(FieldRef, ArrayRef)>,
) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let row_count = args.first().map_or(0, |(_, array)| array.len());
	routine.call(&mut ctx(name, row_count), &args)
}

#[test]
fn text_length_over_a_large_utf8_column_answers_int4_not_int8() {
	// A 64 bit byte length left unnarrowed changes the declared column type and the wire bytes.
	let routine = TextLength::new();

	let result = call(&routine, "text::length", vec![utf8("arg0", ["a", "cafe", ""])]).unwrap();
	let view = ColumnView::try_from(&result).unwrap();

	assert_eq!(view.get_type(), ValueType::Int4);
	assert_eq!(view.get_type(), routine.return_type(&[ValueType::Utf8]));
}

#[test]
fn text_length_counts_bytes_not_characters() {
	// A character count would silently answer 4 for a four character string holding five bytes.
	let routine = TextLength::new();

	let result = call(&routine, "text::length", vec![utf8("arg0", ["café"])]).unwrap();
	let view = ColumnView::try_from(&result).unwrap();

	assert_eq!(view.get_value(0).to_string(), "5");
}

#[test]
fn text_length_of_an_empty_column_answers_an_empty_non_nullable_int4() {
	// An empty input must not gain a null buffer, otherwise the column turns into an optional type.
	let routine = TextLength::new();

	let result = call(&routine, "text::length", vec![utf8("arg0", Vec::<String>::new())]).unwrap();
	let view = ColumnView::try_from(&result).unwrap();

	assert_eq!(view.len(), 0);
	assert_eq!(view.get_type(), ValueType::Int4);
}
