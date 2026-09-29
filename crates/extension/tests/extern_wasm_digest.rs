// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "wasm")]

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_core::value::{
	batch::batch,
	column::{builder::ColumnBuilder, factory, nulls::split_nulls},
};
use reifydb_extension::{
	function::extern_wasm::ExternWasmScalarFunction,
	transform::{Transform, context::TransformContext, extern_wasm::ExternWasmTransform},
};
use reifydb_routine_abi::{Routine, context::FunctionContext, error::RoutineError, registry::Routines};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	error::Diagnostic,
	fragment::Fragment,
	params::Params,
	value::{Value, column_view::ColumnView, digest::Digest, identity::IdentityId, value_type::ValueType},
};

fn digest_value() -> Value {
	let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
	for value in [1.0, 2.0, 3.0] {
		digest.add_value(&Value::float8(value)).unwrap();
	}
	Value::Digest(Box::new(digest))
}

fn digest_column() -> (FieldRef, ArrayRef) {
	let (buffer, _) = split_nulls(factory::none_typed("d", digest_value().get_type(), 0)).unwrap();
	let mut builder = ColumnBuilder::from_view(&ColumnView::try_from(&buffer).unwrap());
	builder.push_value(digest_value());
	builder.finish("d")
}

fn digest_columns() -> RecordBatch {
	batch(vec![digest_column()]).unwrap()
}

fn assert_extern_001(diagnostic: &Diagnostic) {
	assert_eq!(diagnostic.code, "EXTERN_001", "got: {diagnostic:?}");
	assert!(
		diagnostic.message.contains(&digest_value().get_type().to_string()),
		"the message must name the digest type, got: {}",
		diagnostic.message
	);
}

#[test]
fn wasm_transform_given_a_digest_column_reports_extern_001_before_loading_the_module() {
	// The module bytes are empty, so any error other than EXTERN_001 means the digest got past the check.
	let transform = ExternWasmTransform::new("t", Vec::new());
	let routines = Routines::empty();
	let runtime_context = RuntimeContext::testing(0, 0);
	let params = Params::None;
	let ctx = TransformContext {
		routines: &routines,
		runtime_context: &runtime_context,
		params: &params,
	};

	let err = transform.apply(&ctx, digest_columns()).unwrap_err();

	assert_extern_001(&err.diagnostic());
}

#[test]
fn wasm_scalar_function_given_a_digest_argument_reports_extern_001_before_loading_the_module() {
	// A function error that is not wrapped would lose the code and read as a module load failure.
	let function = ExternWasmScalarFunction::new("f", Vec::new());
	let runtime_context = RuntimeContext::testing(0, 0);
	let mut ctx = FunctionContext {
		fragment: Fragment::internal("f"),
		identity: IdentityId::default(),
		row_count: 1,
		runtime_context: &runtime_context,
	};

	let Err(err) = function.execute(&mut ctx, &[digest_column()]) else {
		panic!("a digest argument to a wasm function must fail");
	};

	let RoutineError::Wrapped(inner) = err else {
		panic!("expected the marshal error to be wrapped, got: {err:?}");
	};
	assert_extern_001(&inner.diagnostic());
}
