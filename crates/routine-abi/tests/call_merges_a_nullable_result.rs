// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{Routine, RoutineInfo, context::FunctionContext, error::RoutineError};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{Value, frame::data::FrameColumnData, identity::IdentityId, value_type::ValueType},
};

struct NullableResult {
	info: RoutineInfo,
}

impl<'a> Routine<FunctionContext<'a>> for NullableResult {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Option(Box::new(ValueType::Int4))
	}

	fn execute(&self, _ctx: &mut FunctionContext<'a>, _args: &Columns) -> Result<Columns, RoutineError> {
		Ok(Columns::new(vec![ColumnWithName::new(
			Fragment::internal("nullable_result"),
			ColumnBuffer::int4_with_bitvec([10, 20, 30], vec![true, true, false]),
		)]))
	}
}

#[test]
fn a_nullable_result_gets_the_argument_nones_anded_into_one_layer() {
	// Argument and result nones must merge into exactly one option layer, never nest.
	let runtime = RuntimeContext::testing(0, 0);
	let mut ctx = FunctionContext {
		fragment: Fragment::internal("nullable_result"),
		identity: IdentityId::root(),
		row_count: 3,
		runtime_context: &runtime,
	};
	let args = Columns::new(vec![ColumnWithName::new(
		Fragment::internal("arg"),
		ColumnBuffer::int4_optional([Some(1), None, Some(3)]),
	)]);
	let routine = NullableResult {
		info: RoutineInfo::new("nullable_result"),
	};
	let result = routine.call(&mut ctx, &args).expect("the call succeeds");
	assert_eq!(result.len(), 1, "a scalar routine returns exactly one column");
	let column = result.data_at(0).clone();
	assert_eq!(column.get_type(), ValueType::Option(Box::new(ValueType::Int4)), "exactly one option layer");
	let rows: Vec<Value> = (0..column.len()).map(|row| column.get_value(row)).collect();
	assert_eq!(rows, vec![Value::Int4(10), Value::none_of(ValueType::Int4), Value::none_of(ValueType::Int4)]);
	let FrameColumnData::Option {
		inner,
		bitvec,
	} = FrameColumnData::from(column)
	else {
		panic!("the result must be nullable");
	};
	let FrameColumnData::Int4(values) = *inner else {
		panic!("the single layer must hold the Int4 values directly");
	};
	assert_eq!(values.values().to_vec(), vec![10, 20, 30], "the values under none rows must stay exactly");
	assert_eq!(bitvec.iter().collect::<Vec<bool>>(), vec![true, false, false]);
}
