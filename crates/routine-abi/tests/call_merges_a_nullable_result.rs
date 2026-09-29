// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::int4_with_bitvec;
use reifydb_routine_abi::{Routine, RoutineInfo, context::FunctionContext, error::RoutineError};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		identity::IdentityId,
		value_type::ValueType,
	},
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

	fn execute(
		&self,
		_ctx: &mut FunctionContext<'a>,
		_args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		Ok(int4_with_bitvec("nullable_result", [10, 20, 30], vec![true, true, false]))
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
	let args = [int4_with_bitvec("arg", [1, 0, 3], vec![true, false, true])];
	let routine = NullableResult {
		info: RoutineInfo::new("nullable_result"),
	};
	let result = routine.call(&mut ctx, &args).expect("the call succeeds");
	let column = ColumnView::try_from(&result).expect("the result is a readable column");
	assert_eq!(column.get_type(), ValueType::Option(Box::new(ValueType::Int4)), "exactly one option layer");
	let rows: Vec<Value> = (0..column.len()).map(|row| column.get_value(row)).collect();
	assert_eq!(rows, vec![Value::Int4(10), Value::none_of(ValueType::Int4), Value::none_of(ValueType::Int4)]);
	let ViewData::Int4(values) = &column.data else {
		panic!("the single layer must hold the Int4 values directly");
	};
	assert_eq!(values.values().to_vec(), vec![10, 20, 30], "the values under none rows must stay exactly");
	let nones = column.logical_nulls().expect("the result must be nullable");
	assert_eq!(nones.iter().collect::<Vec<bool>>(), vec![true, false, false]);
}
