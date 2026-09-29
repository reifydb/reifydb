// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_core::value::{batch::batch, column::factory};
use reifydb_routine_abi::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	value::{Value, column_view::ColumnView, identity::IdentityId, value_type::ValueType},
};

struct ReportsNoneArguments {
	info: RoutineInfo,
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for ReportsNoneArguments {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Boolean
	}

	fn execute(
		&self,
		_ctx: &mut ProcedureContext<'a, 'tx>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<RecordBatch, RoutineError> {
		let arg = ColumnView::try_from(&args[0])?;
		let saw_none = (0..arg.len()).map(|row| matches!(arg.get_value(row), Value::None { .. }));
		Ok(batch(vec![factory::bool("saw_none", saw_none)])?)
	}
}

fn call_with(arg: (FieldRef, ArrayRef)) -> RecordBatch {
	let t = TestEngine::new();
	let services = t.inner().services();
	let catalog = services.catalog.clone();
	let params = Params::None;
	let identity = IdentityId::system();
	let mut txn = t.inner().begin_command(identity).expect("command transaction");
	let mut tx = Transaction::Command(&mut txn);
	let mut ctx = ProcedureContext {
		fragment: Fragment::internal("reports_none_arguments"),
		identity,
		row_count: arg.1.len(),
		runtime_context: &services.runtime_context,
		tx: &mut tx,
		params: &params,
		catalog: &catalog,
		ioc: &services.ioc,
	};
	let args = vec![arg];
	let procedure = ReportsNoneArguments {
		info: RoutineInfo::new("reports_none_arguments"),
	};
	procedure.call(&mut ctx, &args).expect("the call succeeds")
}

fn rows(column: &ColumnView) -> Vec<Value> {
	(0..column.len()).map(|row| column.get_value(row)).collect()
}

fn column_at(result: &RecordBatch, index: usize) -> ColumnView<'_> {
	ColumnView::try_from((result.column(index), result.schema_ref().field(index))).unwrap()
}

#[test]
fn a_procedure_with_an_all_none_argument_runs_and_sees_the_none() {
	// With propagation the body never runs on an all-none argument, so a procedure could never act on a none.
	let result = call_with(factory::int4_optional("arg", [None]));
	assert_eq!(result.num_columns(), 1, "the procedure returns exactly the one column its body builds");
	assert_eq!(
		result.schema_ref().field(0).name().as_str(),
		"saw_none",
		"the column must come from the body, not a none stand-in"
	);
	assert_eq!(
		rows(&column_at(&result, 0)),
		vec![Value::Boolean(true)],
		"the body must run and see the none argument"
	);
}

#[test]
fn a_procedure_sees_a_none_row_and_its_answer_for_that_row_is_not_masked() {
	// With propagation the body sees the none stripped and its answer for that row is masked, so it must not apply.
	let result = call_with(factory::int4_optional("arg", [Some(1), None]));
	assert_eq!(result.num_columns(), 1, "the procedure returns exactly the one column its body builds");
	let column = column_at(&result, 0);
	assert_eq!(column.get_type(), ValueType::Boolean, "the output must carry no none layer the body did not add");
	assert_eq!(
		rows(&column),
		vec![Value::Boolean(false), Value::Boolean(true)],
		"the body must see row 1 as none and its answer there must reach the caller"
	);
}
