// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::{date, time};
use reifydb_routine::function::datetime::{new::DateTimeNew, year::DateTimeYear};
use reifydb_routine_abi::{Routine, context::FunctionContext, error::RoutineError};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{
		column_view::{ColumnView, ViewData},
		date::Date,
		datetime::DateTime,
		identity::IdentityId,
		time::Time,
	},
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
fn datetime_new_with_a_1900_date_returns_that_instant_not_the_epoch() {
	// A u64 order value could not hold a pre epoch nanos count and silently clamped it to 1970.
	let dates = date("arg0", [Date::new(1900, 6, 15).unwrap()]);
	let times = time("arg1", [Time::new(12, 0, 0, 0).unwrap()]);

	let result = call(&DateTimeNew::new(), "datetime::new", vec![dates, times]).unwrap();

	let view = ColumnView::try_from(&result).unwrap();
	let ViewData::DateTime(container) = &view.data else {
		panic!("expected a DateTime column");
	};
	let got = container.value(0);
	let expected = DateTime::new(1900, 6, 15, 12, 0, 0, 0).unwrap().to_nanos();
	assert_eq!(got, expected);
	assert_ne!(DateTime::from_nanos(got).year(), 1970);
}

#[test]
fn datetime_year_of_a_1900_datetime_is_1900() {
	let dates = date("arg0", [Date::new(1900, 6, 15).unwrap()]);
	let times = time("arg1", [Time::new(12, 0, 0, 0).unwrap()]);
	let datetime_column = call(&DateTimeNew::new(), "datetime::new", vec![dates, times]).unwrap();

	let result = call(&DateTimeYear::new(), "datetime::year", vec![datetime_column]).unwrap();

	assert_eq!(ColumnView::try_from(&result).unwrap().as_string(0), "1900");
}
