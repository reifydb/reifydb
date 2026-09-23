// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine::function::{
	date::{
		day::DateDay, day_of_week::DateDayOfWeek, day_of_year::DateDayOfYear, month::DateMonth,
		quarter::DateQuarter, week::DateWeek, year::DateYear,
	},
	time::{hour::TimeHour, minute::TimeMinute, nanosecond::TimeNanosecond, second::TimeSecond},
};
use reifydb_routine_abi::{Routine, context::FunctionContext, error::RoutineError};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{date::Date, identity::IdentityId, time::Time, value_type::ValueType},
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

fn date_fields() -> Vec<(&'static str, Box<dyn Routine<FunctionContext<'static>>>)> {
	vec![
		("date::year", Box::new(DateYear::new())),
		("date::month", Box::new(DateMonth::new())),
		("date::day", Box::new(DateDay::new())),
		("date::day_of_year", Box::new(DateDayOfYear::new())),
		("date::day_of_week", Box::new(DateDayOfWeek::new())),
		("date::quarter", Box::new(DateQuarter::new())),
		("date::week", Box::new(DateWeek::new())),
	]
}

fn time_fields() -> Vec<(&'static str, Box<dyn Routine<FunctionContext<'static>>>)> {
	vec![
		("time::hour", Box::new(TimeHour::new())),
		("time::minute", Box::new(TimeMinute::new())),
		("time::second", Box::new(TimeSecond::new())),
		("time::nanosecond", Box::new(TimeNanosecond::new())),
	]
}

fn answers(routine: &dyn Routine<FunctionContext<'static>>, name: &str, arg: ColumnBuffer) -> Vec<String> {
	let result = call(routine, name, arg).unwrap();
	(0..result[0].len()).map(|i| result[0].as_string(i)).collect()
}

#[test]
fn date_and_time_field_functions_answer_a_non_nullable_int4() {
	// The extraction kernel always attaches a null buffer; keeping it turns every field into an optional type.
	let dates = ColumnBuffer::date([Date::new(2024, 3, 17).unwrap(), Date::new(1970, 1, 1).unwrap()]);
	let times = ColumnBuffer::time([Time::new(13, 45, 6, 123_456_789).unwrap(), Time::new(0, 0, 0, 0).unwrap()]);

	for (name, routine) in date_fields() {
		let result = call(routine.as_ref(), name, dates.clone()).unwrap();
		assert_eq!(result[0].get_type(), ValueType::Int4, "{name} over a non nullable date column");
	}

	for (name, routine) in time_fields() {
		let result = call(routine.as_ref(), name, times.clone()).unwrap();
		assert_eq!(result[0].get_type(), ValueType::Int4, "{name} over a non nullable time column");
	}
}

#[test]
fn date_fields_answer_the_calendar_parts_of_the_date() {
	// Picking a neighbouring part from the kernel is silent: every answer stays a plausible small integer.
	let dates = ColumnBuffer::date([Date::new(2024, 3, 17).unwrap()]);

	assert_eq!(answers(&DateYear::new(), "date::year", dates.clone()), vec!["2024"]);
	assert_eq!(answers(&DateMonth::new(), "date::month", dates.clone()), vec!["3"]);
	assert_eq!(answers(&DateDay::new(), "date::day", dates.clone()), vec!["17"]);
	assert_eq!(answers(&DateDayOfYear::new(), "date::day_of_year", dates.clone()), vec!["77"]);
	assert_eq!(answers(&DateQuarter::new(), "date::quarter", dates.clone()), vec!["1"]);
	assert_eq!(answers(&DateWeek::new(), "date::week", dates), vec!["11"]);
}

#[test]
fn date_day_of_week_numbers_monday_one_through_sunday_seven_across_the_epoch() {
	// The kernel offers five different day of week numberings; the wrong one shifts every answer by a constant.
	let days: Vec<Date> = (-1..=7).map(|d| Date::from_days_since_epoch(d).unwrap()).collect();

	let got = answers(&DateDayOfWeek::new(), "date::day_of_week", ColumnBuffer::date(days));

	assert_eq!(got, vec!["3", "4", "5", "6", "7", "1", "2", "3", "4"]);
}

#[test]
fn time_fields_answer_the_part_of_their_own_unit_not_of_the_whole_day() {
	// A nanosecond of the day rather than of the second overflows an int4 and answers a negative number.
	let times = ColumnBuffer::time([Time::new(13, 45, 6, 123_456_789).unwrap()]);

	assert_eq!(answers(&TimeHour::new(), "time::hour", times.clone()), vec!["13"]);
	assert_eq!(answers(&TimeMinute::new(), "time::minute", times.clone()), vec!["45"]);
	assert_eq!(answers(&TimeSecond::new(), "time::second", times.clone()), vec!["6"]);
	assert_eq!(answers(&TimeNanosecond::new(), "time::nanosecond", times), vec!["123456789"]);
}

#[test]
fn date_year_of_a_year_past_two_hundred_sixty_two_thousand_is_not_silently_none() {
	// The kernel cannot represent a date this far out and answers none for it; our own date type accepts it.
	let far = Date::from_days_since_epoch(200_000_000).unwrap();
	let expected = far.year().to_string();

	let got = answers(&DateYear::new(), "date::year", ColumnBuffer::date([far]));

	assert_ne!(got, vec!["none".to_string()], "a date our own type accepts must stay queryable");
	assert_eq!(got, vec![expected]);
}

#[test]
fn a_date_column_mixing_an_in_range_and_an_out_of_range_row_answers_both() {
	// A per row fallback that overwrites the kernel's rows, or skips them, corrupts the rows it did answer.
	let dates = ColumnBuffer::date([
		Date::new(2024, 3, 17).unwrap(),
		Date::from_days_since_epoch(200_000_000).unwrap(),
		Date::new(1970, 1, 1).unwrap(),
	]);
	let expected = vec![
		"2024".to_string(),
		Date::from_days_since_epoch(200_000_000).unwrap().year().to_string(),
		"1970".to_string(),
	];

	let result = call(&DateYear::new(), "date::year", dates).unwrap();

	assert_eq!(result[0].get_type(), ValueType::Int4);
	assert_eq!((0..3).map(|i| result[0].as_string(i)).collect::<Vec<_>>(), expected);
}
