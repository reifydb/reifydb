// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::{
	date, date_with_bitvec, duration, from_many, from_one, int4, time, time_with_bitvec, utf8,
};
use reifydb_routine::function::{date as d, time as t};
use reifydb_routine_abi::{Function, context::FunctionContext};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{Value, date::Date, duration::Duration, identity::IdentityId, time::Time, value_type::ValueType},
};

const ROWS: usize = 4;

type Column = (FieldRef, ArrayRef);

fn ctx(row_count: usize) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal("date_time"),
		identity: IdentityId::root(),
		row_count,
		runtime_context: &RUNTIME,
	}
}

fn ymd(y: i32, m: u32, day: u32) -> Date {
	Date::new(y, m, day).unwrap()
}

fn hms(h: u32, m: u32, s: u32) -> Time {
	Time::new(h, m, s, 123_456_789).unwrap()
}

fn dates() -> Column {
	date_with_bitvec(
		"dates",
		vec![ymd(2024, 2, 29), ymd(1999, 12, 31), Date::default(), ymd(2023, 7, 4)],
		vec![true, true, false, true],
	)
}

fn dense_dates() -> Column {
	date("dates", vec![ymd(2024, 2, 29), ymd(1999, 12, 31), ymd(2000, 1, 1), ymd(2023, 7, 4)])
}

fn times() -> Column {
	time_with_bitvec(
		"times",
		vec![hms(1, 2, 3), hms(23, 59, 59), Time::default(), hms(12, 30, 0)],
		vec![true, true, false, true],
	)
}

fn dense_times() -> Column {
	time("times", vec![hms(1, 2, 3), hms(23, 59, 59), hms(0, 0, 0), hms(12, 30, 0)])
}

fn durations() -> Column {
	duration(
		"durations",
		vec![
			Duration::new(1, 2, 0).unwrap(),
			Duration::new(0, 40, 3_600_000_000_000).unwrap(),
			Duration::new(-13, 0, 0).unwrap(),
			Duration::new(0, 0, 90_000_000_000).unwrap(),
		],
	)
}

fn date_value() -> Value {
	Value::Date(ymd(2020, 1, 31))
}

fn time_value() -> Value {
	Value::Time(hms(8, 15, 45))
}

fn duration_value() -> Value {
	Value::Duration(Duration::new(1, 3, 7_200_000_000_000).unwrap())
}

fn none_of(inner: ValueType) -> Value {
	Value::None {
		inner,
	}
}

fn call(function: &impl Function, args: &[Column]) -> Result<Column, String> {
	function.call(&mut ctx(ROWS), args).map_err(|err| format!("{err:?}"))
}

fn check(name: &str, function: &impl Function, columns: Vec<Column>, constants: Vec<Vec<Value>>) -> Vec<String> {
	let mut wrong = Vec::new();
	let mut cases: Vec<(String, Vec<Column>, Vec<Column>)> = Vec::new();
	for (position, values) in constants.iter().enumerate() {
		for value in values {
			let mut one = columns.clone();
			let mut many = columns.clone();
			one[position] = from_one("constant", value.clone());
			many[position] = from_many("constant", value.clone(), ROWS);
			cases.push((format!("{name} arg {position} = {value:?}"), one, many));
		}
	}
	if columns.len() > 1 {
		let one = constants.iter().map(|values| from_one("constant", values[0].clone())).collect();
		let many = constants.iter().map(|values| from_many("constant", values[0].clone(), ROWS)).collect();
		cases.push((format!("{name} all constant"), one, many));
	}
	for (label, one, many) in cases {
		match (call(function, &one), call(function, &many)) {
			(Ok((_, constant)), Ok((_, repeated))) => {
				if constant.len() != ROWS || repeated.len() != ROWS {
					wrong.push(format!(
						"{label}: {} rows from a constant, {} rows repeated",
						constant.len(),
						repeated.len()
					));
				} else if constant.to_data() != repeated.to_data() {
					wrong.push(format!("{label}: {constant:?} vs {repeated:?}"));
				}
			}
			(Err(_), Err(_)) => {}
			(constant, repeated) => wrong.push(format!(
				"{label}: {:?} vs {:?}",
				constant.map(|(_, a)| a.len()),
				repeated.map(|(_, a)| a.len())
			)),
		}
	}
	wrong
}

fn assert_none_wrong(wrong: Vec<String>) {
	assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

fn unary_date_constants() -> Vec<Vec<Value>> {
	vec![vec![date_value(), none_of(ValueType::Date)]]
}

fn unary_time_constants() -> Vec<Vec<Value>> {
	vec![vec![time_value(), none_of(ValueType::Time)]]
}

#[test]
fn unary_date_functions_broadcast_a_constant_date() {
	// A 1-row date must answer row_count rows, otherwise the result is shorter than the batch.
	let mut wrong = Vec::new();
	for columns in [dates(), dense_dates()] {
		let columns = vec![columns];
		wrong.extend(check("date::year", &d::year::DateYear::new(), columns.clone(), unary_date_constants()));
		wrong.extend(check(
			"date::month",
			&d::month::DateMonth::new(),
			columns.clone(),
			unary_date_constants(),
		));
		wrong.extend(check("date::day", &d::day::DateDay::new(), columns.clone(), unary_date_constants()));
		wrong.extend(check(
			"date::quarter",
			&d::quarter::DateQuarter::new(),
			columns.clone(),
			unary_date_constants(),
		));
		wrong.extend(check(
			"date::day_of_week",
			&d::day_of_week::DateDayOfWeek::new(),
			columns.clone(),
			unary_date_constants(),
		));
		wrong.extend(check(
			"date::day_of_year",
			&d::day_of_year::DateDayOfYear::new(),
			columns.clone(),
			unary_date_constants(),
		));
		wrong.extend(check("date::week", &d::week::DateWeek::new(), columns.clone(), unary_date_constants()));
		wrong.extend(check(
			"date::days_in_month",
			&d::days_in_month::DateDaysInMonth::new(),
			columns.clone(),
			unary_date_constants(),
		));
		wrong.extend(check(
			"date::is_leap_year",
			&d::is_leap_year::DateIsLeapYear::new(),
			columns.clone(),
			unary_date_constants(),
		));
		wrong.extend(check(
			"date::start_of_month",
			&d::start_of_month::DateStartOfMonth::new(),
			columns.clone(),
			unary_date_constants(),
		));
		wrong.extend(check(
			"date::start_of_year",
			&d::start_of_year::DateStartOfYear::new(),
			columns.clone(),
			unary_date_constants(),
		));
		wrong.extend(check(
			"date::end_of_month",
			&d::end_of_month::DateEndOfMonth::new(),
			columns.clone(),
			unary_date_constants(),
		));
	}
	assert_none_wrong(wrong);
}

#[test]
fn unary_time_functions_broadcast_a_constant_time() {
	// A 1-row time must answer row_count rows, otherwise the result is shorter than the batch.
	let mut wrong = Vec::new();
	for columns in [times(), dense_times()] {
		let columns = vec![columns];
		wrong.extend(check("time::hour", &t::hour::TimeHour::new(), columns.clone(), unary_time_constants()));
		wrong.extend(check(
			"time::minute",
			&t::minute::TimeMinute::new(),
			columns.clone(),
			unary_time_constants(),
		));
		wrong.extend(check(
			"time::second",
			&t::second::TimeSecond::new(),
			columns.clone(),
			unary_time_constants(),
		));
		wrong.extend(check(
			"time::nanosecond",
			&t::nanosecond::TimeNanosecond::new(),
			columns.clone(),
			unary_time_constants(),
		));
	}
	assert_none_wrong(wrong);
}

#[test]
fn binary_date_functions_read_a_constant_in_every_position() {
	// Every row must read row 0 of a 1-row argument, otherwise rows past the first see a missing value.
	let date_or_none = vec![date_value(), none_of(ValueType::Date)];
	let duration_or_none = vec![duration_value(), none_of(ValueType::Duration)];
	let mut wrong = Vec::new();
	wrong.extend(check(
		"date::add",
		&d::add::DateAdd::new(),
		vec![dates(), durations()],
		vec![date_or_none.clone(), duration_or_none.clone()],
	));
	wrong.extend(check(
		"date::subtract",
		&d::subtract::DateSubtract::new(),
		vec![dates(), durations()],
		vec![date_or_none.clone(), duration_or_none.clone()],
	));
	wrong.extend(check(
		"date::age",
		&d::age::DateAge::new(),
		vec![dates(), dense_dates()],
		vec![date_or_none.clone(), date_or_none.clone()],
	));
	wrong.extend(check(
		"date::diff",
		&d::diff::DateDiff::new(),
		vec![dates(), dense_dates()],
		vec![date_or_none.clone(), date_or_none.clone()],
	));
	wrong.extend(check(
		"date::format",
		&d::format::DateFormat::new(),
		vec![dates(), utf8("fmt", ["%Y-%m-%d", "%j", "%d/%m", "%Y%%"])],
		vec![date_or_none.clone(), vec![Value::Utf8("%Y-%m-%d".to_string()), none_of(ValueType::Utf8)]],
	));
	wrong.extend(check(
		"date::trunc",
		&d::trunc::DateTrunc::new(),
		vec![dates(), utf8("precision", ["year", "month", "year", "month"])],
		vec![date_or_none, vec![Value::Utf8("month".to_string()), none_of(ValueType::Utf8)]],
	));
	assert_none_wrong(wrong);
}

#[test]
fn date_new_reads_a_constant_year_month_or_day() {
	// A 1-row part must apply to every row, otherwise later rows read past the end of the part.
	let mut wrong = Vec::new();
	wrong.extend(check(
		"date::new",
		&d::new::DateNew::new(),
		vec![
			int4("year", [2024, 1999, 2000, 2023]),
			int4("month", [1, 2, 3, 12]),
			int4("day", [1, 15, 28, 31]),
		],
		vec![
			vec![Value::Int4(2021), none_of(ValueType::Int4)],
			vec![Value::Int4(6), none_of(ValueType::Int4)],
			vec![Value::Int4(9), none_of(ValueType::Int4)],
		],
	));
	assert_none_wrong(wrong);
}

#[test]
fn binary_time_functions_read_a_constant_in_every_position() {
	// Every row must read row 0 of a 1-row argument, otherwise rows past the first see a missing value.
	let time_or_none = vec![time_value(), none_of(ValueType::Time)];
	let duration_or_none = vec![duration_value(), none_of(ValueType::Duration)];
	let mut wrong = Vec::new();
	wrong.extend(check(
		"time::add",
		&t::add::TimeAdd::new(),
		vec![times(), durations()],
		vec![time_or_none.clone(), duration_or_none.clone()],
	));
	wrong.extend(check(
		"time::subtract",
		&t::subtract::TimeSubtract::new(),
		vec![times(), durations()],
		vec![time_or_none.clone(), duration_or_none.clone()],
	));
	wrong.extend(check(
		"time::age",
		&t::age::TimeAge::new(),
		vec![times(), dense_times()],
		vec![time_or_none.clone(), time_or_none.clone()],
	));
	wrong.extend(check(
		"time::diff",
		&t::diff::TimeDiff::new(),
		vec![times(), dense_times()],
		vec![time_or_none.clone(), time_or_none.clone()],
	));
	wrong.extend(check(
		"time::format",
		&t::format::TimeFormat::new(),
		vec![times(), utf8("fmt", ["%H:%M:%S", "%f", "%H%%", "%M.%3"])],
		vec![time_or_none.clone(), vec![Value::Utf8("%H:%M:%S".to_string()), none_of(ValueType::Utf8)]],
	));
	wrong.extend(check(
		"time::trunc",
		&t::trunc::TimeTrunc::new(),
		vec![times(), utf8("precision", ["hour", "minute", "second", "hour"])],
		vec![time_or_none, vec![Value::Utf8("minute".to_string()), none_of(ValueType::Utf8)]],
	));
	assert_none_wrong(wrong);
}

#[test]
fn time_new_reads_a_constant_part_in_every_position() {
	// A 1-row part must apply to every row, otherwise later rows read past the end of the part.
	let hours = int4("hour", [0, 5, 13, 23]);
	let minutes = int4("minute", [0, 15, 30, 59]);
	let seconds = int4("second", [1, 2, 3, 59]);
	let nanos = int4("nano", [0, 7, 999, 1_000_000]);
	let part = |value: i32| vec![Value::Int4(value), none_of(ValueType::Int4)];
	let mut wrong = Vec::new();
	wrong.extend(check(
		"time::new",
		&t::new::TimeNew::new(),
		vec![hours.clone(), minutes.clone(), seconds.clone()],
		vec![part(9), part(41), part(17)],
	));
	wrong.extend(check(
		"time::new",
		&t::new::TimeNew::new(),
		vec![hours, minutes, seconds, nanos],
		vec![part(9), part(41), part(17), part(500)],
	));
	assert_none_wrong(wrong);
}

#[test]
fn now_answers_one_value_per_row() {
	// A zero-argument function must still fill row_count rows, otherwise the batch length assertion fails.
	let wrong: Vec<String> =
		[("date::now", call(&d::now::DateNow::new(), &[])), ("time::now", call(&t::now::TimeNow::new(), &[]))]
			.into_iter()
			.filter_map(|(name, result)| match result {
				Ok((_, array)) if array.len() == ROWS => None,
				other => Some(format!("{name}: {:?}", other.map(|(_, a)| a.len()))),
			})
			.collect();
	assert_none_wrong(wrong);
}
