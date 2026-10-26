// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::{date, datetime, duration, from_many, int8, none_typed, time, utf8};
use reifydb_routine::function::datetime::{
	add::DateTimeAdd, age::DateTimeAge, date::DateTimeDate, day::DateTimeDay, day_of_week::DateTimeDayOfWeek,
	day_of_year::DateTimeDayOfYear, diff::DateTimeDiff, epoch::DateTimeEpoch, epoch_millis::DateTimeEpochMillis,
	format::DateTimeFormat, from_epoch::DateTimeFromEpoch, from_epoch_millis::DateTimeFromEpochMillis,
	hour::DateTimeHour, minute::DateTimeMinute, month::DateTimeMonth, nanosecond::DateTimeNanosecond,
	new::DateTimeNew, quarter::DateTimeQuarter, second::DateTimeSecond, subtract::DateTimeSubtract,
	time::DateTimeTime, trunc::DateTimeTrunc, week::DateTimeWeek, year::DateTimeYear,
};
use reifydb_routine_abi::{Routine, context::FunctionContext};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value, date::Date, datetime::DateTime, duration::Duration, identity::IdentityId, time::Time,
		value_type::ValueType,
	},
};

const N: usize = 4;

fn ctx(row_count: usize) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal("datetime"),
		identity: IdentityId::root(),
		row_count,
		runtime_context: &RUNTIME,
	}
}

enum Constant {
	Some(Value),
	None(ValueType),
}

impl Constant {
	fn column(&self, len: usize) -> (FieldRef, ArrayRef) {
		match self {
			Constant::Some(value) => from_many("arg", value.clone(), len),
			Constant::None(ty) => none_typed("arg", ty.clone(), len),
		}
	}
}

fn dt(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime {
	DateTime::new(y, m, d, h, mi, s, 123_000_000).unwrap()
}

fn datetimes() -> (FieldRef, ArrayRef) {
	datetime(
		"arg",
		[
			dt(2024, 1, 15, 3, 4, 5),
			dt(2023, 6, 30, 12, 0, 0),
			dt(1999, 12, 31, 23, 59, 59),
			dt(2020, 2, 29, 7, 8, 9),
		],
	)
}

fn dt_constants() -> Vec<Constant> {
	vec![Constant::Some(Value::DateTime(dt(2021, 7, 4, 10, 20, 30))), Constant::None(ValueType::DateTime)]
}

fn check(
	name: &str,
	routine: &dyn Routine<FunctionContext<'static>>,
	columns: Vec<(FieldRef, ArrayRef)>,
	constants: Vec<Vec<Constant>>,
) -> usize {
	let mut checked = 0;
	let mut cases: Vec<Vec<Option<&Constant>>> = Vec::new();
	for (position, options) in constants.iter().enumerate() {
		for constant in options {
			let mut case = vec![None; columns.len()];
			case[position] = Some(constant);
			cases.push(case);
		}
	}
	if columns.len() > 1 {
		cases.push(constants.iter().map(|options| options.first()).collect());
	}
	for case in cases {
		let one: Vec<_> = case
			.iter()
			.zip(&columns)
			.map(|(c, col)| c.map_or_else(|| col.clone(), |c| c.column(1)))
			.collect();
		let many: Vec<_> = case
			.iter()
			.zip(&columns)
			.map(|(c, col)| c.map_or_else(|| col.clone(), |c| c.column(N)))
			.collect();
		let got = routine.call(&mut ctx(N), &one).unwrap();
		let want = routine.call(&mut ctx(N), &many).unwrap();
		assert_eq!(got.1.len(), N, "{name}: 1 row constant must still answer row_count rows");
		assert_eq!(got.1.as_ref(), want.1.as_ref(), "{name}: 1 row constant must match the repeated constant");
		checked += 1;
	}
	checked
}

#[test]
fn single_datetime_argument_functions_broadcast_a_constant() {
	// A 1 row constant answered 1 row (taken from the argument length) instead of row_count rows.
	let routines: Vec<(&str, Box<dyn Routine<FunctionContext<'static>>>)> = vec![
		("year", Box::new(DateTimeYear::new())),
		("month", Box::new(DateTimeMonth::new())),
		("day", Box::new(DateTimeDay::new())),
		("hour", Box::new(DateTimeHour::new())),
		("minute", Box::new(DateTimeMinute::new())),
		("second", Box::new(DateTimeSecond::new())),
		("nanosecond", Box::new(DateTimeNanosecond::new())),
		("day_of_year", Box::new(DateTimeDayOfYear::new())),
		("day_of_week", Box::new(DateTimeDayOfWeek::new())),
		("quarter", Box::new(DateTimeQuarter::new())),
		("week", Box::new(DateTimeWeek::new())),
		("epoch", Box::new(DateTimeEpoch::new())),
		("epoch_millis", Box::new(DateTimeEpochMillis::new())),
		("date", Box::new(DateTimeDate::new())),
		("time", Box::new(DateTimeTime::new())),
	];
	for (name, routine) in &routines {
		assert_eq!(check(name, routine.as_ref(), vec![datetimes()], vec![dt_constants()]), 2, "{name}");
	}
}

#[test]
fn from_epoch_functions_broadcast_a_constant() {
	// read_i64 maps the row, but the output length came from the 1 row argument.
	let epochs = || int8("arg", [0i64, 1_700_000_000, 86_400, 951_782_400]);
	let constants = || vec![vec![Constant::Some(Value::Int8(1_600_000_000)), Constant::None(ValueType::Int8)]];
	assert_eq!(check("from_epoch", &DateTimeFromEpoch::new(), vec![epochs()], constants()), 2);
	let millis = int8("arg", [0i64, 1_700_000_000_123, 86_400_000, 951_782_400_999]);
	let constants = vec![vec![Constant::Some(Value::Int8(1_600_000_000_456)), Constant::None(ValueType::Int8)]];
	assert_eq!(check("from_epoch_millis", &DateTimeFromEpochMillis::new(), vec![millis], constants), 2);
}

#[test]
fn datetime_and_duration_functions_broadcast_either_constant() {
	// Reading row i of a 1 row duration returned none past row 0, so rows lost their shift.
	let durations = || {
		duration(
			"arg",
			[
				Duration::from_hours(1).unwrap(),
				Duration::from_days(3).unwrap(),
				Duration::from_seconds(59).unwrap(),
				Duration::from_minutes(90).unwrap(),
			],
		)
	};
	let constants = || {
		vec![
			dt_constants(),
			vec![
				Constant::Some(Value::Duration(Duration::from_hours(5).unwrap())),
				Constant::None(ValueType::Duration),
			],
		]
	};
	assert_eq!(check("add", &DateTimeAdd::new(), vec![datetimes(), durations()], constants()), 5);
	assert_eq!(check("subtract", &DateTimeSubtract::new(), vec![datetimes(), durations()], constants()), 5);
}

#[test]
fn two_datetime_functions_broadcast_either_constant() {
	// Either side as a 1 row constant must pair with every row of the other side.
	let others = || {
		datetime(
			"arg",
			[
				dt(2000, 1, 1, 0, 0, 0),
				dt(2010, 5, 5, 5, 5, 5),
				dt(1990, 3, 3, 3, 3, 3),
				dt(2022, 9, 9, 9, 9, 9),
			],
		)
	};
	assert_eq!(
		check("diff", &DateTimeDiff::new(), vec![datetimes(), others()], vec![dt_constants(), dt_constants()]),
		5
	);
	assert_eq!(
		check("age", &DateTimeAge::new(), vec![datetimes(), others()], vec![dt_constants(), dt_constants()]),
		5
	);
}

#[test]
fn new_broadcasts_a_constant_date_or_time() {
	// A constant date combined with a time column must keep every time, not only row 0.
	let dates = date(
		"arg",
		[
			Date::new(2024, 1, 15).unwrap(),
			Date::new(1999, 12, 31).unwrap(),
			Date::new(2020, 2, 29).unwrap(),
			Date::new(1970, 1, 1).unwrap(),
		],
	);
	let times = time(
		"arg",
		[
			Time::new(1, 2, 3, 4).unwrap(),
			Time::new(23, 59, 59, 0).unwrap(),
			Time::new(12, 0, 0, 500).unwrap(),
			Time::new(0, 0, 0, 0).unwrap(),
		],
	);
	let constants = vec![
		vec![Constant::Some(Value::Date(Date::new(2021, 7, 4).unwrap())), Constant::None(ValueType::Date)],
		vec![Constant::Some(Value::Time(Time::new(8, 30, 0, 0).unwrap())), Constant::None(ValueType::Time)],
	];
	assert_eq!(check("new", &DateTimeNew::new(), vec![dates, times], constants), 5);
}

#[test]
fn trunc_and_format_broadcast_a_constant_text_argument() {
	// The usual call passes the precision or pattern as a literal, so a 1 row text argument is the common case.
	let precisions = utf8("arg", ["year", "month", "hour", "second"]);
	let constants = vec![
		dt_constants(),
		vec![Constant::Some(Value::Utf8("day".to_string())), Constant::None(ValueType::Utf8)],
	];
	assert_eq!(check("trunc", &DateTimeTrunc::new(), vec![datetimes(), precisions], constants), 5);
	let patterns = utf8("arg", ["%Y", "%Y-%m-%d", "%H:%M:%S", "%d/%m/%Y %H"]);
	let constants = vec![
		dt_constants(),
		vec![Constant::Some(Value::Utf8("%Y-%m-%d %H:%M:%S".to_string())), Constant::None(ValueType::Utf8)],
	];
	assert_eq!(check("format", &DateTimeFormat::new(), vec![datetimes(), patterns], constants), 5);
}
