// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine::function::{
	datetime::{from_epoch::DateTimeFromEpoch, from_epoch_millis::DateTimeFromEpochMillis},
	duration::{
		days::DurationDays, hours::DurationHours, millis::DurationMillis, minutes::DurationMinutes,
		months::DurationMonths, scale::DurationScale, seconds::DurationSeconds, weeks::DurationWeeks,
		years::DurationYears,
	},
	time::new::TimeNew,
};
use reifydb_routine_abi::{Routine, context::FunctionContext, error::RoutineError};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value, container::temporal_array::duration_array, datetime::DateTime, duration::Duration,
		identity::IdentityId, time::Time,
	},
};

const ABOVE_U64: u128 = 1 << 64;

const INT16_BEYOND_U64: [i128; 2] = [ABOVE_U64 as i128, i128::MAX];

const UINT16_BEYOND_U64: [u128; 2] = [ABOVE_U64, u128::MAX];

fn ctx(name: &str) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal(name),
		identity: IdentityId::root(),
		row_count: 1,
		runtime_context: &RUNTIME,
	}
}

fn int16(value: i128) -> ColumnWithName {
	ColumnWithName::new("x", ColumnBuffer::int16([value]))
}

fn uint16(value: u128) -> ColumnWithName {
	ColumnWithName::new("x", ColumnBuffer::uint16([value]))
}

fn durations(value: Duration) -> ColumnWithName {
	ColumnWithName::new("d", ColumnBuffer::Duration(duration_array([value])))
}

fn call(
	routine: &dyn Routine<FunctionContext<'static>>,
	name: &str,
	args: Vec<ColumnWithName>,
) -> Result<Columns, RoutineError> {
	routine.call(&mut ctx(name), &Columns::new(args))
}

fn first_value(result: Result<Columns, RoutineError>) -> Value {
	result.expect("an argument that fits must not fail")[0].get_value(0)
}

fn silent_value(result: Result<Columns, RoutineError>) -> Option<Value> {
	match result {
		Err(_) => None,
		Ok(columns) => match columns[0].get_value(0) {
			Value::None {
				..
			} => None,
			value => Some(value),
		},
	}
}

fn silent_values<T: Copy>(inputs: &[T], mut run: impl FnMut(T) -> Result<Columns, RoutineError>) -> Vec<(T, Value)> {
	inputs.iter().filter_map(|&input| silent_value(run(input)).map(|value| (input, value))).collect()
}

macro_rules! single_argument_suite {
	($module:ident, $routine:expr, $name:literal, $fits:expr, $expected:expr) => {
		mod $module {
			use super::*;

			#[test]
			fn int16_fits() {
				// An Int16 argument must give exactly its number, never a misread row.
				assert_eq!(first_value(call(&$routine, $name, vec![int16($fits as i128)])), $expected);
			}

			#[test]
			fn uint16_fits() {
				// A Uint16 argument must give exactly its number, never a signed or 64-bit misread.
				assert_eq!(first_value(call(&$routine, $name, vec![uint16($fits as u128)])), $expected);
			}

			#[test]
			fn int16_beyond_u64_is_error_or_none() {
				// An Int16 beyond 64 bits must fail or give none, never wrap into a defined row.
				let wrong =
					silent_values(&INT16_BEYOND_U64, |v| call(&$routine, $name, vec![int16(v)]));
				assert!(
					wrong.is_empty(),
					"{} gave defined rows for Int16 arguments beyond 64 bits: {:?}",
					$name,
					wrong
				);
			}

			#[test]
			fn uint16_beyond_u64_is_error_or_none() {
				// A Uint16 beyond 64 bits must fail or give none, never wrap into a defined row.
				let wrong =
					silent_values(&UINT16_BEYOND_U64, |v| call(&$routine, $name, vec![uint16(v)]));
				assert!(
					wrong.is_empty(),
					"{} gave defined rows for Uint16 arguments beyond 64 bits: {:?}",
					$name,
					wrong
				);
			}
		}
	};
}

single_argument_suite!(
	duration_years,
	DurationYears::new(),
	"duration::years",
	7,
	Value::Duration(Duration::from_years(7).unwrap())
);
single_argument_suite!(
	duration_months,
	DurationMonths::new(),
	"duration::months",
	7,
	Value::Duration(Duration::from_months(7).unwrap())
);
single_argument_suite!(
	duration_weeks,
	DurationWeeks::new(),
	"duration::weeks",
	7,
	Value::Duration(Duration::from_weeks(7).unwrap())
);
single_argument_suite!(
	duration_days,
	DurationDays::new(),
	"duration::days",
	7,
	Value::Duration(Duration::from_days(7).unwrap())
);
single_argument_suite!(
	duration_hours,
	DurationHours::new(),
	"duration::hours",
	7,
	Value::Duration(Duration::from_hours(7).unwrap())
);
single_argument_suite!(
	duration_minutes,
	DurationMinutes::new(),
	"duration::minutes",
	7,
	Value::Duration(Duration::from_minutes(7).unwrap())
);
single_argument_suite!(
	duration_seconds,
	DurationSeconds::new(),
	"duration::seconds",
	7,
	Value::Duration(Duration::from_seconds(7).unwrap())
);
single_argument_suite!(
	duration_millis,
	DurationMillis::new(),
	"duration::millis",
	5_000_000_000i64,
	Value::Duration(Duration::from_milliseconds(5_000_000_000).unwrap())
);
single_argument_suite!(
	datetime_from_epoch,
	DateTimeFromEpoch::new(),
	"datetime::from_epoch",
	1_700_000_000i64,
	Value::DateTime(DateTime::from_epoch_secs(1_700_000_000).unwrap())
);
single_argument_suite!(
	datetime_from_epoch_millis,
	DateTimeFromEpochMillis::new(),
	"datetime::from_epoch_millis",
	1_700_000_000_123i64,
	Value::DateTime(DateTime::from_epoch_millis(1_700_000_000_123).unwrap())
);

#[test]
fn duration_scale_int16_fits() {
	// An Int16 factor must scale by exactly its number, never by one read at a wrong offset or width.
	let result = call(
		&DurationScale::new(),
		"duration::scale",
		vec![durations(Duration::from_hours(2).unwrap()), int16(3)],
	);
	assert_eq!(first_value(result), Value::Duration(Duration::from_hours(6).unwrap()));
}

#[test]
fn duration_scale_uint16_fits() {
	// A Uint16 factor must scale by exactly its number, never by one read through a signed or 64-bit view.
	let result = call(
		&DurationScale::new(),
		"duration::scale",
		vec![durations(Duration::from_hours(2).unwrap()), uint16(3)],
	);
	assert_eq!(first_value(result), Value::Duration(Duration::from_hours(6).unwrap()));
}

#[test]
fn duration_scale_int16_beyond_u64_is_error_or_none() {
	// An Int16 factor beyond 64 bits must fail or give none, never wrap to 0 or -1 and scale the duration by that.
	let wrong = silent_values(&INT16_BEYOND_U64, |v| {
		call(
			&DurationScale::new(),
			"duration::scale",
			vec![durations(Duration::from_hours(2).unwrap()), int16(v)],
		)
	});
	assert!(wrong.is_empty(), "duration::scale gave defined rows for Int16 factors beyond 64 bits: {:?}", wrong);
}

#[test]
fn duration_scale_uint16_beyond_u64_is_error_or_none() {
	// A Uint16 factor beyond 64 bits must fail or give none, never wrap to 0 or -1 and scale the duration by that.
	let wrong = silent_values(&UINT16_BEYOND_U64, |v| {
		call(
			&DurationScale::new(),
			"duration::scale",
			vec![durations(Duration::from_hours(2).unwrap()), uint16(v)],
		)
	});
	assert!(wrong.is_empty(), "duration::scale gave defined rows for Uint16 factors beyond 64 bits: {:?}", wrong);
}

#[test]
fn time_new_int16_fits() {
	// Int16 hour, minute, second and nano must build exactly that time, never one read at a wrong offset or width.
	let result = call(&TimeNew::new(), "time::new", vec![int16(13), int16(45), int16(30), int16(500)]);
	assert_eq!(first_value(result), Value::Time(Time::new(13, 45, 30, 500).unwrap()));
}

#[test]
fn time_new_uint16_fits() {
	// Uint16 hour, minute, second and nano must build exactly that time, never a signed or 64-bit misread.
	let result = call(&TimeNew::new(), "time::new", vec![uint16(13), uint16(45), uint16(30), uint16(500)]);
	assert_eq!(first_value(result), Value::Time(Time::new(13, 45, 30, 500).unwrap()));
}

#[test]
fn time_new_int16_hour_beyond_u64_is_error_or_none() {
	// An Int16 hour beyond 64 bits must fail or give none, never wrap to a valid hour or fall back to midnight.
	let wrong = silent_values(&INT16_BEYOND_U64, |v| {
		call(&TimeNew::new(), "time::new", vec![int16(v), int16(45), int16(30)])
	});
	assert!(wrong.is_empty(), "time::new gave defined rows for Int16 hours beyond 64 bits: {:?}", wrong);
}

#[test]
fn time_new_uint16_hour_beyond_u64_is_error_or_none() {
	// A Uint16 hour beyond 64 bits must fail or give none, never wrap to a valid hour or fall back to midnight.
	let wrong = silent_values(&UINT16_BEYOND_U64, |v| {
		call(&TimeNew::new(), "time::new", vec![uint16(v), uint16(45), uint16(30)])
	});
	assert!(wrong.is_empty(), "time::new gave defined rows for Uint16 hours beyond 64 bits: {:?}", wrong);
}
