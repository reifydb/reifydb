// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{result::Result as StdResult, slice};

use arrow_array::{Date32Array, IntervalMonthDayNanoArray, PrimitiveArray, Time64NanosecondArray, UInt64Array};
use arrow_buffer::{IntervalMonthDayNano, ScalarBuffer};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::value::{Value, date::Date, datetime::DateTime, duration::Duration, is::IsTemporal, time::Time};

pub fn dates(array: &Date32Array) -> &[Date] {
	let values: &[i32] = array.values();
	// SAFETY: Date is repr(transparent) over i32 with no niche, so the cast keeps the bounds and lifetime.
	unsafe { slice::from_raw_parts(values.as_ptr().cast::<Date>(), values.len()) }
}

pub fn datetimes(array: &UInt64Array) -> &[DateTime] {
	let values: &[u64] = array.values();
	// SAFETY: DateTime is repr(transparent) over u64 with no niche, so the cast keeps the bounds and lifetime.
	unsafe { slice::from_raw_parts(values.as_ptr().cast::<DateTime>(), values.len()) }
}

pub fn times(array: &Time64NanosecondArray) -> &[Time] {
	let values: &[i64] = array.values();
	// SAFETY: Time is repr(transparent) over u64, which has the size and alignment of i64 and no niche.
	unsafe { slice::from_raw_parts(values.as_ptr().cast::<Time>(), values.len()) }
}

pub fn durations(array: &IntervalMonthDayNanoArray) -> &[Duration] {
	let values: &[IntervalMonthDayNano] = array.values();
	// SAFETY: Duration and IntervalMonthDayNano are both repr(C) {i32, i32, i64} with no niche.
	unsafe { slice::from_raw_parts(values.as_ptr().cast::<Duration>(), values.len()) }
}

pub fn date_to_native(value: Date) -> i32 {
	value.to_days_since_epoch()
}

pub fn datetime_to_native(value: DateTime) -> u64 {
	value.to_nanos()
}

pub fn time_to_native(value: Time) -> i64 {
	value.to_nanos_since_midnight() as i64
}

pub fn duration_to_native(value: Duration) -> IntervalMonthDayNano {
	IntervalMonthDayNano::new(value.get_months(), value.get_days(), value.get_nanos())
}

pub fn date_array(values: impl IntoIterator<Item = Date>) -> Date32Array {
	let natives: Vec<i32> = values.into_iter().map(date_to_native).collect();
	PrimitiveArray::new(ScalarBuffer::from(natives), None)
}

pub fn datetime_array(values: impl IntoIterator<Item = DateTime>) -> UInt64Array {
	let natives: Vec<u64> = values.into_iter().map(datetime_to_native).collect();
	PrimitiveArray::new(ScalarBuffer::from(natives), None)
}

pub fn time_array(values: impl IntoIterator<Item = Time>) -> Time64NanosecondArray {
	let natives: Vec<i64> = values.into_iter().map(time_to_native).collect();
	PrimitiveArray::new(ScalarBuffer::from(natives), None)
}

pub fn duration_array(values: impl IntoIterator<Item = Duration>) -> IntervalMonthDayNanoArray {
	let natives: Vec<IntervalMonthDayNano> = values.into_iter().map(duration_to_native).collect();
	PrimitiveArray::new(ScalarBuffer::from(natives), None)
}

pub fn get_value<T: IsTemporal + Copy>(values: &[T], index: usize) -> Value {
	if index < values.len() {
		values[index].to_value()
	} else {
		Value::none()
	}
}

pub fn as_string<T: IsTemporal>(values: &[T], index: usize) -> String {
	if index < values.len() {
		values[index].to_string()
	} else {
		"none".to_string()
	}
}

fn serialize_values<T, Ser>(data: &[T], serializer: Ser) -> StdResult<Ser::Ok, Ser::Error>
where
	T: Serialize,
	Ser: Serializer,
{
	#[derive(Serialize)]
	struct Helper<'a, T: Serialize> {
		data: &'a [T],
	}
	Helper {
		data,
	}
	.serialize(serializer)
}

fn deserialize_values<'de, T, D>(deserializer: D) -> StdResult<Vec<T>, D::Error>
where
	T: Deserialize<'de>,
	D: Deserializer<'de>,
{
	#[derive(Deserialize)]
	struct Helper<T> {
		data: Vec<T>,
	}
	Ok(Helper::<T>::deserialize(deserializer)?.data)
}

pub fn serialize_dates<Ser: Serializer>(array: &Date32Array, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
	serialize_values(dates(array), serializer)
}

pub fn deserialize_dates<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<Date32Array, D::Error> {
	Ok(date_array(deserialize_values::<Date, D>(deserializer)?))
}

pub fn serialize_datetimes<Ser: Serializer>(array: &UInt64Array, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
	serialize_values(datetimes(array), serializer)
}

pub fn deserialize_datetimes<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<UInt64Array, D::Error> {
	Ok(datetime_array(deserialize_values::<DateTime, D>(deserializer)?))
}

pub fn serialize_times<Ser: Serializer>(
	array: &Time64NanosecondArray,
	serializer: Ser,
) -> StdResult<Ser::Ok, Ser::Error> {
	serialize_values(times(array), serializer)
}

pub fn deserialize_times<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<Time64NanosecondArray, D::Error> {
	Ok(time_array(deserialize_values::<Time, D>(deserializer)?))
}

pub fn serialize_durations<Ser: Serializer>(
	array: &IntervalMonthDayNanoArray,
	serializer: Ser,
) -> StdResult<Ser::Ok, Ser::Error> {
	serialize_values(durations(array), serializer)
}

pub fn deserialize_durations<'de, D: Deserializer<'de>>(
	deserializer: D,
) -> StdResult<IntervalMonthDayNanoArray, D::Error> {
	Ok(duration_array(deserialize_values::<Duration, D>(deserializer)?))
}
