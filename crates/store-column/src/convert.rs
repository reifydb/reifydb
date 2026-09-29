// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{
	Array, ArrayRef as ArrowArrayRef, FixedSizeBinaryArray, FixedSizeListArray, Int32Array, Int64Array,
	IntervalMonthDayNanoArray, LargeBinaryArray, LargeStringArray, StructArray, TimestampNanosecondArray,
	UInt8Array,
	cast::AsArray,
	types::{
		Int32Type, Int64Type, IntervalMonthDayNano, IntervalMonthDayNanoType, TimestampNanosecondType,
		UInt8Type,
	},
};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field, FieldRef, Fields, IntervalUnit, TimeUnit};
use reifydb_value::{
	Result,
	value::{
		container::dictionary_array::DICTIONARY_ENTRY_WIDTH,
		value_type::field::{FieldType, to_field},
	},
};
use vortex_array::{ArrayRef, VortexSessionExecute};
use vortex_arrow::ArrowSessionExt;
use vortex_session::VortexSession;

use crate::error::{ColumnError, vortex};

pub fn to_vortex(session: &VortexSession, column: &(FieldRef, ArrowArrayRef)) -> Result<ArrayRef> {
	let (field, array) = column;
	let reshaped = reshape(array);
	let plain = Field::new(field.name(), reshaped.data_type().clone(), field.is_nullable());
	session.arrow().from_arrow_array(reshaped, &plain).map_err(vortex("import"))
}

pub fn to_arrow(
	session: &VortexSession,
	name: &str,
	field_type: &FieldType,
	array: ArrayRef,
) -> Result<(FieldRef, ArrowArrayRef)> {
	let target = to_field(name, field_type);
	let plain = Field::new(name, reshaped_type(target.data_type()), target.is_nullable());
	let mut ctx = session.create_execution_ctx();
	let exported = session.arrow().execute_arrow(array, Some(&plain), &mut ctx).map_err(vortex("export"))?;
	let restored = unshape(name, &exported, target.data_type())?;
	Ok((Arc::new(target), restored))
}

fn reshape(array: &ArrowArrayRef) -> ArrowArrayRef {
	match array.data_type() {
		DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
			let instants = array.as_primitive::<TimestampNanosecondType>();
			Arc::new(Int64Array::new(instants.values().clone(), instants.nulls().cloned()))
		}
		DataType::FixedSizeBinary(width) if *width as usize == DICTIONARY_ENTRY_WIDTH => {
			Arc::new(to_list(&reverse_entries(array.as_fixed_size_binary())))
		}
		DataType::FixedSizeBinary(_) => Arc::new(to_list(array.as_fixed_size_binary())),
		DataType::Interval(IntervalUnit::MonthDayNano) => {
			Arc::new(to_struct(array.as_primitive::<IntervalMonthDayNanoType>()))
		}
		DataType::LargeUtf8 => {
			let text = array.as_string::<i64>();
			Arc::new(LargeBinaryArray::new(
				text.offsets().clone(),
				text.values().clone(),
				text.nulls().cloned(),
			))
		}
		_ => Arc::clone(array),
	}
}

fn unshape(name: &str, array: &ArrowArrayRef, target: &DataType) -> Result<ArrowArrayRef> {
	Ok(match target {
		DataType::Timestamp(TimeUnit::Nanosecond, Some(zone)) => {
			let nanos = array.as_primitive::<Int64Type>();
			Arc::new(
				TimestampNanosecondArray::new(nanos.values().clone(), nanos.nulls().cloned())
					.with_timezone(zone.clone()),
			)
		}
		DataType::FixedSizeBinary(width) if *width as usize == DICTIONARY_ENTRY_WIDTH => {
			Arc::new(reverse_entries(&from_list(array.as_fixed_size_list())))
		}
		DataType::FixedSizeBinary(_) => Arc::new(from_list(array.as_fixed_size_list())),
		DataType::Interval(IntervalUnit::MonthDayNano) => Arc::new(from_struct(array.as_struct())),
		DataType::LargeUtf8 => {
			let bytes = array.as_binary::<i64>();
			let text = LargeStringArray::try_new(
				bytes.offsets().clone(),
				bytes.values().clone(),
				bytes.nulls().cloned(),
			)
			.map_err(|err| ColumnError::PersistDeserialize {
				reason: format!("column '{name}' holds bytes that are not utf8: {err}"),
			})?;
			Arc::new(text)
		}
		_ => Arc::clone(array),
	})
}

fn reshaped_type(data_type: &DataType) -> DataType {
	match data_type {
		DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => DataType::Int64,
		DataType::FixedSizeBinary(width) => DataType::FixedSizeList(Arc::new(byte_item()), *width),
		DataType::Interval(IntervalUnit::MonthDayNano) => DataType::Struct(duration_fields()),
		DataType::LargeUtf8 => DataType::LargeBinary,
		other => other.clone(),
	}
}

fn byte_item() -> Field {
	Field::new("item", DataType::UInt8, false)
}

fn duration_fields() -> Fields {
	Fields::from(vec![
		Field::new("months", DataType::Int32, false),
		Field::new("days", DataType::Int32, false),
		Field::new("nanos", DataType::Int64, false),
	])
}

fn to_list(array: &FixedSizeBinaryArray) -> FixedSizeListArray {
	let bytes = UInt8Array::new(ScalarBuffer::from(array.values().clone()), None);
	FixedSizeListArray::new(Arc::new(byte_item()), array.value_length(), Arc::new(bytes), array.nulls().cloned())
}

fn from_list(array: &FixedSizeListArray) -> FixedSizeBinaryArray {
	let bytes = array.values().as_primitive::<UInt8Type>().values().inner().clone();
	FixedSizeBinaryArray::new(array.value_length(), bytes, array.nulls().cloned())
}

fn reverse_entries(array: &FixedSizeBinaryArray) -> FixedSizeBinaryArray {
	let mut bytes = array.values().to_vec();
	for entry in bytes.chunks_exact_mut(DICTIONARY_ENTRY_WIDTH) {
		entry.reverse();
	}
	FixedSizeBinaryArray::new(DICTIONARY_ENTRY_WIDTH as i32, bytes.into(), array.nulls().cloned())
}

fn to_struct(array: &IntervalMonthDayNanoArray) -> StructArray {
	let values = array.values();
	let months = Int32Array::from_iter_values(values.iter().map(|v| v.months));
	let days = Int32Array::from_iter_values(values.iter().map(|v| v.days));
	let nanos = Int64Array::from_iter_values(values.iter().map(|v| v.nanoseconds));
	StructArray::new(
		duration_fields(),
		vec![Arc::new(months), Arc::new(days), Arc::new(nanos)],
		array.nulls().cloned(),
	)
}

fn from_struct(array: &StructArray) -> IntervalMonthDayNanoArray {
	let months = array.column(0).as_primitive::<Int32Type>();
	let days = array.column(1).as_primitive::<Int32Type>();
	let nanos = array.column(2).as_primitive::<Int64Type>();
	let values: Vec<IntervalMonthDayNano> = (0..array.len())
		.map(|i| IntervalMonthDayNano::new(months.value(i), days.value(i), nanos.value(i)))
		.collect();
	IntervalMonthDayNanoArray::new(ScalarBuffer::from(values), array.nulls().cloned())
}
