// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{
	BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray,
	UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::BooleanBuffer;
use reifydb_value::value::{
	container::{
		temporal_array::{date_array, datetime_array, time_array},
		uuid_array::{uuid4_array, uuid7_array},
		wide_int_array::wide_array,
	},
	date::Date,
	datetime::DateTime,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	time::Time,
	uuid::{Uuid4, Uuid7},
};

fn column(name: &str, data: FrameColumnData) -> FrameColumn {
	FrameColumn {
		name: name.to_string(),
		data,
	}
}

pub fn int8_column(name: &str, values: Vec<i64>) -> FrameColumn {
	column(name, FrameColumnData::Int8(Int64Array::from(values)))
}

pub fn int4_column(name: &str, values: Vec<i32>) -> FrameColumn {
	column(name, FrameColumnData::Int4(Int32Array::from(values)))
}

pub fn int2_column(name: &str, values: Vec<i16>) -> FrameColumn {
	column(name, FrameColumnData::Int2(Int16Array::from(values)))
}

pub fn int1_column(name: &str, values: Vec<i8>) -> FrameColumn {
	column(name, FrameColumnData::Int1(Int8Array::from(values)))
}

pub fn int16_column(name: &str, values: Vec<i128>) -> FrameColumn {
	column(name, FrameColumnData::Int16(wide_array(values)))
}

pub fn uint8_column(name: &str, values: Vec<u64>) -> FrameColumn {
	column(name, FrameColumnData::Uint8(UInt64Array::from(values)))
}

pub fn uint4_column(name: &str, values: Vec<u32>) -> FrameColumn {
	column(name, FrameColumnData::Uint4(UInt32Array::from(values)))
}

pub fn uint2_column(name: &str, values: Vec<u16>) -> FrameColumn {
	column(name, FrameColumnData::Uint2(UInt16Array::from(values)))
}

pub fn uint1_column(name: &str, values: Vec<u8>) -> FrameColumn {
	column(name, FrameColumnData::Uint1(UInt8Array::from(values)))
}

pub fn uint16_column(name: &str, values: Vec<u128>) -> FrameColumn {
	column(name, FrameColumnData::Uint16(wide_array(values)))
}

pub fn float8_column(name: &str, values: Vec<f64>) -> FrameColumn {
	column(name, FrameColumnData::Float8(Float64Array::from(values)))
}

pub fn float4_column(name: &str, values: Vec<f32>) -> FrameColumn {
	column(name, FrameColumnData::Float4(Float32Array::from(values)))
}

pub fn bool_column(name: &str, values: Vec<bool>) -> FrameColumn {
	column(name, FrameColumnData::Bool(BooleanArray::from(values)))
}

pub fn utf8_column(name: &str, values: Vec<&str>) -> FrameColumn {
	column(
		name,
		FrameColumnData::Utf8(LargeStringArray::from(
			values.into_iter().map(|s| s.to_string()).collect::<Vec<String>>(),
		)),
	)
}

pub fn utf8_column_owned(name: &str, values: Vec<String>) -> FrameColumn {
	column(name, FrameColumnData::Utf8(LargeStringArray::from(values)))
}

pub fn date_column(name: &str, values: Vec<Date>) -> FrameColumn {
	column(name, FrameColumnData::Date(date_array(values)))
}

pub fn datetime_column(name: &str, values: Vec<DateTime>) -> FrameColumn {
	column(name, FrameColumnData::DateTime(datetime_array(values)))
}

pub fn time_column(name: &str, values: Vec<Time>) -> FrameColumn {
	column(name, FrameColumnData::Time(time_array(values)))
}

pub fn uuid4_column(name: &str, values: Vec<Uuid4>) -> FrameColumn {
	column(name, FrameColumnData::Uuid4(uuid4_array(values)))
}

pub fn uuid7_column(name: &str, values: Vec<Uuid7>) -> FrameColumn {
	column(name, FrameColumnData::Uuid7(uuid7_array(values)))
}

pub fn optional_int8_column(name: &str, values: Vec<Option<i64>>) -> FrameColumn {
	let len = values.len();
	let mut data = Vec::with_capacity(len);
	let mut bits = Vec::with_capacity(len);

	for v in values {
		match v {
			Some(val) => {
				data.push(val);
				bits.push(true);
			}
			None => {
				data.push(0);
				bits.push(false);
			}
		}
	}

	column(
		name,
		FrameColumnData::Option {
			inner: Box::new(FrameColumnData::Int8(Int64Array::from(data))),
			bitvec: BooleanBuffer::from(bits),
		},
	)
}

pub fn optional_utf8_column(name: &str, values: Vec<Option<&str>>) -> FrameColumn {
	let len = values.len();
	let mut data = Vec::with_capacity(len);
	let mut bits = Vec::with_capacity(len);

	for v in values {
		match v {
			Some(val) => {
				data.push(val.to_string());
				bits.push(true);
			}
			None => {
				data.push(String::new());
				bits.push(false);
			}
		}
	}

	column(
		name,
		FrameColumnData::Option {
			inner: Box::new(FrameColumnData::Utf8(LargeStringArray::from(data))),
			bitvec: BooleanBuffer::from(bits),
		},
	)
}

pub fn frame(columns: Vec<FrameColumn>) -> Frame {
	Frame::new(columns)
}
