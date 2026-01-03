// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Test helper functions for building Frame and FrameColumn instances.

use reifydb_type::{
	BitVec, Date, DateTime, Frame, FrameColumn, FrameColumnData, Time, Uuid4, Uuid7,
	value::container::{BoolContainer, NumberContainer, TemporalContainer, Utf8Container, UuidContainer},
};

/// Create a simple column with just a name.
fn column(name: &str, data: FrameColumnData) -> FrameColumn {
	FrameColumn {
		name: name.to_string(),
		data,
	}
}

pub fn int8_column(name: &str, values: Vec<i64>) -> FrameColumn {
	column(name, FrameColumnData::Int8(NumberContainer::from_vec(values)))
}

pub fn int4_column(name: &str, values: Vec<i32>) -> FrameColumn {
	column(name, FrameColumnData::Int4(NumberContainer::from_vec(values)))
}

pub fn int2_column(name: &str, values: Vec<i16>) -> FrameColumn {
	column(name, FrameColumnData::Int2(NumberContainer::from_vec(values)))
}

pub fn int1_column(name: &str, values: Vec<i8>) -> FrameColumn {
	column(name, FrameColumnData::Int1(NumberContainer::from_vec(values)))
}

pub fn int16_column(name: &str, values: Vec<i128>) -> FrameColumn {
	column(name, FrameColumnData::Int16(NumberContainer::from_vec(values)))
}

pub fn uint8_column(name: &str, values: Vec<u64>) -> FrameColumn {
	column(name, FrameColumnData::Uint8(NumberContainer::from_vec(values)))
}

pub fn uint4_column(name: &str, values: Vec<u32>) -> FrameColumn {
	column(name, FrameColumnData::Uint4(NumberContainer::from_vec(values)))
}

pub fn uint2_column(name: &str, values: Vec<u16>) -> FrameColumn {
	column(name, FrameColumnData::Uint2(NumberContainer::from_vec(values)))
}

pub fn uint1_column(name: &str, values: Vec<u8>) -> FrameColumn {
	column(name, FrameColumnData::Uint1(NumberContainer::from_vec(values)))
}

pub fn uint16_column(name: &str, values: Vec<u128>) -> FrameColumn {
	column(name, FrameColumnData::Uint16(NumberContainer::from_vec(values)))
}

pub fn float8_column(name: &str, values: Vec<f64>) -> FrameColumn {
	column(name, FrameColumnData::Float8(NumberContainer::from_vec(values)))
}

pub fn float4_column(name: &str, values: Vec<f32>) -> FrameColumn {
	column(name, FrameColumnData::Float4(NumberContainer::from_vec(values)))
}

pub fn bool_column(name: &str, values: Vec<bool>) -> FrameColumn {
	column(name, FrameColumnData::Bool(BoolContainer::from_vec(values)))
}

pub fn utf8_column(name: &str, values: Vec<&str>) -> FrameColumn {
	column(
		name,
		FrameColumnData::Utf8(Utf8Container::from_vec(values.into_iter().map(|s| s.to_string()).collect())),
	)
}

pub fn utf8_column_owned(name: &str, values: Vec<String>) -> FrameColumn {
	column(name, FrameColumnData::Utf8(Utf8Container::from_vec(values)))
}

pub fn date_column(name: &str, values: Vec<Date>) -> FrameColumn {
	column(name, FrameColumnData::Date(TemporalContainer::from_vec(values)))
}

pub fn datetime_column(name: &str, values: Vec<DateTime>) -> FrameColumn {
	column(name, FrameColumnData::DateTime(TemporalContainer::from_vec(values)))
}

pub fn time_column(name: &str, values: Vec<Time>) -> FrameColumn {
	column(name, FrameColumnData::Time(TemporalContainer::from_vec(values)))
}

pub fn uuid4_column(name: &str, values: Vec<Uuid4>) -> FrameColumn {
	column(name, FrameColumnData::Uuid4(UuidContainer::from_vec(values)))
}

pub fn uuid7_column(name: &str, values: Vec<Uuid7>) -> FrameColumn {
	column(name, FrameColumnData::Uuid7(UuidContainer::from_vec(values)))
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

	column(name, FrameColumnData::Int8(NumberContainer::new(data, BitVec::from_slice(&bits))))
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

	column(name, FrameColumnData::Utf8(Utf8Container::new(data, BitVec::from_slice(&bits))))
}

pub fn frame(columns: Vec<FrameColumn>) -> Frame {
	Frame::new(columns)
}
