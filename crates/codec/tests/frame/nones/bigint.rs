// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	constraint::precision::Precision, container::decimal_array::int_array, frame::data::FrameColumnData, int::Int,
	value_type::ValueType,
};

fn column(precision: u8, values: Vec<Int>) -> FrameColumnData {
	FrameColumnData::Int(int_array(Precision::new(precision), values))
}

fn int(value: i128) -> Int {
	Int::from_i128(value)
}

mod narrow {
	use super::*;

	fn make(values: Vec<Int>) -> FrameColumnData {
		column(38, values)
	}

	crate::nones_tests! {
		values: vec![int(0), int(i64::MAX as i128), int(i64::MIN as i128), int(42), int(-999)],
		inner_type: ValueType::int(Precision::new(38)),
	}
}

mod wide {
	use super::*;

	fn make(values: Vec<Int>) -> FrameColumnData {
		column(76, values)
	}

	crate::nones_tests! {
		values: vec![int(0), Int::MAX, Int::MIN, int(42), int(-999)],
		inner_type: ValueType::int(Precision::new(76)),
	}
}
