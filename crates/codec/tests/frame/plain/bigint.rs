// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	constraint::precision::Precision, container::decimal_array::int_array, frame::data::FrameColumnData, int::Int,
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

	crate::plain_tests! {
		typical: vec![int(0), int(i64::MAX as i128), int(i64::MIN as i128)],
		boundary: vec![int(0), int(-1), int(1), int(9_999_999_999_999_999_999_999_999_999_999_999_999), int(-9_999_999_999_999_999_999_999_999_999_999_999_999)],
		single: int(0),
	}
}

mod wide {
	use super::*;

	fn make(values: Vec<Int>) -> FrameColumnData {
		column(76, values)
	}

	crate::plain_tests! {
		typical: vec![int(0), int(i128::MAX), int(i128::MIN), Int::MAX, Int::MIN],
		boundary: vec![int(0), int(-1), int(1), Int::MAX, Int::MIN],
		single: Int::MAX,
	}
}
