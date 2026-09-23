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

	crate::rle_tests! {
		repeated: {
			let mut v = Vec::new();
			for val in [42i128, -1, 1_000_000_000_000, 0, 999] {
				v.extend(std::iter::repeat_n(int(val), 100));
			}
			v
		},
		unique: (0..100i128).map(|i| int(i * 997)).collect::<Vec<_>>(),
	}
}

mod wide {
	use super::*;

	fn make(values: Vec<Int>) -> FrameColumnData {
		column(76, values)
	}

	crate::rle_tests! {
		repeated: {
			let mut v = Vec::new();
			for val in [Int::MAX, int(-1), Int::MIN, int(0), int(i128::MAX)] {
				v.extend(std::iter::repeat_n(val, 100));
			}
			v
		},
		unique: (0..100i128).map(|i| int(i * 997)).collect::<Vec<_>>(),
	}
}
