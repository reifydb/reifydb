// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	constraint::precision::Precision, container::decimal_array::uint_array, frame::data::FrameColumnData,
	uint::Uint,
};

fn column(precision: u8, values: Vec<Uint>) -> FrameColumnData {
	FrameColumnData::Uint(uint_array(Precision::new(precision), values))
}

fn uint(value: u128) -> Uint {
	Uint::from_u128(value)
}

mod narrow {
	use super::*;

	fn make(values: Vec<Uint>) -> FrameColumnData {
		column(38, values)
	}

	crate::rle_tests! {
		repeated: {
			let mut v = Vec::new();
			for val in [0u128, 42, 1_000_000_000_000, u64::MAX as u128, 999] {
				v.extend(std::iter::repeat_n(uint(val), 100));
			}
			v
		},
		unique: (0..100u128).map(|i| uint(i * 997)).collect::<Vec<_>>(),
	}
}

mod wide {
	use super::*;

	fn make(values: Vec<Uint>) -> FrameColumnData {
		column(76, values)
	}

	crate::rle_tests! {
		repeated: {
			let mut v = Vec::new();
			for val in [uint(0), Uint::MAX, uint(1_000_000_000_000), uint(u128::MAX), uint(999)] {
				v.extend(std::iter::repeat_n(val, 100));
			}
			v
		},
		unique: (0..100u128).map(|i| uint(i * 997)).collect::<Vec<_>>(),
	}
}
