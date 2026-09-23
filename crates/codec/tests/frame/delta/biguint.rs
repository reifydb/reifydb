// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::i256;
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

	crate::delta_tests! {
		ascending: (0..200u128).map(uint).collect::<Vec<_>>(),
		descending: (0..200u128).rev().map(uint).collect::<Vec<_>>(),
		unsorted: (0..200u128).map(|i| uint((i * 7 + 13) % 97)).collect::<Vec<_>>(),
	}
}

mod wide {
	use super::*;

	fn make(values: Vec<Uint>) -> FrameColumnData {
		column(76, values)
	}

	crate::delta_tests! {
		ascending: (0..200).map(|i| Uint::from_i256(Uint::MAX.to_i256().wrapping_sub(i256::from_i128(200 - i))).unwrap()).collect::<Vec<_>>(),
		descending: (0..200).map(|i| Uint::from_i256(Uint::MAX.to_i256().wrapping_sub(i256::from_i128(i))).unwrap()).collect::<Vec<_>>(),
		unsorted: (0..200u128).map(|i| uint((i * 7 + 13) % 97)).collect::<Vec<_>>(),
	}
}
