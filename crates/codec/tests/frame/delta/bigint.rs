// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::i256;
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

	crate::delta_tests! {
		ascending: (0..200i128).map(int).collect::<Vec<_>>(),
		descending: (0..200i128).rev().map(|i| int(-i)).collect::<Vec<_>>(),
		unsorted: (0..200i128).map(|i| int((i * 7 + 13) % 97)).collect::<Vec<_>>(),
	}
}

mod wide {
	use super::*;

	fn make(values: Vec<Int>) -> FrameColumnData {
		column(76, values)
	}

	crate::delta_tests! {
		ascending: (0..200).map(|i| Int::from_i256(Int::MIN.to_i256().wrapping_add(i256::from_i128(i))).unwrap()).collect::<Vec<_>>(),
		descending: (0..200).map(|i| Int::from_i256(Int::MAX.to_i256().wrapping_sub(i256::from_i128(i))).unwrap()).collect::<Vec<_>>(),
		unsorted: (0..200i128).map(|i| int((i * 7 + 13) % 97)).collect::<Vec<_>>(),
	}
}
