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

	crate::plain_tests! {
		typical: vec![uint(0), uint(u64::MAX as u128)],
		boundary: vec![uint(0), uint(1), uint(9_999_999_999_999_999_999_999_999_999_999_999_999)],
		single: uint(0),
	}
}

mod wide {
	use super::*;

	fn make(values: Vec<Uint>) -> FrameColumnData {
		column(76, values)
	}

	crate::plain_tests! {
		typical: vec![uint(0), uint(u128::MAX), Uint::MAX],
		boundary: vec![uint(0), uint(1), Uint::MAX],
		single: Uint::MAX,
	}
}
