// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	constraint::precision::Precision, container::decimal_array::uint_array, frame::data::FrameColumnData,
	uint::Uint, value_type::ValueType,
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

	crate::nones_tests! {
		values: vec![uint(0), uint(u64::MAX as u128), uint(1), uint(42), uint(1_000_000)],
		inner_type: ValueType::uint(Precision::new(38)),
	}
}

mod wide {
	use super::*;

	fn make(values: Vec<Uint>) -> FrameColumnData {
		column(76, values)
	}

	crate::nones_tests! {
		values: vec![uint(0), Uint::MAX, uint(1), uint(42), uint(1_000_000)],
		inner_type: ValueType::uint(Precision::new(76)),
	}
}
