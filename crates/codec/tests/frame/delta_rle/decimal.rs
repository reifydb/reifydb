// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	constraint::{precision::Precision, scale::Scale},
	container::decimal_array::decimal_array,
	decimal::Decimal,
	frame::data::FrameColumnData,
};

fn column(precision: u8, values: Vec<Decimal>) -> FrameColumnData {
	FrameColumnData::Decimal(decimal_array(Precision::new(precision), Scale::new(9), values))
}

fn dec(text: &str) -> Decimal {
	Decimal::parse(text).expect("valid decimal literal")
}

mod narrow {
	use super::*;

	fn make(values: Vec<Decimal>) -> FrameColumnData {
		column(38, values)
	}

	crate::delta_rle_tests! {
		constant_stride: (0..200).map(|i| dec(&format!("{i}.5"))).collect::<Vec<_>>(),
		descending_stride: (0..200).map(|i| dec(&format!("-{i}.5"))).collect::<Vec<_>>(),
	}
}

mod wide {
	use super::*;

	fn make(values: Vec<Decimal>) -> FrameColumnData {
		column(76, values)
	}

	crate::delta_rle_tests! {
		constant_stride: (0..200).map(|i| dec(&format!("99999999999999999999999999999999999999999999999999999999999999{i:05}.5"))).collect::<Vec<_>>(),
		descending_stride: (0..200).map(|i| dec(&format!("-99999999999999999999999999999999999999999999999999999999999999{i:05}.5"))).collect::<Vec<_>>(),
	}
}
