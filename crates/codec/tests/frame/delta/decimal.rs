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

	crate::delta_tests! {
		ascending: (0..200).map(|i| dec(&format!("{i}.25"))).collect::<Vec<_>>(),
		descending: (0..200).rev().map(|i| dec(&format!("-{i}.5"))).collect::<Vec<_>>(),
		unsorted: (0..200).map(|i| dec(&format!("{}.1", (i * 7 + 13) % 97))).collect::<Vec<_>>(),
	}
}

mod wide {
	use super::*;

	fn make(values: Vec<Decimal>) -> FrameColumnData {
		column(76, values)
	}

	crate::delta_tests! {
		ascending: (0..200).map(|i| dec(&format!("99999999999999999999999999999999999999999999999999999999999999{i:05}.25"))).collect::<Vec<_>>(),
		descending: (0..200).rev().map(|i| dec(&format!("-99999999999999999999999999999999999999999999999999999999999999{i:05}.5"))).collect::<Vec<_>>(),
		unsorted: (0..200).map(|i| dec(&format!("{}.1", (i * 7 + 13) % 97))).collect::<Vec<_>>(),
	}
}
