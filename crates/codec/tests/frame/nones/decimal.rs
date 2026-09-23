// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	constraint::{precision::Precision, scale::Scale},
	container::decimal_array::decimal_array,
	decimal::Decimal,
	frame::data::FrameColumnData,
	value_type::ValueType,
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

	crate::nones_tests! {
		values: vec![dec("0"), dec("123.456"), dec("-99.99"), dec("0.000001"), dec("-999999999.999999999")],
		inner_type: ValueType::decimal(Precision::new(38), Scale::new(9)),
	}
}

mod wide {
	use super::*;

	fn make(values: Vec<Decimal>) -> FrameColumnData {
		column(76, values)
	}

	crate::nones_tests! {
		values: vec![dec("0"), dec("9999999999999999999999999999999999999999999999999999999999999999999.999999999"), dec("-99.99"), dec("0.000001"), dec("-9999999999999999999999999999999999999999999999999999999999999999999.999999999")],
		inner_type: ValueType::decimal(Precision::new(76), Scale::new(9)),
	}
}
