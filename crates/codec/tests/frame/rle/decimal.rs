// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	constraint::{precision::Precision, scale::Scale},
	container::decimal_array::decimal_array,
	decimal::Decimal,
	value_type::{ValueType, field::FieldType},
};

use crate::common::ColumnData;

fn column(precision: u8, values: Vec<Decimal>) -> ColumnData {
	(
		FieldType::from(ValueType::decimal(Precision::new(precision), Scale::new(9))),
		decimal_array(Precision::new(precision), Scale::new(9), values).into_array(),
	)
}

fn dec(text: &str) -> Decimal {
	Decimal::parse(text).expect("valid decimal literal")
}

mod narrow {
	use super::*;

	fn make(values: Vec<Decimal>) -> ColumnData {
		column(38, values)
	}

	crate::rle_tests! {
		repeated: {
			let mut v = Vec::new();
			for s in ["0.00", "99.99", "-123.456", "1000000.001", "0.000001"] {
				v.extend(std::iter::repeat_n(dec(s), 100));
			}
			v
		},
		unique: (0..100i64).map(|i| dec(&format!("{}.{}", i * 7, i % 100))).collect::<Vec<_>>(),
	}
}

mod wide {
	use super::*;

	fn make(values: Vec<Decimal>) -> ColumnData {
		column(76, values)
	}

	crate::rle_tests! {
		repeated: {
			let mut v = Vec::new();
			for s in ["0.00", "9999999999999999999999999999999999999999999999999999999999999999999.999999999", "-123.456", "-9999999999999999999999999999999999999999999999999999999999999999999.999999999", "0.000001"] {
				v.extend(std::iter::repeat_n(dec(s), 100));
			}
			v
		},
		unique: (0..100i64).map(|i| dec(&format!("{}.{}", i * 7, i % 100))).collect::<Vec<_>>(),
	}
}
