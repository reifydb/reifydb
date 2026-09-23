// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::str::FromStr;

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::value::{
	constraint::{precision::Precision, scale::Scale},
	decimal::Decimal,
};

use super::common::{assert_column_eq, round_trip_column};

const WIDTHS: [Precision; 2] = [Precision::new(38), Precision::MAX];

fn round_trip_at_every_width(label: &str, scale: Scale, values: &[Decimal]) {
	for precision in WIDTHS {
		let input = ColumnBuffer::decimal(precision, scale, values.iter().cloned());
		let output = round_trip_column("d", input.clone());
		assert_column_eq(&format!("{label} at precision {}", precision.value()), &input, &output);
	}
}

#[test]
fn decimal_zero() {
	round_trip_at_every_width("decimal_zero", Scale::new(0), &[Decimal::zero()]);
}

#[test]
fn decimal_small_values() {
	round_trip_at_every_width(
		"decimal_small",
		Scale::new(20),
		&[
			Decimal::from_i64(1),
			Decimal::from_i64(-1),
			Decimal::from_str("3.14159265358979323846").expect("parse"),
			Decimal::from_str("-2.71828182845904523536").expect("parse"),
		],
	);
}

#[test]
fn decimal_narrow_width_edge() {
	// 38 digits at scale 10 fill a 16 byte cell; dropping the scale on the way back reads them as integers.
	round_trip_at_every_width(
		"decimal_narrow_edge",
		Scale::new(10),
		&[
			Decimal::from_str("9999999999999999999999999999.9999999999").expect("parse"),
			Decimal::from_str("-9999999999999999999999999999.9999999999").expect("parse"),
		],
	);
}

#[test]
fn decimal_high_precision() {
	// 32 integer digits at scale 31 need 63 digits, so only the 32 byte cell holds the column.
	let input = ColumnBuffer::decimal(
		Precision::MAX,
		Scale::new(31),
		[
			Decimal::from_str("0.0000000000000000000000000000001").expect("parse"),
			Decimal::from_str("99999999999999999999999999999999").expect("parse"),
		],
	);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("decimal_high_precision", &input, &output);
}

#[test]
fn decimal_thirty_two_rows() {
	let values: Vec<Decimal> = (0..32i64).map(Decimal::from_i64).collect();
	round_trip_at_every_width("decimal_thirty_two", Scale::new(2), &values);
}

#[test]
fn decimal_with_undefined() {
	for precision in WIDTHS {
		let input = ColumnBuffer::decimal_with_bitvec(
			precision,
			Scale::new(4),
			[
				Decimal::from_i64(42),
				Decimal::default(),
				Decimal::zero(),
				Decimal::default(),
				Decimal::one(),
			],
			vec![true, false, true, false, true],
		);
		let output = round_trip_column("d", input.clone());
		assert_column_eq(
			&format!("decimal_with_undefined at precision {}", precision.value()),
			&input,
			&output,
		);
	}
}
