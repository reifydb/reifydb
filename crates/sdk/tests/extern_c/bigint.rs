// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::value::{constraint::precision::Precision, int::Int};

use super::common::{assert_column_eq, round_trip_column};

const WIDTHS: [Precision; 2] = [Precision::new(38), Precision::MAX];

fn round_trip_at_every_width(label: &str, values: &[Int]) {
	for precision in WIDTHS {
		let input = ColumnBuffer::int(precision, values.iter().cloned());
		let output = round_trip_column("i", input.clone());
		assert_column_eq(&format!("{label} at precision {}", precision.value()), &input, &output);
	}
}

#[test]
fn bigint_zero() {
	round_trip_at_every_width("bigint_zero", &[Int::zero()]);
}

#[test]
fn bigint_small_positive() {
	round_trip_at_every_width("bigint_small", &[Int::from_i64(1), Int::from_i64(42), Int::from_i64(-1)]);
}

#[test]
fn bigint_narrow_width_edge() {
	// The largest 38 digit magnitudes fill a 16 byte cell; a lost sign extension flips the negative one.
	let edge = Int::parse("99999999999999999999999999999999999999").unwrap();
	round_trip_at_every_width("bigint_narrow_edge", &[edge.clone(), edge.negate()]);
}

#[test]
fn bigint_i128_max_min() {
	// i128 extremes have 39 digits, so only the 32 byte cell holds them; a 16 byte copy would truncate the top.
	let input = ColumnBuffer::int(Precision::MAX, [Int::from_i128(i128::MIN), Int::from_i128(i128::MAX)]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("bigint_i128_extremes", &input, &output);
}

#[test]
fn bigint_outside_i128_range() {
	// Values past i128 use the high half of the 32 byte cell, which a narrow read would drop.
	let big = Int::from_i128(i128::MAX).checked_add(&Int::from_i128(i128::MAX)).unwrap();
	let neg_big = Int::from_i128(i128::MIN).checked_add(&Int::from_i128(i128::MIN)).unwrap();
	let input = ColumnBuffer::int(Precision::MAX, [big, neg_big, Int::MAX, Int::MIN]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("bigint_outside_i128", &input, &output);
}

#[test]
fn bigint_thirty_two_rows() {
	let values: Vec<Int> = (0..32i64).map(Int::from_i64).collect();
	round_trip_at_every_width("bigint_thirty_two", &values);
}

#[test]
fn bigint_with_undefined() {
	for precision in WIDTHS {
		let input = ColumnBuffer::int_with_bitvec(
			precision,
			[Int::from_i64(7), Int::default(), Int::zero(), Int::default(), Int::one()],
			vec![true, false, true, false, true],
		);
		let output = round_trip_column("i", input.clone());
		assert_column_eq(&format!("bigint_with_undefined at precision {}", precision.value()), &input, &output);
	}
}
