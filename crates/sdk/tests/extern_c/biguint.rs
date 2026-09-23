// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::value::{constraint::precision::Precision, uint::Uint};

use super::common::{assert_column_eq, round_trip_column};

const WIDTHS: [Precision; 2] = [Precision::new(38), Precision::MAX];

fn round_trip_at_every_width(label: &str, values: &[Uint]) {
	for precision in WIDTHS {
		let input = ColumnBuffer::uint(precision, values.iter().cloned());
		let output = round_trip_column("u", input.clone());
		assert_column_eq(&format!("{label} at precision {}", precision.value()), &input, &output);
	}
}

#[test]
fn biguint_zero() {
	round_trip_at_every_width("biguint_zero", &[Uint::zero()]);
}

#[test]
fn biguint_small() {
	round_trip_at_every_width("biguint_small", &[Uint::from_u64(1), Uint::from_u64(42), Uint::from_u64(u64::MAX)]);
}

#[test]
fn biguint_narrow_width_edge() {
	// The largest 38 digit value sets bit 126 of a 16 byte cell; a read of only the low 64 bits loses it.
	round_trip_at_every_width(
		"biguint_narrow_edge",
		&[Uint::parse("99999999999999999999999999999999999999").unwrap()],
	);
}

#[test]
fn biguint_u128_max() {
	// u128::MAX has 39 digits and sets the sign bit of an i128, so only the 32 byte cell holds it unchanged.
	let input = ColumnBuffer::uint(Precision::MAX, [Uint::from_u128(u128::MAX)]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("biguint_u128_max", &input, &output);
}

#[test]
fn biguint_outside_u128_range() {
	// Values past u128 use the high half of the 32 byte cell, which a narrow read would drop.
	let big = Uint::from_u128(u128::MAX).checked_add(&Uint::from_u128(u128::MAX)).unwrap();
	let input = ColumnBuffer::uint(Precision::MAX, [big, Uint::MAX]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("biguint_outside_u128", &input, &output);
}

#[test]
fn biguint_thirty_two_rows() {
	let values: Vec<Uint> = (0..32u64).map(Uint::from_u64).collect();
	round_trip_at_every_width("biguint_thirty_two", &values);
}

#[test]
fn biguint_with_undefined() {
	for precision in WIDTHS {
		let input = ColumnBuffer::uint_with_bitvec(
			precision,
			[Uint::from_u64(7), Uint::default(), Uint::zero(), Uint::default(), Uint::one()],
			vec![true, false, true, false, true],
		);
		let output = round_trip_column("u", input.clone());
		assert_column_eq(
			&format!("biguint_with_undefined at precision {}", precision.value()),
			&input,
			&output,
		);
	}
}
