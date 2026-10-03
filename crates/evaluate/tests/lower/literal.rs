// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::common::{Env, boolean, duration, none, number, rows, temporal, text};

#[test]
fn every_constant_shape_lowers_to_the_field_and_array_of_the_old_path() {
	// A constant typed differently from today changes every compare and arithmetic built on it.
	let env = Env::new();
	let shapes = [
		none(),
		boolean("true"),
		boolean("false"),
		number("42"),
		number("300"),
		number("70000"),
		number("5000000000"),
		number("100000000000000000000"),
		number("1.5"),
		text("abc"),
		temporal("2024-01-02"),
		temporal("2024-01-02T03:04:05"),
		temporal("03:04:05"),
		temporal("P1D"),
		duration("30s"),
		duration("3mo"),
	];

	for shape in shapes {
		let lowered = env.lowered(&shape, rows(3)).unwrap();
		let old = env.old(&shape, rows(3)).unwrap();
		assert_eq!(lowered.0, old.0, "field of {shape:?}");
		assert_eq!(&lowered.1, &old.1, "array of {shape:?}");
	}
}

#[test]
fn a_constant_is_expanded_to_the_row_count_of_the_batch() {
	// A filter mask shorter than the batch would drop or panic on every row past the first.
	let env = Env::new();

	let result = env.lowered(&boolean("true"), rows(3)).unwrap();

	assert_eq!(result.1.len(), 3);
}

#[test]
fn an_invalid_constant_fails_with_the_same_error_as_the_old_path() {
	// A constant that does not parse must keep its error code and fragment, or goldens that pin it move.
	let env = Env::new();
	let bad = temporal("not-a-date");

	let lowered = env.lowered(&bad, rows(1)).unwrap_err();
	let old = env.old(&bad, rows(1)).unwrap_err();

	assert_eq!(lowered.code, old.code);
	assert_eq!(lowered.fragment.text(), old.fragment.text());
}
