// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{Value, digest::Digest, value_type::ValueType};

pub const PPM: u32 = 1_000;

pub const MEDIAN: &str = "total: stats::approx_percentile(v, 0.5, 0.001)";

pub const MEDIAN_NEXT_TO_MIN: &str = "total: stats::approx_percentile(v, 0.5, 0.001); lo: math::min(v)";

pub fn median(values: &[i64]) -> Value {
	assert!(!values.is_empty(), "a percentile over no live rows has no rank; the oracle must not publish one");
	let mut sorted = values.to_vec();
	sorted.sort_unstable();
	let rank = sorted.len().div_ceil(2);
	bucketed(sorted[rank - 1])
}

pub fn no_rows() -> Value {
	Value::none_of(ValueType::Float8)
}

pub fn bucketed(value: i64) -> Value {
	// One value in a fresh digest reads back its bucket representative with no merge, unmerge or remove.
	let mut one = Digest::new(ValueType::Int8, PPM).expect("the oracle accuracy is in range");
	one.add_value(&Value::Int8(value)).expect("an int8 is a digest input");
	one.percentile_value(1.0).expect("a one-value digest has a percentile")
}
