// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::{from_bytes, to_allocvec};
use reifydb_value::value::{ordered_f32::OrderedF32, ordered_f64::OrderedF64};

#[test]
fn deserializing_nan_into_an_ordered_float_is_an_error_like_constructing_one() {
	// The constructors reject NaN, so a deserialized NaN must fail too or it breaks the total order.
	assert!(OrderedF64::try_from(f64::NAN).is_err(), "the f64 constructor must reject NaN");
	assert!(OrderedF32::try_from(f32::NAN).is_err(), "the f32 constructor must reject NaN");

	let f64_result = from_bytes::<OrderedF64>(&to_allocvec(&f64::NAN).unwrap());
	let f32_result = from_bytes::<OrderedF32>(&to_allocvec(&f32::NAN).unwrap());

	assert!(
		f64_result.is_err() && f32_result.is_err(),
		"NaN must not deserialize into an ordered float: f64 {f64_result:?}, f32 {f32_result:?}"
	);
}
