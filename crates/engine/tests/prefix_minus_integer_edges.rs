// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::{AssertUnwindSafe, catch_unwind};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

fn outcome(t: &TestEngine, rql: &str) -> String {
	match catch_unwind(AssertUnwindSafe(|| t.inner().query_as(TestEngine::identity(), rql, Params::None))) {
		Err(panic) => format!(
			"panic {}",
			panic.downcast_ref::<String>()
				.cloned()
				.or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
				.unwrap_or_default()
		),
		Ok(result) => match result.error {
			Some(err) => format!("error {}", err.diagnostic().code),
			None => {
				let column = result.frames[0]
					.columns
					.iter()
					.find(|c| c.name == "v")
					.unwrap_or_else(|| panic!("no column v in {rql}"));
				let values: Vec<String> =
					(0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect();
				format!("ok {values:?}")
			}
		},
	}
}

fn negate(width: &str, value: &str) -> String {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(&format!("CREATE TABLE test::t {{ x: {width} }}"));
	t.command(&format!("INSERT test::t [{{ x: cast('{value}', {width}) }}]"));
	outcome(&t, "FROM test::t | map { v: -x }")
}

#[test]
fn negating_an_unsigned_value_past_the_signed_range_is_an_error_or_exact_never_wrapped() {
	// Casting to the signed twin with as wraps, so -(u128::MAX) came out as 1 instead of failing.
	let cases = [
		("uint16", "340282366920938463463374607431768211455", "-340282366920938463463374607431768211455"),
		("uint8", "18446744073709551615", "-18446744073709551615"),
		("uint4", "4294967295", "-4294967295"),
		("uint2", "65535", "-65535"),
		("uint1", "255", "-255"),
	];

	let mut wrong = Vec::new();
	for (width, value, negative) in cases {
		let result = negate(width, value);
		if !result.starts_with("error ") && result != format!("ok [\"{negative}\"]") {
			wrong.push((width, value, result));
		}
	}

	assert!(wrong.is_empty(), "-x must be the exact negative or a range error, got {wrong:#?}");
}

#[test]
fn negating_two_to_the_127_as_uint16_gives_i128_min() {
	// -(2^127) is exactly i128::MIN, so it must fit Int16 instead of overflowing on the negation of a cast.
	let result = negate("uint16", "170141183460469231731687303715884105728");

	assert_eq!(result, r#"ok ["-170141183460469231731687303715884105728"]"#);
}

#[test]
fn negating_the_minimum_signed_value_is_an_error_or_exact_never_a_panic() {
	// The negative of MIN does not fit its width, so -x must report that instead of panicking or wrapping.
	let cases = [
		("int1", "-128", "128"),
		("int2", "-32768", "32768"),
		("int4", "-2147483648", "2147483648"),
		("int8", "-9223372036854775808", "9223372036854775808"),
		("int16", "-170141183460469231731687303715884105728", "170141183460469231731687303715884105728"),
	];

	let mut wrong = Vec::new();
	for (width, min, positive) in cases {
		let result = negate(width, min);
		if !result.starts_with("error ") && result != format!("ok [\"{positive}\"]") {
			wrong.push((width, min, result));
		}
	}

	assert!(wrong.is_empty(), "-MIN must be a range error or the exact positive, got {wrong:#?}");
}
