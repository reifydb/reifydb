// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

fn nan() -> &'static str {
	"math::sqrt(-1.0)"
}

#[test]
fn arithmetic_on_a_nan_propagates_it_instead_of_reporting_an_overflow() {
	// NaN is a representable float8, so inheriting one must never be reported as exceeding the type's range.
	let t = TestEngine::new();
	let mut wrong = Vec::new();

	for op in ["*", "+", "-"] {
		let rql = format!("map {{ v: {} {op} cast(2.0, float8) }}", nan());
		let err = std::panic::catch_unwind(|| TestEngine::new().query(&rql));
		match err {
			Ok(frames) => {
				let rendered = format!("{}", frames[0]);
				if !rendered.contains("NaN") {
					wrong.push((rql, rendered));
				}
			}
			Err(_) => wrong.push((rql, "rejected".to_string())),
		}
	}

	assert!(wrong.is_empty(), "NaN must survive arithmetic, got {wrong:#?}");
	drop(t);
}

#[test]
fn a_finite_overflow_is_still_out_of_range() {
	// Letting NaN through must not also let a real overflow through; finite operands reaching infinity is an error.
	let t = TestEngine::new();

	let err = t.query_err("map { v: cast('1e308', float8) * cast(10.0, float8) }");

	assert!(err.contains("NUMBER_002"), "a finite overflow must still be rejected, got: {err}");
}
