// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt::Debug,
	panic::{AssertUnwindSafe, catch_unwind},
};

use reifydb_value::value::duration::Duration;

trait Negated {
	fn outcome(self) -> Result<Duration, String>;
}

impl Negated for Duration {
	fn outcome(self) -> Result<Duration, String> {
		Ok(self)
	}
}

impl<E: Debug> Negated for Result<Duration, E> {
	fn outcome(self) -> Result<Duration, String> {
		// Without this impl a fallible negate would not compile, so the test must accept both signatures.
		self.map_err(|e| format!("{e:?}"))
	}
}

fn negate(duration: Duration) -> String {
	match catch_unwind(AssertUnwindSafe(|| duration.negate().outcome())) {
		Err(_) => "panic".to_string(),
		Ok(Err(_)) => "error".to_string(),
		Ok(Ok(negated)) => format!("ok {negated:?}"),
	}
}

#[test]
fn negating_the_minimum_months_or_days_is_an_error_never_a_panic() {
	// The negative of i32::MIN does not fit i32, so negate must report it instead of panicking or wrapping.
	let cases =
		[("months", Duration::new(i32::MIN, 0, 0).unwrap()), ("days", Duration::new(0, i32::MIN, 0).unwrap())];

	let observed: Vec<_> = cases.into_iter().map(|(part, duration)| (part, negate(duration))).collect();

	assert_eq!(observed, [("months", "error".to_string()), ("days", "error".to_string())]);
}
