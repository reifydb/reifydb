// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub fn retry<R, E>(retries: usize, f: impl Fn() -> Result<R, E>) -> Result<R, E> {
	let mut retries_left = retries;
	loop {
		match f() {
			Ok(r) => return Ok(r),
			Err(err) => {
				if retries_left > 0 {
					retries_left -= 1;
				} else {
					return Err(err);
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::cell::Cell;

	use crate::util::retry;

	#[test]
	fn test_ok() {
		let result = retry::<i32, ()>(10, || return Ok(23));
		assert_eq!(result, Ok(23));
	}

	#[test]
	fn test_success_after_some_retries() {
		let counter = Cell::new(0);
		let result = retry::<i32, &'static str>(5, || {
			if counter.get() < 3 {
				counter.set(counter.get() + 1);
				Err("fail")
			} else {
				Ok(42)
			}
		});
		assert_eq!(result, Ok(42));
		assert_eq!(counter.get(), 3);
	}

	#[test]
	fn test_failure_after_retries_exhausted() {
		let counter = Cell::new(0);
		let result = retry::<i32, &'static str>(3, || {
			counter.set(counter.get() + 1);
			Err("still failing")
		});
		assert_eq!(result, Err("still failing"));
		assert_eq!(counter.get(), 4); // initial + 3 retries
	}

	#[test]
	fn test_zero_retries_allowed() {
		let counter = Cell::new(0);
		let result = retry::<i32, &'static str>(0, || {
			counter.set(counter.get() + 1);
			Err("fail fast")
		});
		assert_eq!(result, Err("fail fast"));
		assert_eq!(counter.get(), 1); // only one try
	}

	#[test]
	fn test_retry_closure_panics() {
		let result = std::panic::catch_unwind(|| {
			let _ = retry::<(), ()>(2, || panic!("boom"));
		});
		assert!(result.is_err());
	}
}
