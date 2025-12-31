// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	fmt,
	panic::{AssertUnwindSafe, catch_unwind},
};

/// Error type that can represent both regular errors and panics
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryError<E> {
	/// The original error from the function
	Error(E),
	/// A panic occurred during execution
	Panic(String),
}

impl<E: fmt::Display> fmt::Display for RetryError<E> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			RetryError::Error(e) => write!(f, "{}", e),
			RetryError::Panic(msg) => write!(f, "panic: {}", msg),
		}
	}
}

impl<E: fmt::Display + fmt::Debug> std::error::Error for RetryError<E> {}

impl<E> From<E> for RetryError<E> {
	fn from(err: E) -> Self {
		RetryError::Error(err)
	}
}

pub fn retry<R, E>(retries: usize, f: impl Fn() -> Result<R, E>) -> Result<R, RetryError<E>> {
	let mut retries_left = retries;
	loop {
		match catch_unwind(AssertUnwindSafe(&f)) {
			Ok(Ok(r)) => return Ok(r),
			Ok(Err(err)) => {
				if retries_left > 0 {
					retries_left -= 1;
				} else {
					return Err(RetryError::Error(err));
				}
			}
			Err(panic) => {
				let msg = if let Some(s) = panic.downcast_ref::<String>() {
					s.clone()
				} else if let Some(s) = panic.downcast_ref::<&str>() {
					s.to_string()
				} else {
					"Unknown panic".to_string()
				};

				if retries_left > 0 {
					retries_left -= 1;
				} else {
					return Err(RetryError::Panic(msg));
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::cell::Cell;

	use crate::util::{RetryError, retry};

	#[test]
	fn test_ok() {
		let result = retry::<i32, ()>(10, || Ok(23));
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
		assert_eq!(result, Err(RetryError::Error("still failing")));
		assert_eq!(counter.get(), 4); // initial + 3 retries
	}

	#[test]
	fn test_zero_retries_allowed() {
		let counter = Cell::new(0);
		let result = retry::<i32, &'static str>(0, || {
			counter.set(counter.get() + 1);
			Err("fail fast")
		});
		assert_eq!(result, Err(RetryError::Error("fail fast")));
		assert_eq!(counter.get(), 1); // only one try
	}

	#[test]
	fn test_retry_catches_panic() {
		let counter = Cell::new(0);
		let result = retry::<(), &'static str>(2, || {
			counter.set(counter.get() + 1);
			panic!("boom");
		});
		assert_eq!(result, Err(RetryError::Panic("boom".to_string())));
		assert_eq!(counter.get(), 3); // initial + 2 retries
	}

	#[test]
	fn test_retry_panic_with_string() {
		let result = retry::<(), &'static str>(1, || {
			panic!("{}", String::from("custom panic message"));
		});
		assert_eq!(result, Err(RetryError::Panic("custom panic message".to_string())));
	}

	#[test]
	fn test_retry_panic_then_success() {
		let counter = Cell::new(0);
		let result = retry::<i32, &'static str>(3, || {
			let count = counter.get();
			counter.set(count + 1);
			if count < 2 {
				panic!("panic #{}", count);
			} else {
				Ok(42)
			}
		});
		assert_eq!(result, Ok(42));
		assert_eq!(counter.get(), 3);
	}

	#[test]
	fn test_retry_mixed_errors_and_panics() {
		let counter = Cell::new(0);
		let result = retry::<i32, &'static str>(5, || {
			let count = counter.get();
			counter.set(count + 1);
			match count {
				0 => Err("error 1"),
				1 => panic!("panic 1"),
				2 => Err("error 2"),
				3 => panic!("panic 2"),
				_ => Ok(100),
			}
		});
		assert_eq!(result, Ok(100));
		assert_eq!(counter.get(), 5);
	}

	#[test]
	fn test_retry_panic_no_retries() {
		let result = retry::<(), &'static str>(0, || {
			panic!("immediate panic");
		});
		assert_eq!(result, Err(RetryError::Panic("immediate panic".to_string())));
	}

	#[test]
	fn test_retry_error_display() {
		let err: RetryError<&str> = RetryError::Error("test error");
		assert_eq!(format!("{}", err), "test error");

		let panic: RetryError<&str> = RetryError::Panic("test panic".to_string());
		assert_eq!(format!("{}", panic), "panic: test panic");
	}
}
