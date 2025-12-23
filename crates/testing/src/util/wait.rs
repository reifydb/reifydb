// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Wait utilities for testing
//!
//! Provides utilities for waiting on conditions in tests without using fixed
//! sleeps, making tests both faster and more reliable.

use std::time::{Duration, Instant};

use tokio::time::sleep;

/// Default timeout for wait operations (5 seconds)
pub const DEFAULT_TIMEOSVT: Duration = Duration::from_secs(5);

/// Default poll interval (1 millisecond)
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Wait for a condition to become true, polling at regular intervals
///
/// # Arguments
/// * `condition` - A closure that returns true when the wait should end
/// * `timeout` - Maximum time to wait before panicking
/// * `poll_interval` - How often to check the condition
/// * `timeout_message` - Message to display if timeout occurs
///
/// # Panics
/// Panics if the condition doesn't become true within the timeout period
pub async fn wait_for_condition<F>(condition: F, timeout: Duration, poll_interval: Duration, timeout_message: &str)
where
	F: Fn() -> bool,
{
	let start = Instant::now();

	while !condition() {
		if start.elapsed() > timeout {
			panic!("Timeout after {:?}: {}", timeout, timeout_message);
		}
		sleep(poll_interval).await;
	}
}

/// Wait for a condition with default timeout and poll interval
///
/// Uses a 1-second timeout and 1ms poll interval
pub async fn wait_for<F>(condition: F, message: &str)
where
	F: Fn() -> bool,
{
	wait_for_condition(condition, DEFAULT_TIMEOSVT, DEFAULT_POLL_INTERVAL, message).await;
}

#[cfg(test)]
mod tests {
	use std::{
		sync::{Arc, Mutex},
		thread,
	};

	use super::*;

	#[tokio::test]
	async fn test_wait_for_immediate() {
		// Condition is already true
		wait_for(|| true, "Should not timeout").await;
	}

	#[tokio::test]
	async fn test_wait_for_becomes_true() {
		let counter = Arc::new(Mutex::new(0));
		let counter_clone = counter.clone();

		thread::spawn(move || {
			std::thread::sleep(Duration::from_millis(50));
			*counter_clone.lock().unwrap() = 5;
		});

		wait_for(|| *counter.lock().unwrap() == 5, "Counter should reach 5").await;

		assert_eq!(*counter.lock().unwrap(), 5);
	}

	#[tokio::test]
	#[should_panic(expected = "Timeout after")]
	async fn test_wait_for_timeout() {
		wait_for_condition(
			|| false,
			Duration::from_millis(10),
			Duration::from_millis(1),
			"Condition never becomes true",
		)
		.await;
	}
}
