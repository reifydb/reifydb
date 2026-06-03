// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::time::Instant;

use reifydb_value::value::duration::Duration;
use tokio::time::sleep;

pub fn default_timeout() -> Duration {
	Duration::from_seconds(5).unwrap()
}

pub fn default_poll_interval() -> Duration {
	Duration::from_milliseconds(1).unwrap()
}

pub async fn wait_for_condition<F>(condition: F, timeout: Duration, poll_interval: Duration, timeout_message: &str)
where
	F: Fn() -> bool,
{
	#[allow(clippy::disallowed_methods)]
	let start = Instant::now();
	let mut poll_count = 0u64;

	while !condition() {
		if start.elapsed() > timeout.to_std() {
			println!(
				"[DEBUG:await] TIMEOUT elapsed={:.1}s polls={poll_count} msg={timeout_message}",
				start.elapsed().as_secs_f64()
			);
			panic!("Timeout after {:?}: {}", timeout, timeout_message);
		}
		poll_count += 1;
		if poll_count.is_multiple_of(1000) {
			println!(
				"[DEBUG:await] poll #{poll_count} elapsed={:.1}s msg={timeout_message}",
				start.elapsed().as_secs_f64()
			);
		}
		sleep(poll_interval.to_std()).await;
	}
	println!(
		"[DEBUG:await] condition met after {poll_count} polls elapsed={:.3}s msg={timeout_message}",
		start.elapsed().as_secs_f64()
	);
}

pub async fn wait_for<F>(condition: F, message: &str)
where
	F: Fn() -> bool,
{
	wait_for_condition(condition, default_timeout(), default_poll_interval(), message).await;
}

#[cfg(test)]
pub mod tests {
	use std::{sync::Arc, thread};

	use reifydb_runtime::sync::mutex::Mutex;

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
			thread::sleep(Duration::from_milliseconds(50).unwrap().to_std());
			*counter_clone.lock() = 5;
		});

		wait_for(|| *counter.lock() == 5, "Counter should reach 5").await;

		assert_eq!(*counter.lock(), 5);
	}

	#[tokio::test]
	#[should_panic(expected = "Timeout after")]
	async fn test_wait_for_timeout() {
		wait_for_condition(
			|| false,
			Duration::from_milliseconds(10).unwrap(),
			Duration::from_milliseconds(1).unwrap(),
			"Condition never becomes true",
		)
		.await;
	}
}
