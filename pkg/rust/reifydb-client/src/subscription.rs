// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::duration::Duration;

#[derive(Debug, Clone, Copy)]
pub struct Throttle(Duration);

impl Throttle {
	pub fn new(duration: Duration) -> Self {
		if duration.is_negative() {
			panic!("throttle must not be negative");
		}
		Self(duration)
	}

	pub fn duration(&self) -> Duration {
		self.0
	}
}

#[derive(Debug, Clone, Copy)]
pub struct Linger(Duration);

impl Linger {
	pub fn new(duration: Duration) -> Self {
		if duration.is_negative() {
			panic!("linger must not be negative");
		}
		Self(duration)
	}

	pub fn duration(&self) -> Duration {
		self.0
	}
}

#[derive(Debug, Clone)]
pub struct HydrationConfig {
	pub enabled: bool,
	pub max_rows: Option<u64>,
}

impl Default for HydrationConfig {
	fn default() -> Self {
		Self {
			enabled: true,
			max_rows: None,
		}
	}
}

#[derive(Debug, Clone, Default)]
pub struct SubscriptionConfig {
	pub hydration: HydrationConfig,
	pub throttle: Option<Throttle>,
	pub linger: Option<Linger>,
}

#[derive(Debug, Clone)]
pub struct BatchSubscribeItem<'a> {
	pub rql: &'a str,
	pub config: SubscriptionConfig,
}

impl<'a> BatchSubscribeItem<'a> {
	pub fn new(rql: &'a str, config: SubscriptionConfig) -> Self {
		Self {
			rql,
			config,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	#[should_panic(expected = "throttle must not be negative")]
	fn negative_throttle_panics_naming_the_knob() {
		// A negative throttle must be rejected at construction, never carried into the subscribe options.
		Throttle::new(Duration::from_seconds(-5).unwrap());
	}

	#[test]
	#[should_panic(expected = "linger must not be negative")]
	fn negative_linger_panics_naming_the_knob() {
		// A negative linger must be rejected at construction, never carried into the subscribe options.
		Linger::new(Duration::from_seconds(-5).unwrap());
	}
}
