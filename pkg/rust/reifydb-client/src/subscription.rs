// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{self, Display, Formatter};

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

impl Display for Throttle {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
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

impl Display for Linger {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
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
pub struct BatchItem<'a> {
	pub rql: &'a str,
	pub config: SubscriptionConfig,
}

impl<'a> BatchItem<'a> {
	pub fn new(rql: &'a str, config: SubscriptionConfig) -> Self {
		Self {
			rql,
			config,
		}
	}
}

pub fn build_subscription_rql(body: &str, config: &SubscriptionConfig) -> String {
	let h = &config.hydration;
	let mut opts = match h.max_rows {
		Some(n) => format!("hydration: {{ enabled: {}, max_rows: {} }}", h.enabled, n),
		None => format!("hydration: {{ enabled: {} }}", h.enabled),
	};
	if let Some(throttle) = config.throttle {
		opts.push_str(&format!(", throttle: {}", throttle));
	}
	if let Some(linger) = config.linger {
		opts.push_str(&format!(", linger: {}", linger));
	}
	let with_clause = format!(" WITH {{ {} }}", opts);
	let mut out = String::with_capacity(body.len() + with_clause.len() + 32);
	out.push_str("CREATE SUBSCRIPTION");
	out.push_str(&with_clause);
	out.push_str(" AS { ");
	out.push_str(body);
	out.push_str(" }");
	out
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default_builds_with_hydration_enabled_no_cap() {
		let s = build_subscription_rql("from a::b", &SubscriptionConfig::default());
		assert_eq!(s, "CREATE SUBSCRIPTION WITH { hydration: { enabled: true } } AS { from a::b }");
	}

	#[test]
	fn explicit_max_rows() {
		let cfg = SubscriptionConfig {
			hydration: HydrationConfig {
				enabled: true,
				max_rows: Some(500),
			},
			throttle: None,
			linger: None,
		};
		let s = build_subscription_rql("from a::b", &cfg);
		assert_eq!(
			s,
			"CREATE SUBSCRIPTION WITH { hydration: { enabled: true, max_rows: 500 } } AS { from a::b }"
		);
	}

	#[test]
	fn hydration_disabled() {
		let cfg = SubscriptionConfig {
			hydration: HydrationConfig {
				enabled: false,
				max_rows: None,
			},
			throttle: None,
			linger: None,
		};
		let s = build_subscription_rql("from a::b | take 10", &cfg);
		assert_eq!(s, "CREATE SUBSCRIPTION WITH { hydration: { enabled: false } } AS { from a::b | take 10 }");
	}

	#[test]
	fn throttle_renders_as_a_bare_duration_literal() {
		// The knob must reach RQL unquoted, otherwise it never parses as a duration literal.
		let cfg = SubscriptionConfig {
			hydration: HydrationConfig {
				enabled: true,
				max_rows: None,
			},
			throttle: Some(Throttle::new(Duration::from_milliseconds(500).unwrap())),
			linger: None,
		};
		let s = build_subscription_rql("from a::b", &cfg);
		assert_eq!(
			s,
			"CREATE SUBSCRIPTION WITH { hydration: { enabled: true }, throttle: 500ms } AS { from a::b }"
		);
	}

	#[test]
	fn zero_is_accepted_and_renders_as_zero_seconds() {
		// Zero must stay constructible; a zero linger is due immediately and is load-bearing.
		let cfg = SubscriptionConfig {
			hydration: HydrationConfig {
				enabled: true,
				max_rows: None,
			},
			throttle: Some(Throttle::new(Duration::zero())),
			linger: Some(Linger::new(Duration::zero())),
		};
		let s = build_subscription_rql("from a::b", &cfg);
		assert_eq!(
			s,
			"CREATE SUBSCRIPTION WITH { hydration: { enabled: true }, throttle: 0s, linger: 0s } AS { from a::b }"
		);
	}

	#[test]
	#[should_panic(expected = "throttle must not be negative")]
	fn negative_throttle_panics_naming_the_knob() {
		// Without the guard a negative renders as "-5s", which is not a legal bare literal.
		Throttle::new(Duration::from_seconds(-5).unwrap());
	}

	#[test]
	#[should_panic(expected = "linger must not be negative")]
	fn negative_linger_panics_naming_the_knob() {
		// Without the guard a negative renders as "-5s", which is not a legal bare literal.
		Linger::new(Duration::from_seconds(-5).unwrap());
	}

	#[test]
	fn linger_is_woven_into_the_with_clause() {
		let cfg = SubscriptionConfig {
			hydration: HydrationConfig {
				enabled: true,
				max_rows: None,
			},
			throttle: None,
			linger: Some(Linger::new(Duration::from_milliseconds(250).unwrap())),
		};
		let s = build_subscription_rql("from a::b", &cfg);
		assert_eq!(
			s,
			"CREATE SUBSCRIPTION WITH { hydration: { enabled: true }, linger: 250ms } AS { from a::b }"
		);
	}
}
