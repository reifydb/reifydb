// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::duration::Duration;

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
	pub throttle: Option<Duration>,
	pub linger: Option<Duration>,
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
		opts.push_str(&format!(", throttle: \"{}ms\"", throttle.to_std().as_millis()));
	}
	if let Some(linger) = config.linger {
		opts.push_str(&format!(", linger: \"{}ms\"", linger.to_std().as_millis()));
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
	fn linger_is_woven_into_the_with_clause() {
		let cfg = SubscriptionConfig {
			hydration: HydrationConfig {
				enabled: true,
				max_rows: None,
			},
			throttle: None,
			linger: Some(Duration::from_milliseconds(250).unwrap()),
		};
		let s = build_subscription_rql("from a::b", &cfg);
		assert_eq!(
			s,
			"CREATE SUBSCRIPTION WITH { hydration: { enabled: true }, linger: \"250ms\" } AS { from a::b }"
		);
	}
}
