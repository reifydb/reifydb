// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use clap::{Parser, ValueEnum};
use reifydb_testing_scenario::{
	profile::{Scale, StopCondition},
	scenario::Scenario,
};
use reifydb_value::value::duration::Duration;

pub const DEFAULT_CONNECTIONS: usize = 50;
pub const DEFAULT_REQUESTS: u64 = 100_000;
pub const DEFAULT_SCALE: u64 = 10_000;

#[derive(Parser)]
#[command(name = "reifydb-load-test")]
#[command(about = "ReifyDB load testing tool - similar to redis-benchmark", long_about = None)]
#[command(version)]
pub struct Config {
	/// Protocol to use for connections
	#[arg(value_enum)]
	pub protocol: Protocol,

	/// Server host
	#[arg(short = 'H', long, env = "REIFYDB_HOST", default_value = "127.0.0.1")]
	pub host: String,

	/// Server port (default: 8090 for http, 8091 for ws)
	#[arg(short = 'p', long, env = "REIFYDB_PORT")]
	pub port: Option<u16>,

	/// Admin port used for scenario setup and teardown (default: 9090 for http, 9091 for ws)
	#[arg(long, env = "REIFYDB_ADMIN_PORT")]
	pub admin_port: Option<u16>,

	/// Authentication token
	#[arg(short = 't', long, env = "REIFYDB_TOKEN")]
	pub token: Option<String>,

	/// Scenario to run (see --list)
	#[arg(short = 's', long, default_value = "read")]
	pub scenario: String,

	/// Named profile supplying thread count, stop condition and dataset scale
	#[arg(short = 'P', long)]
	pub profile: Option<String>,

	/// Named query within the scenario (required only when the scenario defines more than one)
	#[arg(short = 'Q', long)]
	pub query: Option<String>,

	/// List the registered scenarios with their queries and profiles, then exit
	#[arg(long)]
	pub list: bool,

	/// Number of parallel connections/workers, overriding the profile
	#[arg(short = 'c', long)]
	pub connections: Option<usize>,

	/// Total number of requests, overriding the profile
	#[arg(short = 'n', long)]
	pub requests: Option<u64>,

	/// Run for a duration instead of a request count (e.g. "30s", "5m"), overriding the profile
	#[arg(long, value_parser = parse_duration)]
	pub duration: Option<Duration>,

	/// Rows to seed for generated datasets, overriding the profile
	#[arg(long)]
	pub table_size: Option<u64>,

	/// Warmup requests before measuring (set to 0 to disable)
	#[arg(long, default_value = "1000")]
	pub warmup: u64,

	/// Quiet mode - only show final summary
	#[arg(short = 'q', long)]
	pub quiet: bool,

	/// Seed for random number generation (for reproducible runs)
	#[arg(long)]
	pub seed: Option<u64>,
}

#[derive(Debug)]
pub struct Resolved {
	pub connections: usize,
	pub stop: StopCondition,
	pub scale: u64,
	pub profile: Option<String>,
}

impl Config {
	pub fn effective_port(&self) -> u16 {
		self.port.unwrap_or(match self.protocol {
			Protocol::Http => 8090,
			Protocol::Ws => 8091,
		})
	}

	pub fn effective_admin_port(&self) -> u16 {
		self.admin_port.unwrap_or(match self.protocol {
			Protocol::Http => 9090,
			Protocol::Ws => 9091,
		})
	}

	fn scheme(&self) -> &'static str {
		match self.protocol {
			Protocol::Http => "http",
			Protocol::Ws => "ws",
		}
	}

	pub fn url(&self) -> String {
		format!("{}://{}:{}", self.scheme(), self.host, self.effective_port())
	}

	pub fn admin_url(&self) -> String {
		format!("{}://{}:{}", self.scheme(), self.host, self.effective_admin_port())
	}

	pub fn resolve(&self, scenario: &Scenario) -> Result<Resolved, String> {
		let profile = match &self.profile {
			Some(name) => Some(scenario.profile(name).ok_or_else(|| {
				format!(
					"scenario '{}' has no profile '{}'; available: {}",
					scenario.name,
					name,
					scenario.profiles
						.iter()
						.map(|p| p.name.as_str())
						.collect::<Vec<_>>()
						.join(", ")
				)
			})?),
			None => None,
		};

		let connections =
			self.connections.or_else(|| profile.map(|p| p.threads)).unwrap_or(DEFAULT_CONNECTIONS);

		if connections == 0 {
			return Err("connections must be greater than zero".to_string());
		}

		let stop = if let Some(duration) = self.duration {
			StopCondition::Duration(duration)
		} else if let Some(requests) = self.requests {
			StopCondition::Iterations(requests)
		} else if let Some(profile) = profile {
			profile.stop
		} else {
			StopCondition::Iterations(DEFAULT_REQUESTS)
		};

		let scale = if scenario.dataset.is_manual() {
			0
		} else {
			self.table_size
				.or_else(|| match profile.map(|p| p.scale) {
					Some(Scale::Rows(rows)) => Some(rows),
					_ => None,
				})
				.unwrap_or(DEFAULT_SCALE)
		};

		Ok(Resolved {
			connections,
			stop,
			scale,
			profile: profile.map(|p| p.name.clone()),
		})
	}
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum Protocol {
	/// HTTP protocol
	Http,
	/// WebSocket protocol
	Ws,
}

fn parse_duration(s: &str) -> Result<Duration, String> {
	let s = s.trim();
	if s.is_empty() {
		return Err("duration cannot be empty".to_string());
	}

	let (num_str, unit) = if let Some(n) = s.strip_suffix("ms") {
		(n, "ms")
	} else if let Some(n) = s.strip_suffix('s') {
		(n, "s")
	} else if let Some(n) = s.strip_suffix('m') {
		(n, "m")
	} else if let Some(n) = s.strip_suffix('h') {
		(n, "h")
	} else {
		(s, "s")
	};

	let num: u64 = num_str.parse().map_err(|_| format!("invalid duration number: {}", num_str))?;

	let duration = match unit {
		"ms" => Duration::from_milliseconds(num as i64).unwrap(),
		"s" => Duration::from_seconds(num as i64).unwrap(),
		"m" => Duration::from_seconds((num * 60) as i64).unwrap(),
		"h" => Duration::from_seconds((num * 3600) as i64).unwrap(),
		_ => return Err(format!("unknown duration unit: {}", unit)),
	};

	Ok(duration)
}

#[cfg(test)]
mod tests {
	use reifydb_testing_scenario::registry::by_name;

	use super::*;

	fn config(scenario: &str) -> Config {
		Config {
			protocol: Protocol::Http,
			host: "127.0.0.1".to_string(),
			port: None,
			admin_port: None,
			token: None,
			scenario: scenario.to_string(),
			profile: None,
			query: None,
			list: false,
			connections: None,
			requests: None,
			duration: None,
			table_size: None,
			warmup: 0,
			quiet: true,
			seed: None,
		}
	}

	#[test]
	fn test_parse_duration() {
		assert_eq!(parse_duration("30s").unwrap(), Duration::from_seconds(30).unwrap());
		assert_eq!(parse_duration("5m").unwrap(), Duration::from_seconds(300).unwrap());
		assert_eq!(parse_duration("1h").unwrap(), Duration::from_seconds(3600).unwrap());
		assert_eq!(parse_duration("500ms").unwrap(), Duration::from_milliseconds(500).unwrap());
		assert_eq!(parse_duration("60").unwrap(), Duration::from_seconds(60).unwrap());
	}

	#[test]
	fn a_profile_supplies_threads_stop_and_scale() {
		// The whole point of naming a profile is that one flag pins all three axes; if any of
		// them silently fell back to a default the reported scaling curve would be a fiction.
		let mut config = config("read");
		config.profile = Some("t8_100k".to_string());

		let scenario = by_name("read").unwrap();
		let resolved = config.resolve(&scenario).unwrap();

		assert_eq!(resolved.connections, 8);
		assert_eq!(resolved.scale, 100_000);
		assert_eq!(resolved.stop, StopCondition::Iterations(100_000));
	}

	#[test]
	fn explicit_flags_override_the_profile() {
		let mut config = config("read");
		config.profile = Some("t8_100k".to_string());
		config.connections = Some(2);
		config.table_size = Some(500);
		config.requests = Some(7);

		let scenario = by_name("read").unwrap();
		let resolved = config.resolve(&scenario).unwrap();

		assert_eq!(resolved.connections, 2);
		assert_eq!(resolved.scale, 500);
		assert_eq!(resolved.stop, StopCondition::Iterations(7));
	}

	#[test]
	fn duration_takes_precedence_over_a_request_count() {
		let mut config = config("read");
		config.requests = Some(10);
		config.duration = Some(Duration::from_seconds(30).unwrap());

		let scenario = by_name("read").unwrap();
		let resolved = config.resolve(&scenario).unwrap();

		assert_eq!(resolved.stop, StopCondition::Duration(Duration::from_seconds(30).unwrap()));
	}

	#[test]
	fn omitting_the_profile_keeps_the_historical_defaults() {
		// Existing invocations that never passed --profile must behave exactly as before.
		let scenario = by_name("read").unwrap();
		let resolved = config("read").resolve(&scenario).unwrap();

		assert_eq!(resolved.connections, DEFAULT_CONNECTIONS);
		assert_eq!(resolved.scale, DEFAULT_SCALE);
		assert_eq!(resolved.stop, StopCondition::Iterations(DEFAULT_REQUESTS));
	}

	#[test]
	fn a_manual_dataset_reports_no_scale_to_seed() {
		// `write` seeds nothing; asking it to generate rows would invent a table the scenario
		// never described.
		let mut config = config("write");
		config.table_size = Some(999);

		let scenario = by_name("write").unwrap();
		assert_eq!(config.resolve(&scenario).unwrap().scale, 0);
	}

	#[test]
	fn an_unknown_profile_is_rejected_with_the_available_names() {
		let mut config = config("read");
		config.profile = Some("t99_9m".to_string());

		let scenario = by_name("read").unwrap();
		let error = config.resolve(&scenario).unwrap_err();

		assert!(error.contains("has no profile 't99_9m'"), "{}", error);
		assert!(error.contains("t1_10k"), "{}", error);
	}

	#[test]
	fn zero_connections_is_rejected_rather_than_dividing_by_zero() {
		let mut config = config("read");
		config.connections = Some(0);

		let scenario = by_name("read").unwrap();
		assert!(config.resolve(&scenario).is_err());
	}
}
