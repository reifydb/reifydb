// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use clap::{Parser, ValueEnum};

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

	/// Server port (default: 8091 for http, 8090 for ws)
	#[arg(short = 'p', long, env = "REIFYDB_PORT")]
	pub port: Option<u16>,

	/// Authentication token
	#[arg(short = 't', long, env = "REIFYDB_TOKEN")]
	pub token: Option<String>,

	/// Number of parallel connections/workers
	#[arg(short = 'c', long, default_value = "50")]
	pub connections: usize,

	/// Total number of requests (ignored if --duration is set)
	#[arg(short = 'n', long, default_value = "100000")]
	pub requests: u64,

	/// Workload preset to run
	#[arg(short = 'w', long, value_enum, default_value = "mixed")]
	pub workload: WorkloadPreset,

	/// Warmup requests before measuring (set to 0 to disable)
	#[arg(long, default_value = "1000")]
	pub warmup: u64,

	/// Run for specified duration instead of request count (e.g., "30s", "5m")
	#[arg(long, value_parser = parse_duration)]
	pub duration: Option<Duration>,

	/// Quiet mode - only show final summary
	#[arg(short = 'q', long)]
	pub quiet: bool,

	/// Seed for random number generation (for reproducible runs)
	#[arg(long)]
	pub seed: Option<u64>,

	/// Table size for workloads that need pre-populated data
	#[arg(long, default_value = "10000")]
	pub table_size: u64,
}

impl Config {
	/// Get the effective port based on protocol defaults
	pub fn effective_port(&self) -> u16 {
		self.port.unwrap_or(match self.protocol {
			Protocol::Http => 8091,
			Protocol::Ws => 8090,
		})
	}

	/// Get the connection URL
	pub fn url(&self) -> String {
		let scheme = match self.protocol {
			Protocol::Http => "http",
			Protocol::Ws => "ws",
		};
		format!("{}://{}:{}", scheme, self.host, self.effective_port())
	}
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum Protocol {
	/// HTTP protocol
	Http,
	/// WebSocket protocol
	Ws,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum WorkloadPreset {
	/// Baseline latency test - simple query returning 1 row
	Ping,
	/// Point lookups by primary key
	Read,
	/// Insert operations
	Write,
	/// Mixed workload: 80% reads, 20% writes
	Mixed,
	/// Table scans with filters
	Scan,
	/// Join queries across two tables
	Join,
}

fn parse_duration(s: &str) -> Result<Duration, String> {
	let s = s.trim();
	if s.is_empty() {
		return Err("duration cannot be empty".to_string());
	}

	// Find where the number ends and unit begins
	let (num_str, unit) = if s.ends_with("ms") {
		(&s[..s.len() - 2], "ms")
	} else if s.ends_with('s') {
		(&s[..s.len() - 1], "s")
	} else if s.ends_with('m') {
		(&s[..s.len() - 1], "m")
	} else if s.ends_with('h') {
		(&s[..s.len() - 1], "h")
	} else {
		// Default to seconds if no unit
		(s, "s")
	};

	let num: u64 = num_str.parse().map_err(|_| format!("invalid duration number: {}", num_str))?;

	let duration = match unit {
		"ms" => Duration::from_millis(num),
		"s" => Duration::from_secs(num),
		"m" => Duration::from_secs(num * 60),
		"h" => Duration::from_secs(num * 3600),
		_ => return Err(format!("unknown duration unit: {}", unit)),
	};

	Ok(duration)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_parse_duration() {
		assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
		assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
		assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
		assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
		assert_eq!(parse_duration("60").unwrap(), Duration::from_secs(60));
	}
}
