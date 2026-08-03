// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_testing_scenario::{profile::StopCondition, scenario::Scenario};

use crate::{
	config::{Config, Protocol, Resolved},
	metrics::MetricsSummary,
};

pub fn print_header(config: &Config, resolved: &Resolved, description: &str) {
	println!();
	println!("====== {} ======", description);
	println!("Host: {}:{}", config.host, config.effective_port());
	println!(
		"Protocol: {}",
		match config.protocol {
			Protocol::Http => "HTTP",
			Protocol::Ws => "WebSocket",
		}
	);

	if let Some(profile) = &resolved.profile {
		println!("Profile: {}", profile);
	}

	println!("Connections: {}", resolved.connections);

	if resolved.scale > 0 {
		println!("Rows: {}", format_number(resolved.scale));
	}

	match resolved.stop {
		StopCondition::Duration(duration) => println!("Duration: {:?}", duration),
		StopCondition::Iterations(requests) => println!("Requests: {}", format_number(requests)),
	}
	println!();
}

pub fn print_scenarios(scenarios: &[Scenario]) {
	for scenario in scenarios {
		println!();
		println!("{} - {}", scenario.name, scenario.description);
		println!("  queries:  {}", scenario.queries.iter().map(|q| q.name).collect::<Vec<_>>().join(", "));
		println!(
			"  profiles: {}",
			scenario.profiles.iter().map(|p| p.name.as_str()).collect::<Vec<_>>().join(", ")
		);
	}
	println!();
}

/// Laid out to match redis-benchmark output.
pub fn print_summary(summary: &MetricsSummary, description: &str) {
	println!();
	println!("====== {} ======", description);
	println!(
		"{} requests completed in {:.2} seconds",
		format_number(summary.total_requests),
		summary.duration_secs
	);
	println!();
	println!("Throughput: {} requests/second", format_number(summary.throughput as u64));
	println!();
	println!("Latency summary:");
	println!("  min:       {}", format_latency(summary.latency_min_us));
	println!("  avg:       {}", format_latency_f64(summary.latency_avg_us));
	println!("  max:       {}", format_latency(summary.latency_max_us));
	println!("  p50:       {}", format_latency(summary.latency_p50_us));
	println!("  p90:       {}", format_latency(summary.latency_p90_us));
	println!("  p95:       {}", format_latency(summary.latency_p95_us));
	println!("  p99:       {}", format_latency(summary.latency_p99_us));
	println!("  p99.9:     {}", format_latency(summary.latency_p999_us));
	println!();
	println!(
		"Successful: {} / {} ({:.2}% success rate)",
		format_number(summary.successful_requests),
		format_number(summary.total_requests),
		100.0 - summary.error_rate()
	);
	println!("Errors: {} ({:.2}%)", format_number(summary.failed_requests), summary.error_rate());

	if !summary.top_errors.is_empty() {
		println!();
		println!("Top errors:");
		for (error, count) in &summary.top_errors {
			println!("  {} - {}", count, error);
		}
	}
}

/// Groups digits with commas.
fn format_number(n: u64) -> String {
	let s = n.to_string();
	let mut result = String::new();
	let chars: Vec<char> = s.chars().rev().collect();

	for (i, c) in chars.iter().enumerate() {
		if i > 0 && i % 3 == 0 {
			result.push(',');
		}
		result.push(*c);
	}

	result.chars().rev().collect()
}

fn format_latency(us: u64) -> String {
	format_latency_f64(us as f64)
}

/// Picks the unit that keeps the value readable: microseconds, milliseconds, then seconds.
fn format_latency_f64(us: f64) -> String {
	if us < 1000.0 {
		format!("{:.0} µs", us)
	} else if us < 1_000_000.0 {
		format!("{:.2} ms", us / 1000.0)
	} else {
		format!("{:.2} s", us / 1_000_000.0)
	}
}

pub fn print_progress(current: u64, rate: u64) {
	eprint!("\r{} requests completed ({}/s)    ", format_number(current), format_number(rate));
}

pub fn clear_progress() {
	eprintln!();
}
