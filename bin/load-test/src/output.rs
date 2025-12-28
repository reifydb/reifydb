// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	config::{Config, Protocol},
	metrics::MetricsSummary,
};

/// Print the benchmark header
pub fn print_header(config: &Config, description: &str) {
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
	println!("Connections: {}", config.connections);

	if let Some(duration) = config.duration {
		println!("Duration: {:?}", duration);
	} else {
		println!("Requests: {}", format_number(config.requests));
	}
	println!();
}

/// Print the benchmark summary in redis-benchmark style
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

	// Print top errors if any
	if !summary.top_errors.is_empty() {
		println!();
		println!("Top errors:");
		for (error, count) in &summary.top_errors {
			println!("  {} - {}", count, error);
		}
	}
}

/// Format a number with thousands separators
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

/// Format latency in microseconds to a human-readable string
fn format_latency(us: u64) -> String {
	format_latency_f64(us as f64)
}

/// Format latency from f64 microseconds
fn format_latency_f64(us: f64) -> String {
	if us < 1000.0 {
		// Sub-millisecond: show microseconds
		format!("{:.0} µs", us)
	} else if us < 1_000_000.0 {
		// Sub-second: show milliseconds
		format!("{:.2} ms", us / 1000.0)
	} else {
		// >= 1 second: show seconds
		format!("{:.2} s", us / 1_000_000.0)
	}
}

/// Print progress update (for non-quiet mode)
pub fn print_progress(current: u64, rate: u64) {
	eprint!("\r{} requests completed ({}/s)    ", format_number(current), format_number(rate));
}

/// Clear the progress line
pub fn clear_progress() {
	eprintln!();
}
