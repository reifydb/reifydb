// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[allow(clippy::disallowed_types)]
use std::time::Duration;
use std::{
	env, fs,
	path::PathBuf,
	process::Command,
	time::{SystemTime, UNIX_EPOCH},
};

use hdrhistogram::Histogram;

pub fn env_opt(key: &str) -> Option<String> {
	env::var(key).ok().map(|raw| raw.trim().to_string()).filter(|raw| !raw.is_empty())
}

pub fn env_flag(key: &str) -> bool {
	env_opt(key).is_some_and(|raw| !matches!(raw.to_ascii_lowercase().as_str(), "0" | "false" | "no"))
}

pub fn env_u64(key: &str, fallback: u64) -> u64 {
	env_opt(key).and_then(|raw| raw.parse().ok()).unwrap_or(fallback)
}

pub fn env_usize(key: &str, fallback: usize) -> usize {
	env_opt(key).and_then(|raw| raw.parse().ok()).unwrap_or(fallback)
}

pub fn env_list_u64(key: &str, fallback: &[u64]) -> Vec<u64> {
	match env_opt(key) {
		Some(raw) => raw.split(',').filter_map(|part| part.trim().parse().ok()).collect(),
		None => fallback.to_vec(),
	}
}

pub fn env_list_usize(key: &str, fallback: &[usize]) -> Vec<usize> {
	match env_opt(key) {
		Some(raw) => raw.split(',').filter_map(|part| part.trim().parse().ok()).collect(),
		None => fallback.to_vec(),
	}
}

pub fn env_select<T: Copy>(key: &str, all: &[(&str, T)], fallback: &[T]) -> Vec<T> {
	match env_opt(key) {
		Some(raw) => raw
			.split(',')
			.filter_map(|part| {
				let wanted = part.trim();
				all.iter().find(|(name, _)| *name == wanted).map(|(_, value)| *value)
			})
			.collect(),
		None => fallback.to_vec(),
	}
}

pub fn latency_histogram() -> Histogram<u64> {
	Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("static histogram bounds are valid")
}

pub fn merge(histograms: impl IntoIterator<Item = Histogram<u64>>) -> Histogram<u64> {
	let mut merged = latency_histogram();
	for histogram in histograms {
		merged.add(&histogram).expect("histograms share static bounds");
	}
	merged
}

#[allow(clippy::disallowed_types)]
pub fn median_by_throughput<S>(samples: &[S], key: impl Fn(&S) -> (u64, Duration)) -> &S {
	assert!(!samples.is_empty(), "median of an empty sample set is undefined");
	let mut ranked: Vec<(f64, usize)> = samples
		.iter()
		.enumerate()
		.map(|(index, sample)| {
			let (ops, elapsed) = key(sample);
			(ops as f64 / elapsed.as_secs_f64(), index)
		})
		.collect();
	ranked.sort_by(|a, b| a.0.partial_cmp(&b.0).expect("throughput is finite"));
	&samples[ranked[ranked.len() / 2].1]
}

pub struct BenchReport {
	name: String,
	lines: Vec<String>,
}

impl BenchReport {
	pub fn new(name: impl Into<String>) -> Self {
		Self {
			name: name.into(),
			lines: Vec::new(),
		}
	}

	#[allow(clippy::disallowed_types)]
	pub fn record(&mut self, label: &str, ops: u64, elapsed: Duration, histogram: &Histogram<u64>) {
		let ops_per_sec = ops as f64 / elapsed.as_secs_f64();
		let line = format!(
			"{label} ops={ops} elapsed_ms={} ops_per_sec={ops_per_sec:.0} p50_ns={} p90_ns={} p99_ns={} p999_ns={} max_ns={}",
			elapsed.as_millis(),
			histogram.value_at_quantile(0.50),
			histogram.value_at_quantile(0.90),
			histogram.value_at_quantile(0.99),
			histogram.value_at_quantile(0.999),
			histogram.max(),
		);
		println!("{line}");
		self.lines.push(line);
	}

	#[allow(clippy::disallowed_types)]
	pub fn record_throughput(&mut self, label: &str, ops: u64, elapsed: Duration) {
		let ops_per_sec = ops as f64 / elapsed.as_secs_f64();
		let line = format!("{label} ops={ops} elapsed_ms={} ops_per_sec={ops_per_sec:.0}", elapsed.as_millis());
		println!("{line}");
		self.lines.push(line);
	}

	pub fn save(&self) {
		let dir = results_dir();
		fs::create_dir_all(&dir).expect("bench results directory is creatable");
		let path = dir.join(format!("{}-{}-{}.txt", self.name, git_revision(), unix_seconds()));
		fs::write(&path, self.lines.join("\n") + "\n").expect("bench results file is writable");
		println!("results written to {}", path.display());
	}
}

fn results_dir() -> PathBuf {
	let target = env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".into());
	PathBuf::from(target).join("bench-results")
}

fn git_revision() -> String {
	let revision = Command::new("git")
		.args(["rev-parse", "--short", "HEAD"])
		.output()
		.ok()
		.filter(|output| output.status.success())
		.map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
		.unwrap_or_else(|| "unknown".into());
	let dirty = Command::new("git")
		.args(["status", "--porcelain"])
		.output()
		.ok()
		.filter(|output| output.status.success())
		.map(|output| !output.stdout.is_empty())
		.unwrap_or(false);
	if dirty {
		format!("{revision}-dirty")
	} else {
		revision
	}
}

#[allow(clippy::disallowed_methods)]
fn unix_seconds() -> u64 {
	SystemTime::now().duration_since(UNIX_EPOCH).expect("system clock is past the epoch").as_secs()
}
