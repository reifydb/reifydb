// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	collections::HashMap,
	sync::{
		Mutex,
		atomic::{AtomicU64, Ordering},
	},
	time::Instant,
};

use hdrhistogram::Histogram;

/// Metrics collector for benchmark results
pub struct Metrics {
	// Atomic counters for fast path
	pub total_requests: AtomicU64,
	pub successful_requests: AtomicU64,
	pub failed_requests: AtomicU64,

	// Latency histogram (protected by mutex)
	// Range: 1 microsecond to 60 seconds, 3 significant figures
	latency_histogram: Mutex<Histogram<u64>>,

	// Start time for throughput calculation
	start_time: Mutex<Option<Instant>>,

	// Error categorization
	error_counts: Mutex<HashMap<String, u64>>,
}

impl Metrics {
	/// Create a new metrics collector
	pub fn new() -> Self {
		Self {
			total_requests: AtomicU64::new(0),
			successful_requests: AtomicU64::new(0),
			failed_requests: AtomicU64::new(0),
			// 1 microsecond to 60 seconds (60_000_000 microseconds), 3 significant figures
			latency_histogram: Mutex::new(
				Histogram::new_with_bounds(1, 60_000_000, 3).expect("Failed to create histogram"),
			),
			start_time: Mutex::new(None),
			error_counts: Mutex::new(HashMap::new()),
		}
	}

	/// Start the benchmark timer
	pub fn start(&self) {
		let mut start = self.start_time.lock().unwrap();
		*start = Some(Instant::now());
	}

	/// Record only the success count (no latency) - for use with per-worker histograms
	pub fn record_success_count_only(&self) {
		self.successful_requests.fetch_add(1, Ordering::Relaxed);
		self.total_requests.fetch_add(1, Ordering::Relaxed);
	}

	/// Merge a worker's histogram into the global histogram
	pub fn merge_histogram(&self, other: &Histogram<u64>) {
		self.latency_histogram.lock().unwrap().add(other).ok();
	}

	/// Record a failed request
	pub fn record_error(&self, error: &str) {
		self.failed_requests.fetch_add(1, Ordering::Relaxed);
		self.total_requests.fetch_add(1, Ordering::Relaxed);

		// Categorize error (truncate long messages)
		let error_key = if error.len() > 100 {
			format!("{}...", &error[..97])
		} else {
			error.to_string()
		};

		*self.error_counts.lock().unwrap().entry(error_key).or_insert(0) += 1;
	}

	/// Reset metrics for a new run (e.g., after warmup)
	pub fn reset(&self) {
		self.total_requests.store(0, Ordering::Relaxed);
		self.successful_requests.store(0, Ordering::Relaxed);
		self.failed_requests.store(0, Ordering::Relaxed);
		self.latency_histogram.lock().unwrap().reset();
		self.error_counts.lock().unwrap().clear();
		*self.start_time.lock().unwrap() = Some(Instant::now());
	}

	/// Get the current request count
	pub fn current_count(&self) -> u64 {
		self.total_requests.load(Ordering::Relaxed)
	}

	/// Generate a summary of the metrics
	pub fn summary(&self) -> MetricsSummary {
		let histogram = self.latency_histogram.lock().unwrap();
		let start = self.start_time.lock().unwrap();
		let duration = start.map(|s| s.elapsed()).unwrap_or_default();
		let total = self.total_requests.load(Ordering::Relaxed);
		let successful = self.successful_requests.load(Ordering::Relaxed);
		let failed = self.failed_requests.load(Ordering::Relaxed);

		let duration_secs = duration.as_secs_f64();
		let throughput = if duration_secs > 0.0 {
			total as f64 / duration_secs
		} else {
			0.0
		};

		MetricsSummary {
			total_requests: total,
			successful_requests: successful,
			failed_requests: failed,
			duration_secs,
			throughput,
			latency_min_us: histogram.min(),
			latency_max_us: histogram.max(),
			latency_avg_us: histogram.mean(),
			latency_p50_us: histogram.value_at_quantile(0.50),
			latency_p90_us: histogram.value_at_quantile(0.90),
			latency_p95_us: histogram.value_at_quantile(0.95),
			latency_p99_us: histogram.value_at_quantile(0.99),
			latency_p999_us: histogram.value_at_quantile(0.999),
			top_errors: self.top_errors(5),
		}
	}

	/// Get the top N errors by count
	fn top_errors(&self, n: usize) -> Vec<(String, u64)> {
		let errors = self.error_counts.lock().unwrap();
		let mut sorted: Vec<_> = errors.iter().map(|(k, v)| (k.clone(), *v)).collect();
		sorted.sort_by(|a, b| b.1.cmp(&a.1));
		sorted.truncate(n);
		sorted
	}
}

impl Default for Metrics {
	fn default() -> Self {
		Self::new()
	}
}

/// Summary of benchmark metrics
#[derive(Debug)]
pub struct MetricsSummary {
	pub total_requests: u64,
	pub successful_requests: u64,
	pub failed_requests: u64,
	pub duration_secs: f64,
	pub throughput: f64,
	pub latency_min_us: u64,
	pub latency_max_us: u64,
	pub latency_avg_us: f64,
	pub latency_p50_us: u64,
	pub latency_p90_us: u64,
	pub latency_p95_us: u64,
	pub latency_p99_us: u64,
	pub latency_p999_us: u64,
	pub top_errors: Vec<(String, u64)>,
}

impl MetricsSummary {
	/// Get the error rate as a percentage
	pub fn error_rate(&self) -> f64 {
		if self.total_requests > 0 {
			(self.failed_requests as f64 / self.total_requests as f64) * 100.0
		} else {
			0.0
		}
	}
}
