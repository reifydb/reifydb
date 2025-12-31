// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::{Duration, Instant},
};

use hdrhistogram::Histogram;
use rand::{SeedableRng, rngs::StdRng};

use crate::{client::Client, metrics::Metrics, workload::Workload};

/// A worker that executes operations against the server
pub struct Worker {
	/// Worker ID (for logging/debugging)
	#[allow(dead_code)]
	id: usize,
	/// Client connection
	client: Client,
	/// Workload generator
	workload: Arc<dyn Workload>,
	/// Metrics collector (for counts only)
	metrics: Arc<Metrics>,
	/// Random number generator
	rng: StdRng,
	/// Local histogram for latency recording (no mutex contention)
	local_histogram: Histogram<u64>,
}

impl Worker {
	/// Create a new worker
	pub fn new(id: usize, client: Client, workload: Arc<dyn Workload>, metrics: Arc<Metrics>, seed: u64) -> Self {
		// Each worker gets a unique but deterministic RNG
		let rng = StdRng::seed_from_u64(seed.wrapping_add(id as u64));

		// Create local histogram with same bounds as global
		let local_histogram = Histogram::new_with_bounds(1, 60_000_000, 3).expect("Failed to create histogram");

		Self {
			id,
			client,
			workload,
			metrics,
			rng,
			local_histogram,
		}
	}

	/// Run a fixed number of requests
	pub async fn run_requests(&mut self, count: u64) {
		for _ in 0..count {
			self.execute_one().await;
		}
	}

	/// Run until duration expires or stop signal is received
	pub async fn run_duration(&mut self, duration: Duration, stop_signal: Arc<AtomicBool>) {
		let deadline = Instant::now() + duration;

		while Instant::now() < deadline && !stop_signal.load(Ordering::Relaxed) {
			self.execute_one().await;
		}
	}

	/// Execute a single operation and record metrics
	async fn execute_one(&mut self) {
		let operation = self.workload.next_operation(&mut self.rng, self.id);

		let start = Instant::now();
		let result = self.client.execute(&operation).await;
		let latency = start.elapsed();

		let latency_us = latency.as_micros() as u64;

		match result {
			Ok(()) => {
				// Record count atomically (fast path)
				self.metrics.record_success_count_only();
				// Record latency locally (no contention)
				let clamped = latency_us.clamp(1, 60_000_000);
				self.local_histogram.record(clamped).ok();
			}
			Err(e) => {
				self.metrics.record_error(&e.to_string());
			}
		}
	}

	/// Get a reference to the local histogram for merging
	pub fn histogram(&self) -> &Histogram<u64> {
		&self.local_histogram
	}

	/// Consume the worker and return the client for cleanup
	pub fn into_client(self) -> Client {
		self.client
	}
}
