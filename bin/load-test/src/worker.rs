// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Instant,
};

use hdrhistogram::Histogram;
use rand::{SeedableRng, rngs::StdRng};
use reifydb_value::value::duration::Duration;

use crate::{client::Client, metrics::Metrics, runner::Runner};

pub struct Worker {
	client: Client,
	runner: Arc<Runner>,
	metrics: Arc<Metrics>,
	rng: StdRng,
	/// Latency is recorded here rather than in Metrics, so workers never contend on its mutex.
	local_histogram: Histogram<u64>,
}

impl Worker {
	pub fn new(id: usize, client: Client, runner: Arc<Runner>, metrics: Arc<Metrics>, seed: u64) -> Self {
		// Unique per worker but derived from the run seed, so a run stays reproducible.
		let rng = StdRng::seed_from_u64(seed.wrapping_add(id as u64));

		// Must match the global histogram's bounds for merge_histogram to accept it.
		let local_histogram = Histogram::new_with_bounds(1, 60_000_000, 3).expect("Failed to create histogram");

		Self {
			client,
			runner,
			metrics,
			rng,
			local_histogram,
		}
	}

	pub async fn run_requests(&mut self, count: u64) {
		for _ in 0..count {
			self.execute_one().await;
		}
	}

	pub async fn run_duration(&mut self, duration: Duration, stop_signal: Arc<AtomicBool>) {
		let deadline = Instant::now() + duration.to_std();

		while Instant::now() < deadline && !stop_signal.load(Ordering::Relaxed) {
			self.execute_one().await;
		}
	}

	async fn execute_one(&mut self) {
		let operation = self.runner.next_operation(&mut self.rng);

		let start = Instant::now();
		let result = self.client.execute(&operation).await;
		let latency = start.elapsed();

		let latency_us = latency.as_micros() as u64;

		match result {
			Ok(()) => {
				self.metrics.record_success_count_only();
				let clamped = latency_us.clamp(1, 60_000_000);
				self.local_histogram.record(clamped).ok();
			}
			Err(e) => {
				self.metrics.record_error(&e.to_string());
			}
		}
	}

	pub fn histogram(&self) -> &Histogram<u64> {
		&self.local_histogram
	}

	pub fn into_client(self) -> Client {
		self.client
	}
}
