// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
	thread,
	time::Instant,
};

use hdrhistogram::Histogram;
use reifydb_benches::{BenchReport, latency_histogram, merge};
use reifydb_core::common::CommitVersion;
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};
use reifydb_transaction::multi::watermark::watermark::WaterMark;
use reifydb_value::value::duration::Duration as ValueDuration;

const PIPELINE_OPS: u64 = 1_000_000;
const BURST_OPS_PER_THREAD: u64 = 5_000;
const WAIT_FAST_PATH_OPS: u64 = 1_000_000;

fn pipeline(report: &mut BenchReport, threads: usize) {
	let watermark = Arc::new(WaterMark::new("bench-pipeline".into()));
	let counter = Arc::new(AtomicU64::new(0));

	let start = Instant::now();
	let mut handles = Vec::with_capacity(threads);
	for _ in 0..threads {
		let watermark = watermark.clone();
		let counter = counter.clone();
		handles.push(thread::spawn(move || {
			let mut histogram = latency_histogram();
			loop {
				let version = counter.fetch_add(1, Ordering::Relaxed) + 1;
				if version > PIPELINE_OPS {
					break;
				}
				let op_start = Instant::now();
				watermark.register_in_flight(CommitVersion(version));
				watermark.mark_finished(CommitVersion(version));
				histogram.record(op_start.elapsed().as_nanos() as u64).expect("latency within bounds");
			}
			histogram
		}));
	}
	let histogram = merge(handles.into_iter().map(|handle| handle.join().expect("bench thread panicked")));
	let elapsed = start.elapsed();

	assert!(
		watermark.wait_for_mark_timeout(CommitVersion(PIPELINE_OPS), ValueDuration::from_seconds(30).unwrap()),
		"pipeline benchmark lost a mark: done_until={:?} expected {PIPELINE_OPS}",
		watermark.done_until()
	);
	report.record(&format!("pipeline threads={threads}"), PIPELINE_OPS, elapsed, &histogram);
}

fn burst(report: &mut BenchReport, threads: usize) {
	burst_against(report, threads, "burst", Arc::new(WaterMark::new("bench-burst".into())));
}

fn burst_with_advancer(report: &mut BenchReport, threads: usize) {
	let system = ActorSystem::testing(Clock::Real);
	let watermark = Arc::new(WaterMark::with_advancer("bench-burst-advancer".into(), &system.spawner()));
	burst_against(report, threads, "burst_advancer", watermark);
}

fn pipeline_with_advancer(report: &mut BenchReport, threads: usize) {
	let system = ActorSystem::testing(Clock::Real);
	let watermark = Arc::new(WaterMark::with_advancer("bench-pipeline-advancer".into(), &system.spawner()));
	let counter = Arc::new(AtomicU64::new(0));

	let start = Instant::now();
	let mut handles = Vec::with_capacity(threads);
	for _ in 0..threads {
		let watermark = watermark.clone();
		let counter = counter.clone();
		handles.push(thread::spawn(move || {
			let mut histogram = latency_histogram();
			loop {
				let version = counter.fetch_add(1, Ordering::Relaxed) + 1;
				if version > PIPELINE_OPS {
					break;
				}
				let op_start = Instant::now();
				watermark.register_in_flight(CommitVersion(version));
				watermark.mark_finished(CommitVersion(version));
				histogram.record(op_start.elapsed().as_nanos() as u64).expect("latency within bounds");
			}
			histogram
		}));
	}
	let histogram = merge(handles.into_iter().map(|handle| handle.join().expect("bench thread panicked")));
	let elapsed = start.elapsed();

	assert!(
		watermark.wait_for_mark_timeout(CommitVersion(PIPELINE_OPS), ValueDuration::from_seconds(30).unwrap()),
		"pipeline_advancer benchmark lost a mark: done_until={:?} expected {PIPELINE_OPS}",
		watermark.done_until()
	);
	report.record(&format!("pipeline_advancer threads={threads}"), PIPELINE_OPS, elapsed, &histogram);
}

fn burst_against(report: &mut BenchReport, threads: usize, label: &str, watermark: Arc<WaterMark>) {
	let total = BURST_OPS_PER_THREAD * threads as u64;

	let start = Instant::now();
	let mut handles = Vec::with_capacity(threads);
	for thread_id in 0..threads as u64 {
		let watermark = watermark.clone();
		handles.push(thread::spawn(move || {
			let mut histogram = latency_histogram();
			let first = thread_id * BURST_OPS_PER_THREAD + 1;
			let last = (thread_id + 1) * BURST_OPS_PER_THREAD;
			for version in first..=last {
				let op_start = Instant::now();
				watermark.register_in_flight(CommitVersion(version));
				watermark.mark_finished(CommitVersion(version));
				histogram.record(op_start.elapsed().as_nanos() as u64).expect("latency within bounds");
			}
			histogram
		}));
	}
	let histogram = merge(handles.into_iter().map(|handle| handle.join().expect("bench thread panicked")));
	let elapsed = start.elapsed();

	assert!(
		watermark.wait_for_mark_timeout(CommitVersion(total), ValueDuration::from_seconds(30).unwrap()),
		"burst benchmark lost a mark: done_until={:?} expected {total}",
		watermark.done_until()
	);
	report.record(&format!("{label} threads={threads}"), total, elapsed, &histogram);
}

fn wait_fast_path(report: &mut BenchReport) {
	let watermark = WaterMark::new("bench-wait".into());
	for version in 1..=1_000 {
		watermark.register_in_flight(CommitVersion(version));
		watermark.mark_finished(CommitVersion(version));
	}

	let timeout = ValueDuration::from_seconds(1).unwrap();
	let mut histogram = latency_histogram();
	let start = Instant::now();
	for _ in 0..WAIT_FAST_PATH_OPS {
		let op_start = Instant::now();
		let reached = watermark.wait_for_mark_timeout(CommitVersion(1_000), timeout);
		histogram.record(op_start.elapsed().as_nanos() as u64).expect("latency within bounds");
		assert!(reached, "fast path wait must succeed for an already reached mark");
	}
	let elapsed = start.elapsed();

	report.record("wait_fast_path threads=1", WAIT_FAST_PATH_OPS, elapsed, &histogram);
}

fn mixed_poll(report: &mut BenchReport, worker_threads: usize) {
	let watermark = Arc::new(WaterMark::new("bench-mixed".into()));
	let counter = Arc::new(AtomicU64::new(0));
	let stop = Arc::new(AtomicBool::new(false));

	let poller = {
		let watermark = watermark.clone();
		let stop = stop.clone();
		thread::spawn(move || {
			let mut polls: u64 = 0;
			while !stop.load(Ordering::Relaxed) {
				let _ = watermark.done_until();
				polls += 1;
			}
			polls
		})
	};

	let start = Instant::now();
	let mut handles = Vec::with_capacity(worker_threads);
	for _ in 0..worker_threads {
		let watermark = watermark.clone();
		let counter = counter.clone();
		handles.push(thread::spawn(move || {
			let mut histogram = latency_histogram();
			loop {
				let version = counter.fetch_add(1, Ordering::Relaxed) + 1;
				if version > PIPELINE_OPS {
					break;
				}
				let op_start = Instant::now();
				watermark.register_in_flight(CommitVersion(version));
				watermark.mark_finished(CommitVersion(version));
				histogram.record(op_start.elapsed().as_nanos() as u64).expect("latency within bounds");
			}
			histogram
		}));
	}
	let histogram: Histogram<u64> =
		merge(handles.into_iter().map(|handle| handle.join().expect("bench thread panicked")));
	let elapsed = start.elapsed();
	stop.store(true, Ordering::Relaxed);
	let polls = poller.join().expect("poller thread panicked");

	report.record(&format!("mixed_poll workers={worker_threads}"), PIPELINE_OPS, elapsed, &histogram);
	report.record_throughput(&format!("mixed_poll_reads workers={worker_threads}"), polls, elapsed);
}

fn main() {
	let mut report = BenchReport::new("watermark");
	for threads in [1, 2, 4, 8, 16, 32] {
		pipeline(&mut report, threads);
	}
	for threads in [1, 8, 16] {
		pipeline_with_advancer(&mut report, threads);
	}
	for threads in [8, 16] {
		burst(&mut report, threads);
		burst_with_advancer(&mut report, threads);
	}
	wait_fast_path(&mut report);
	mixed_poll(&mut report, 4);
	report.save();
}
