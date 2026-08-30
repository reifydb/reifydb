// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::{
		Mutex, Once,
		atomic::{AtomicU64, Ordering},
	},
	thread::{Builder, sleep},
	time::{Duration, Instant},
};

const SLOTS: usize = 16;
const SLOW: u128 = 1000;

pub static LIFECYCLE_MASK: AtomicU64 = AtomicU64::new(0);
static NAMES: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

static CALLS: AtomicU64 = AtomicU64::new(0);
static MICROS: AtomicU64 = AtomicU64::new(0);
static SLOW_CALLS: AtomicU64 = AtomicU64::new(0);
static SLOW_MICROS: AtomicU64 = AtomicU64::new(0);
static SLOW_BY_SLOT: [AtomicU64; SLOTS] = [const { AtomicU64::new(0) }; SLOTS];
static MICROS_BY_SLOT: [AtomicU64; SLOTS] = [const { AtomicU64::new(0) }; SLOTS];

pub const PHASES: [&str; 10] = [
	"register_in_flight",
	"wait_for_mark",
	"tm_new",
	"engine_rest",
	"commit_lease",
	"commit_optimize",
	"commit_store",
	"commit_emit",
	"commit_done",
	"commit_finalize_total",
];
static PHASE_CALLS: [AtomicU64; PHASES.len()] = [const { AtomicU64::new(0) }; PHASES.len()];
static PHASE_MICROS: [AtomicU64; PHASES.len()] = [const { AtomicU64::new(0) }; PHASES.len()];
static PHASE_MAX: [AtomicU64; PHASES.len()] = [const { AtomicU64::new(0) }; PHASES.len()];

static EMIT_DROPPED: AtomicU64 = AtomicU64::new(0);

pub fn emit_dropped() {
	EMIT_DROPPED.fetch_add(1, Ordering::Relaxed);
}

pub struct PhaseScope(usize, Instant);

impl PhaseScope {
	pub fn enter(phase: usize) -> Self {
		PhaseScope(phase, Instant::now())
	}
}

impl Drop for PhaseScope {
	fn drop(&mut self) {
		let micros = self.1.elapsed().as_micros() as u64;
		PHASE_CALLS[self.0].fetch_add(1, Ordering::Relaxed);
		PHASE_MICROS[self.0].fetch_add(micros, Ordering::Relaxed);
		PHASE_MAX[self.0].fetch_max(micros, Ordering::Relaxed);
	}
}

pub fn register(index: usize, name: &'static str) {
	let mut names = NAMES.lock().unwrap();
	if names.len() <= index {
		names.resize(index + 1, "?");
	}
	names[index] = name;
}

pub struct LifecycleScope(u64);

impl LifecycleScope {
	pub fn enter(index: usize) -> Self {
		start();
		let bit = 1u64 << index.min(SLOTS - 2);
		LIFECYCLE_MASK.fetch_or(bit, Ordering::Relaxed);
		LifecycleScope(bit)
	}
}

impl Drop for LifecycleScope {
	fn drop(&mut self) {
		LIFECYCLE_MASK.fetch_and(!self.0, Ordering::Relaxed);
	}
}

pub struct QueryScope(Instant);

impl QueryScope {
	pub fn enter() -> Self {
		start();
		QueryScope(Instant::now())
	}
}

impl Drop for QueryScope {
	fn drop(&mut self) {
		let micros = self.0.elapsed().as_micros();
		CALLS.fetch_add(1, Ordering::Relaxed);
		MICROS.fetch_add(micros as u64, Ordering::Relaxed);
		if micros < SLOW {
			return;
		}
		SLOW_CALLS.fetch_add(1, Ordering::Relaxed);
		SLOW_MICROS.fetch_add(micros as u64, Ordering::Relaxed);
		let mask = LIFECYCLE_MASK.load(Ordering::Relaxed);
		if mask == 0 {
			SLOW_BY_SLOT[0].fetch_add(1, Ordering::Relaxed);
			MICROS_BY_SLOT[0].fetch_add(micros as u64, Ordering::Relaxed);
			return;
		}
		for index in 0..SLOTS - 1 {
			if mask & (1u64 << index) == 0 {
				continue;
			}
			SLOW_BY_SLOT[index + 1].fetch_add(1, Ordering::Relaxed);
			MICROS_BY_SLOT[index + 1].fetch_add(micros as u64, Ordering::Relaxed);
		}
	}
}

static PROBE: Once = Once::new();

fn start() {
	PROBE.call_once(|| {
		Builder::new()
			.name("bq-probe".into())
			.spawn(|| {
				let mut last = [0u64; 4];
				let mut last_emit_dropped = 0u64;
				let mut last_slot = [0u64; SLOTS];
				let mut last_slot_micros = [0u64; SLOTS];
				let mut last_phase = [0u64; PHASES.len()];
				let mut last_phase_micros = [0u64; PHASES.len()];
				loop {
					sleep(Duration::from_secs(10));
					let now = [
						CALLS.load(Ordering::Relaxed),
						MICROS.load(Ordering::Relaxed),
						SLOW_CALLS.load(Ordering::Relaxed),
						SLOW_MICROS.load(Ordering::Relaxed),
					];
					let calls = now[0] - last[0];
					let slow = now[2] - last[2];
					println!(
						"[bq] calls={} total_ms={} slow={} slow_ms={} slow_share={:.2}%",
						calls,
						(now[1] - last[1]) / 1000,
						slow,
						(now[3] - last[3]) / 1000,
						if calls > 0 { slow as f64 * 100.0 / calls as f64 } else { 0.0 }
					);
					let names = NAMES.lock().unwrap();
					println!("[bq]   attribution set: overlapping slices count in every task, columns do not sum to slow_ms");
					for slot in 0..SLOTS {
						let count = SLOW_BY_SLOT[slot].load(Ordering::Relaxed);
						let micros = MICROS_BY_SLOT[slot].load(Ordering::Relaxed);
						let delta = count - last_slot[slot];
						if delta > 0 {
							let label = match slot {
								0 => "<lifecycle idle>",
								n => names.get(n - 1).copied().unwrap_or("?"),
							};
							println!(
								"[bq]     set {label}: slow={} ms={}",
								delta,
								(micros - last_slot_micros[slot]) / 1000
							);
						}
						last_slot[slot] = count;
						last_slot_micros[slot] = micros;
					}
					for phase in 0..PHASES.len() {
						let calls = PHASE_CALLS[phase].load(Ordering::Relaxed);
						let micros = PHASE_MICROS[phase].load(Ordering::Relaxed);
						let delta = calls - last_phase[phase];
						if delta > 0 {
							println!(
								"[bq]   phase {}: calls={} ms={} max_us={}",
								PHASES[phase],
								delta,
								(micros - last_phase_micros[phase]) / 1000,
								PHASE_MAX[phase].swap(0, Ordering::Relaxed)
							);
						}
						last_phase[phase] = calls;
						last_phase_micros[phase] = micros;
					}
					let emit_dropped = EMIT_DROPPED.load(Ordering::Relaxed);
					if emit_dropped != last_emit_dropped {
						println!("[bq]   emit_dropped={}", emit_dropped - last_emit_dropped);
						last_emit_dropped = emit_dropped;
					}
					last = now;
				}
			})
			.expect("bq probe thread");
	});
}
