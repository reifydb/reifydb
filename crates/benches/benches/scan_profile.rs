// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[allow(clippy::disallowed_types)]
use std::time::Duration;
use std::{cell::RefCell, collections::HashMap, sync::Arc, time::Instant};

use rand::{SeedableRng, rngs::StdRng};
use reifydb::{Database, embedded};
use reifydb_allocator::set_global_allocator;
use reifydb_benches::{BenchReport, env_u64};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_testing_scenario::{query::OperationKind, registry::by_name, scenario::Scenario};
use tracing::{Id, Subscriber, subscriber::set_default};
use tracing_subscriber::{
	Registry,
	layer::{Context, Layer, SubscriberExt},
	registry::LookupSpan,
};

set_global_allocator!();

const DEFAULT_SCALE: u64 = 100_000;
const DEFAULT_ITERATIONS: u64 = 20;
const DEFAULT_WARMUP: u64 = 3;

#[derive(Default, Clone, Copy)]
#[allow(clippy::disallowed_types)]
struct Stat {
	inclusive: Duration,
	exclusive: Duration,
	calls: u64,
}

#[allow(clippy::disallowed_types)]
struct Frame {
	name: &'static str,
	entered: Instant,
	child: Duration,
}

thread_local! {
	static STACK: RefCell<Vec<Frame>> = const { RefCell::new(Vec::new()) };
}

#[derive(Clone)]
struct Timing {
	stats: Arc<Mutex<HashMap<&'static str, Stat>>>,
}

impl Timing {
	fn new() -> Self {
		Self {
			stats: Arc::new(Mutex::new(HashMap::new())),
		}
	}

	fn reset(&self) {
		self.stats.lock().clear();
	}

	fn snapshot(&self) -> Vec<(&'static str, Stat)> {
		let mut rows: Vec<(&'static str, Stat)> =
			self.stats.lock().iter().map(|(name, stat)| (*name, *stat)).collect();
		rows.sort_by(|a, b| b.1.exclusive.cmp(&a.1.exclusive));
		rows
	}
}

impl<S> Layer<S> for Timing
where
	S: Subscriber + for<'a> LookupSpan<'a>,
{
	fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
		let Some(span) = ctx.span(id) else {
			return;
		};
		let name = span.name();
		STACK.with(|stack| {
			stack.borrow_mut().push(Frame {
				name,
				entered: Instant::now(),
				child: Duration::ZERO,
			})
		});
	}

	fn on_exit(&self, _id: &Id, _ctx: Context<'_, S>) {
		let finished = STACK.with(|stack| {
			let mut stack = stack.borrow_mut();
			let frame = stack.pop()?;
			let inclusive = frame.entered.elapsed();
			if let Some(parent) = stack.last_mut() {
				parent.child += inclusive;
			}
			Some((frame.name, inclusive, inclusive.saturating_sub(frame.child)))
		});

		let Some((name, inclusive, exclusive)) = finished else {
			return;
		};

		let mut stats = self.stats.lock();
		let entry = stats.entry(name).or_default();
		entry.inclusive += inclusive;
		entry.exclusive += exclusive;
		entry.calls += 1;
	}
}

fn seed(db: &Database, scenario: &Scenario, scale: u64) {
	for statement in scenario.setup_statements(scale) {
		let outcome = match statement.kind {
			OperationKind::Admin => db.admin_as_root(&statement.rql, ()),
			OperationKind::Command => db.command_as_root(&statement.rql, ()),
			OperationKind::Query => db.query_as_root(&statement.rql, ()),
		};
		outcome.unwrap_or_else(|e| panic!("scan setup rejected `{}`: {}", statement.rql, e));
	}
}

#[allow(clippy::disallowed_types)]
fn run(db: &Database, rql: &str, iterations: u64) -> Duration {
	let started = Instant::now();
	for _ in 0..iterations {
		db.query_as_root(rql, ()).expect("full_scan executes");
	}
	started.elapsed()
}

fn main() {
	let scale = env_u64("SCALES", DEFAULT_SCALE);
	let iterations = env_u64("ITERATIONS", DEFAULT_ITERATIONS);
	let warmup = env_u64("WARMUP", DEFAULT_WARMUP);

	let scenario = by_name("scan").expect("scan scenario is registered");
	let query = scenario.query("full_scan").expect("scan scenario defines full_scan");
	let rql = query.rql.render(&mut StdRng::seed_from_u64(0), scale, 0);

	let db = embedded::memory().build().expect("embedded database builds");
	seed(&db, &scenario, scale);
	println!("scenario=scan query=full_scan scale={} rql={}", scale, rql);

	run(&db, &rql, warmup);

	let baseline = run(&db, &rql, iterations);

	let timing = Timing::new();
	let _guard = set_default(Registry::default().with(timing.clone()));

	run(&db, &rql, warmup);
	timing.reset();
	let observed = run(&db, &rql, iterations);

	let baseline_per_query = baseline / iterations as u32;
	let observed_per_query = observed / iterations as u32;
	let overhead = observed_per_query.saturating_sub(baseline_per_query);

	println!(
		"baseline_per_query_us={} instrumented_per_query_us={} observer_overhead_pct={:.1}",
		baseline_per_query.as_micros(),
		observed_per_query.as_micros(),
		overhead.as_secs_f64() / baseline_per_query.as_secs_f64() * 100.0
	);

	let mut report = BenchReport::new("scan-profile");
	let rows = timing.snapshot();

	for (name, stat) in &rows {
		let exclusive_per_query = stat.exclusive / iterations as u32;
		let inclusive_per_query = stat.inclusive / iterations as u32;
		let share = stat.exclusive.as_secs_f64() / baseline.as_secs_f64() * 100.0;

		report.record_throughput(
			&format!(
				"stage={} self_us={} incl_us={} calls_per_query={} self_share_pct={:.1}",
				name,
				exclusive_per_query.as_micros(),
				inclusive_per_query.as_micros(),
				stat.calls / iterations,
				share
			),
			stat.calls,
			stat.exclusive,
		);
	}

	report.save();
}
