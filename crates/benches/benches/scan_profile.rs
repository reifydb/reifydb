// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::RefCell, cmp::Reverse, collections::HashMap, sync::Arc};

use rand::{SeedableRng, rngs::StdRng};
use reifydb::{Database, embedded};
use reifydb_allocator::set_global_allocator;
use reifydb_benches::{BenchReport, env_opt, env_u64};
use reifydb_core::interface::catalog::config::ConfigKey;
use reifydb_runtime::{
	context::clock::{Clock, Instant},
	sync::mutex::Mutex,
};
use reifydb_testing_scenario::{query::OperationKind, registry::by_name, scenario::Scenario};
use reifydb_value::value::{Value, duration::Duration as ValueDuration};
use tracing::{Id, Subscriber, subscriber::set_default};
use tracing_subscriber::{
	Registry,
	layer::{Context, Layer, SubscriberExt},
	registry::LookupSpan,
};

set_global_allocator!();

const DEFAULT_SCENARIO: &str = "scan";
const DEFAULT_QUERY: &str = "full_scan";
const DEFAULT_BATCH_SIZE: u64 = 128;
const DEFAULT_SCALE: u64 = 100_000;
const DEFAULT_ITERATIONS: u64 = 20;
const DEFAULT_WARMUP: u64 = 3;

#[derive(Default, Clone, Copy)]
struct Stat {
	inclusive: ValueDuration,
	exclusive: ValueDuration,
	calls: u64,
}

struct Frame {
	name: &'static str,
	entered: Instant,
	child: ValueDuration,
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
		rows.sort_by_key(|b| Reverse(b.1.exclusive));
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
				entered: Clock::Real.instant(),
				child: ValueDuration::zero(),
			})
		});
	}

	fn on_exit(&self, _id: &Id, _ctx: Context<'_, S>) {
		let finished = STACK.with(|stack| {
			let mut stack = stack.borrow_mut();
			let frame = stack.pop()?;
			let inclusive = frame.entered.elapsed();
			if let Some(parent) = stack.last_mut() {
				parent.child = parent
					.child
					.try_add(inclusive.into())
					.expect("accumulated child time is a representable duration");
			}
			Some((frame.name, inclusive, inclusive.saturating_sub(frame.child.to_std())))
		});

		let Some((name, inclusive, exclusive)) = finished else {
			return;
		};

		let mut stats = self.stats.lock();
		let entry = stats.entry(name).or_default();
		entry.inclusive = entry
			.inclusive
			.try_add(inclusive.into())
			.expect("accumulated inclusive time is a representable duration");
		entry.exclusive = entry
			.exclusive
			.try_add(exclusive.into())
			.expect("accumulated exclusive time is a representable duration");
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
		outcome.unwrap_or_else(|e| panic!("scenario setup rejected `{}`: {}", statement.rql, e));
	}
}

fn run(db: &Database, rql: &str, iterations: u64) -> ValueDuration {
	let started = Clock::Real.instant();
	for _ in 0..iterations {
		db.query_as_root(rql, ()).expect("profiled query executes");
	}
	started.elapsed().into()
}

fn main() {
	let scale = env_u64("SCALES", DEFAULT_SCALE);
	let iterations = env_u64("ITERATIONS", DEFAULT_ITERATIONS);
	let warmup = env_u64("WARMUP", DEFAULT_WARMUP);

	let scenario_name = env_opt("SCENARIO").unwrap_or_else(|| DEFAULT_SCENARIO.to_string());
	let scenario =
		by_name(&scenario_name).unwrap_or_else(|| panic!("scenario '{}' is not registered", scenario_name));
	let query_name = env_opt("QUERY").unwrap_or_else(|| DEFAULT_QUERY.to_string());
	let query = scenario
		.query(&query_name)
		.unwrap_or_else(|| panic!("scenario '{}' defines no query '{}'", scenario_name, query_name));
	let rql = query.rql.render(&mut StdRng::seed_from_u64(0), scale, 0);

	let batch_size = env_u64("BATCH_SIZE", DEFAULT_BATCH_SIZE) as u16;
	let db = embedded::memory()
		.with_config(ConfigKey::QueryRowBatchSize, Value::Uint2(batch_size))
		.build()
		.expect("embedded database builds");
	seed(&db, &scenario, scale);
	println!(
		"scenario={} query={} scale={} batch_size={} rql={}",
		scenario_name, query_name, scale, batch_size, rql
	);

	run(&db, &rql, warmup);

	let baseline = run(&db, &rql, iterations);

	let timing = Timing::new();
	let _guard = set_default(Registry::default().with(timing.clone()));

	run(&db, &rql, warmup);
	timing.reset();
	let observed = run(&db, &rql, iterations);

	let baseline_per_query = baseline.to_std() / iterations as u32;
	let observed_per_query = observed.to_std() / iterations as u32;
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
		let exclusive_per_query = stat.exclusive.to_std() / iterations as u32;
		let inclusive_per_query = stat.inclusive.to_std() / iterations as u32;
		let share = stat.exclusive.to_std().as_secs_f64() / baseline.to_std().as_secs_f64() * 100.0;

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
			stat.exclusive.to_std(),
		);
	}

	report.save();
}
