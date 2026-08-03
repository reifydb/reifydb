// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod transport;

use std::{
	sync::{
		Arc, Barrier,
		atomic::{AtomicU64, Ordering},
	},
	thread,
	time::{Duration, Instant},
};

use hdrhistogram::Histogram;
use rand::{SeedableRng, rngs::StdRng};
use reifydb::{Database, embedded, engine::engine::StandardEngine, server};
use reifydb_allocator::set_global_allocator;
use reifydb_benches::{
	BenchReport, env_list_u64, env_list_usize, env_opt, env_u64, latency_histogram, median_by_throughput, merge,
};
use reifydb_client::WireFormat;
use reifydb_testing_scenario::{
	query::{NamedQuery, OperationKind},
	registry,
	scenario::Scenario,
};
use rustls::crypto::ring::default_provider;

use crate::transport::{
	ALL_IDENTITIES, ALL_TRANSPORTS, DEFAULT_GRPC_PORT, DEFAULT_HTTP_PORT, DEFAULT_WS_PORT, Driver, Identity,
	Transport,
};

set_global_allocator!();

const DEFAULT_REPEATS: u64 = 5;
const SEED: u64 = 20260803;
const DEFAULT_SCALES: [u64; 2] = [10_000, 100_000];
const DEFAULT_THREADS: [usize; 3] = [1, 4, 16];
const DEFAULT_ITERATIONS: u64 = 20_000;
const DEFAULT_BUDGET_SECONDS: u64 = 3;
const DEFAULT_WARMUP: u64 = 200;
const OVERRUN_FACTOR: u32 = 2;

struct Sample {
	ops: u64,
	elapsed: Duration,
	latency: Histogram<u64>,
	compile_ns: u64,
	execute_ns: u64,
	attributed: bool,
}

fn identities() -> Vec<Identity> {
	match env_opt("IDENTITIES") {
		Some(raw) => raw.split(',').filter_map(Identity::parse).collect(),
		None => ALL_IDENTITIES.to_vec(),
	}
}

fn transports() -> Vec<Transport> {
	match env_opt("TRANSPORTS") {
		Some(raw) => raw.split(',').filter_map(Transport::parse).collect(),
		None => ALL_TRANSPORTS.to_vec(),
	}
}

fn wire_format() -> WireFormat {
	match env_opt("WIRE_FORMAT").as_deref() {
		Some("frames") => WireFormat::Frames,
		_ => WireFormat::Rbcf,
	}
}

fn build_database(serve: bool) -> Database {
	if !serve {
		return embedded::memory().build().expect("embedded database builds");
	}

	server::memory()
		.with_http(|http| http.bind_addr(format!("127.0.0.1:{}", DEFAULT_HTTP_PORT)))
		.with_ws(|ws| ws.bind_addr(format!("127.0.0.1:{}", DEFAULT_WS_PORT)))
		.with_grpc(|grpc| grpc.bind_addr(format!("127.0.0.1:{}", DEFAULT_GRPC_PORT)))
		.build()
		.expect("server database builds")
}

fn seed_database(db: &Database, scenario: &Scenario, scale: u64) -> Duration {
	let started = Instant::now();

	for statement in scenario.setup_statements(scale) {
		let outcome = match statement.kind {
			OperationKind::Admin => db.admin_as_root(&statement.rql, ()),
			OperationKind::Command => db.command_as_root(&statement.rql, ()),
			OperationKind::Query => db.query_as_root(&statement.rql, ()),
		};

		outcome.unwrap_or_else(|e| {
			panic!("scenario '{}' setup rejected `{}`: {}", scenario.name, statement.rql, e)
		});
	}

	started.elapsed()
}

#[allow(clippy::too_many_arguments)]
fn run_once(
	engine: &StandardEngine,
	transport: Transport,
	format: WireFormat,
	identity: Identity,
	token: Option<&str>,
	query: &NamedQuery,
	scale: u64,
	threads: usize,
	iterations: u64,
	budget: Duration,
	seed: u64,
) -> Sample {
	let sequence = Arc::new(AtomicU64::new(0));
	let per_thread = iterations / threads as u64;

	let ready = Arc::new(Barrier::new(threads + 1));
	let mut started = Instant::now();

	let outcomes: Vec<(Histogram<u64>, u64, u64, u64)> = thread::scope(|scope| {
		let handles: Vec<_> = (0..threads)
			.map(|index| {
				let sequence = Arc::clone(&sequence);
				let ready = Arc::clone(&ready);
				scope.spawn(move || {
					let driver = Driver::connect(transport, engine, format, identity, token);
					let mut rng = StdRng::seed_from_u64(seed.wrapping_add(index as u64));
					let mut latency = latency_histogram();
					let (mut compile_ns, mut execute_ns, mut ops) = (0u64, 0u64, 0u64);

					ready.wait();
					let deadline = Instant::now() + budget;

					for _ in 0..per_thread {
						if Instant::now() >= deadline {
							break;
						}

						let next = sequence.fetch_add(1, Ordering::Relaxed);
						let rql = query.rql.render(&mut rng, scale, next);

						let call = Instant::now();
						let timing = driver.execute(query.kind, &rql);
						let elapsed = call.elapsed();

						latency.record(elapsed.as_nanos().max(1) as u64).ok();
						if let Some(timing) = timing {
							compile_ns += timing.compile_ns;
							execute_ns += timing.execute_ns;
						}
						ops += 1;
					}

					(latency, compile_ns, execute_ns, ops)
				})
			})
			.collect();

		ready.wait();
		started = Instant::now();

		handles.into_iter().map(|handle| handle.join().expect("worker thread does not panic")).collect()
	});

	let elapsed = started.elapsed();
	let ops = outcomes.iter().map(|outcome| outcome.3).sum();

	Sample {
		ops,
		elapsed,
		latency: merge(outcomes.iter().map(|outcome| outcome.0.clone())),
		compile_ns: outcomes.iter().map(|outcome| outcome.1).sum(),
		execute_ns: outcomes.iter().map(|outcome| outcome.2).sum(),
		attributed: !transport.is_wire(),
	}
}

fn record(report: &mut BenchReport, label: &str, sample: &Sample) {
	report.record(&format!("{label} section=e2e"), sample.ops, sample.elapsed, &sample.latency);

	if !sample.attributed {
		return;
	}

	let ops = sample.ops.max(1);
	let compile_avg = sample.compile_ns / ops;
	let execute_avg = sample.execute_ns / ops;

	let e2e_avg = sample.latency.mean() as u64;

	let attributed = compile_avg.saturating_add(execute_avg);
	let overhead_avg = e2e_avg.saturating_sub(attributed);

	report.record_throughput(&format!("{label} section=compile avg_ns={compile_avg}"), ops, sample.elapsed);
	report.record_throughput(&format!("{label} section=execute avg_ns={execute_avg}"), ops, sample.elapsed);
	report.record_throughput(&format!("{label} section=overhead avg_ns={overhead_avg}"), ops, sample.elapsed);
}

fn mint_tokens(db: &Database, identities: &[Identity], serve: bool) -> Vec<(Identity, String)> {
	if !serve {
		return Vec::new();
	}

	identities
		.iter()
		.filter(|identity| identity.is_privileged())
		.filter_map(|identity| {
			let token = format!("bench-{}", identity.label());
			match db.auth_service().create_token(&token, identity.id(), None) {
				Ok(_) => Some((*identity, token)),
				Err(e) => {
					println!("could not mint a {} token: {}", identity, e);
					None
				}
			}
		})
		.collect()
}

fn print_repro(scenario: &Scenario, query: &str, transport: Transport, identity: Identity, scale: u64, threads: usize) {
	let scale_env = if scenario.dataset.is_manual() {
		String::new()
	} else {
		format!("SCALES={} ", scale)
	};

	println!(
		"repro= make bench-query SCENARIO={} QUERY={} TRANSPORTS={} IDENTITIES={} {}THREADS={}",
		scenario.name, query, transport, identity, scale_env, threads
	);
}

fn main() {
	let _ = default_provider().install_default();

	let scales = env_list_u64("SCALES", &DEFAULT_SCALES);
	let threads = env_list_usize("THREADS", &DEFAULT_THREADS);
	let iterations = env_u64("ITERATIONS", DEFAULT_ITERATIONS);
	let warmup = env_u64("WARMUP", DEFAULT_WARMUP);
	let budget = Duration::from_secs(env_u64("BUDGET_SECONDS", DEFAULT_BUDGET_SECONDS));
	let repeats = env_u64("REPEATS", DEFAULT_REPEATS);
	let only = env_opt("SCENARIO");
	let only_query = env_opt("QUERY");
	let transports = transports();
	let identities = identities();
	let format = wire_format();

	assert!(!transports.is_empty(), "TRANSPORTS matched no known transport");
	assert!(!identities.is_empty(), "IDENTITIES matched no known identity");
	assert!(repeats > 0, "REPEATS must be at least one");

	let serve = transports.iter().any(|transport| transport.is_wire());

	let cells =
		registry::all()
			.iter()
			.filter(|scenario| only.as_deref().is_none_or(|only| scenario.name == only))
			.map(|scenario| {
				if scenario.dataset.is_manual() {
					1
				} else {
					scales.len()
				}
			})
			.sum::<usize>() * transports.len()
			* identities.len() * threads.len();
	println!(
		"matrix cells={} transports={} identities={} threads={} repeats={} worst_case_minutes={}",
		cells,
		transports.len(),
		identities.len(),
		threads.len(),
		repeats,
		cells as u64 * repeats * budget.as_secs() / 60
	);

	let mut report = BenchReport::new("query");

	for scenario in registry::all() {
		if only.as_deref().is_some_and(|only| scenario.name != only) {
			continue;
		}

		let scenario_scales: Vec<u64> = if scenario.dataset.is_manual() {
			vec![0]
		} else {
			scales.clone()
		};

		for scale in scenario_scales {
			let db = build_database(serve);
			let seed_elapsed = seed_database(&db, &scenario, scale);
			println!(
				"scenario={} scale={} seeded_rows={} seed_ms={}",
				scenario.name,
				scale,
				scenario.dataset.row_count(scale),
				seed_elapsed.as_millis()
			);

			let engine = db.engine();
			let tokens = mint_tokens(&db, &identities, serve);

			for query in &scenario.queries {
				if only_query.as_deref().is_some_and(|only| query.name != only) {
					continue;
				}

				for transport in &transports {
					for identity in &identities {
						let token = tokens
							.iter()
							.find(|(candidate, _)| candidate == identity)
							.map(|(_, token)| token.as_str());

						if transport.is_wire() && identity.is_privileged() && token.is_none() {
							println!(
								"skipped scenario={} query={} transport={} identity={}: no token",
								scenario.name, query.name, transport, identity
							);
							continue;
						}

						for count in &threads {
							if warmup > 0 {
								run_once(
									engine, *transport, format, *identity, token,
									query, scale, *count, warmup, budget, SEED,
								);
							}

							let mut samples: Vec<Sample> = Vec::new();
							for repeat in 0..repeats {
								let sample = run_once(
									engine,
									*transport,
									format,
									*identity,
									token,
									query,
									scale,
									*count,
									iterations,
									budget,
									SEED.wrapping_add(repeat),
								);
								let overran = sample.elapsed > budget * OVERRUN_FACTOR;
								samples.push(sample);

								if overran {
									println!(
										"truncated scenario={} query={} scale={} threads={} after {} of {} samples: a single request outran the {}s budget",
										scenario.name,
										query.name,
										scale,
										count,
										samples.len(),
										repeats,
										budget.as_secs()
									);
									break;
								}
							}

							let median = median_by_throughput(&samples, |sample| {
								(sample.ops, sample.elapsed)
							});
							let label = format!(
								"scenario={} query={} transport={} identity={} scale={} threads={}",
								scenario.name,
								query.name,
								transport,
								identity,
								scale,
								count
							);
							record(&mut report, &label, median);
							print_repro(
								&scenario, query.name, *transport, *identity, scale,
								*count,
							);
						}
					}
				}
			}
		}
	}

	report.save();
}
