// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[allow(dead_code)]
mod transport;

use std::{
	collections::{HashMap, HashSet},
	fmt::{Display, Formatter, Result as FmtResult},
	sync::{
		Arc, Barrier,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
	thread,
	time::{Duration, Instant},
};

use hdrhistogram::Histogram;
use reifydb::{Database, embedded, engine::engine::StandardEngine, server};
use reifydb_allocator::set_global_allocator;
use reifydb_benches::{BenchReport, env_list_usize, env_opt, env_u64, latency_histogram, median_by_throughput, merge};
use reifydb_client::WireFormat;
use reifydb_testing_scenario::query::OperationKind;
use reifydb_value::value::{Value, frame::frame::Frame};
use rustls::crypto::ring::default_provider;

use crate::transport::{
	ALL_TRANSPORTS, DEFAULT_GRPC_PORT, DEFAULT_HTTP_PORT, DEFAULT_WS_PORT, Driver, Identity, Transport,
};

set_global_allocator!();

const DEFAULT_WORKERS: [usize; 4] = [1, 4, 16, 32];
const DEFAULT_PARTITIONS: [usize; 3] = [1, 16, 128];
const DEFAULT_BATCH: u64 = 10;
const DEFAULT_DEPTH: u64 = 10_000;
const DEFAULT_RATE: u64 = 20_000;
const DEFAULT_SECONDS: u64 = 3;
const DEFAULT_KEYS: u64 = 0;
const DEFAULT_LEASE_SECONDS: u64 = 30;
const DEFAULT_REPEATS: u64 = 5;
const DEFAULT_BUDGET_SECONDS: u64 = 30;
const INSERT_BATCH: u64 = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
	Drain,
	Steady,
}

const ALL_MODES: [Mode; 2] = [Mode::Drain, Mode::Steady];

impl Mode {
	fn label(self) -> &'static str {
		match self {
			Mode::Drain => "drain",
			Mode::Steady => "steady",
		}
	}

	fn parse(raw: &str) -> Option<Self> {
		ALL_MODES.into_iter().find(|mode| mode.label() == raw.trim())
	}
}

impl Display for Mode {
	fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
		f.write_str(self.label())
	}
}

#[derive(Debug, Clone, Copy)]
struct Cell {
	mode: Mode,
	transport: Transport,
	workers: usize,
	partitions: usize,
}

struct Knobs {
	batch: u64,
	depth: u64,
	rate: u64,
	seconds: u64,
	keys: u64,
	lease_seconds: u64,
	budget: Duration,
	format: WireFormat,
}

struct Claimed {
	seq: u64,
	key: String,
	at: Duration,
}

struct ClaimedRow {
	token: String,
	seq: u64,
	key: String,
}

struct Outcome {
	claim_latency: Histogram<u64>,
	delay: Histogram<u64>,
	e2e: Histogram<u64>,
	claimed: Vec<Claimed>,
	acked: u64,
	claim_calls: u64,
	empty_claims: u64,
}

impl Outcome {
	fn new() -> Self {
		Self {
			claim_latency: latency_histogram(),
			delay: latency_histogram(),
			e2e: latency_histogram(),
			claimed: Vec::new(),
			acked: 0,
			claim_calls: 0,
			empty_claims: 0,
		}
	}
}

struct Sample {
	acked: u64,
	claim_calls: u64,
	empty_claims: u64,
	enqueued: u64,
	elapsed: Duration,
	claim_latency: Histogram<u64>,
	delay: Histogram<u64>,
	e2e: Histogram<u64>,
}

struct Shared<'a> {
	queue: &'a str,
	knobs: &'a Knobs,
	origin: Instant,
	enqueued_at: &'a [AtomicU64],
	producing: &'a AtomicBool,
}

fn transports() -> Vec<Transport> {
	match env_opt("TRANSPORTS") {
		Some(raw) => raw.split(',').filter_map(Transport::parse).collect(),
		None => ALL_TRANSPORTS.to_vec(),
	}
}

fn modes() -> Vec<Mode> {
	match env_opt("MODES") {
		Some(raw) => raw.split(',').filter_map(Mode::parse).collect(),
		None => ALL_MODES.to_vec(),
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

fn mint_token(db: &Database, serve: bool) -> Option<String> {
	if !serve {
		return None;
	}

	let token = "bench-queue-root".to_string();
	db.auth_service()
		.create_token(&token, Identity::Root.id(), None)
		.unwrap_or_else(|e| panic!("root token mints: {}", e));
	Some(token)
}

fn create_queue_rql(queue: &str, partitions: usize, keys: u64) -> String {
	if keys == 0 {
		format!("CREATE QUEUE {queue} {{ seq: int8 }} WITH {{ fifo: {{ partitions: {partitions} }} }}")
	} else {
		format!(
			"CREATE QUEUE {queue} {{ seq: int8, tenant: utf8 }} WITH {{ fifo: {{ partitions: {partitions}, ordered_by: tenant }} }}"
		)
	}
}

fn insert_rql(queue: &str, from: u64, count: u64, keys: u64) -> String {
	let rows: Vec<String> = (from..from + count)
		.map(|seq| {
			if keys == 0 {
				format!("{{ seq: {seq} }}")
			} else {
				format!("{{ seq: {seq}, tenant: \"k{}\" }}", seq % keys)
			}
		})
		.collect();
	format!("INSERT {queue} [{}]", rows.join(", "))
}

fn parse_claim(frames: &[Frame], keyed: bool) -> Vec<ClaimedRow> {
	let mut rows = Vec::new();

	for frame in frames {
		for row in frame.to_rows() {
			let mut token = None;
			let mut seq = None;
			let mut key = None;

			for (name, value) in row {
				match (name.as_str(), value) {
					("token", Value::Utf8(value)) => token = Some(value),
					("seq", Value::Int8(value)) => seq = Some(value as u64),
					("tenant", Value::Utf8(value)) => key = Some(value),
					_ => {}
				}
			}

			let token = token.expect("claim returns a token column");
			let seq = seq.expect("claim returns the seq payload column with a readable value");
			let key = match keyed {
				true => key.expect("claim returns the tenant payload column with a readable value"),
				false => String::new(),
			};

			rows.push(ClaimedRow {
				token,
				seq,
				key,
			});
		}
	}

	rows
}

fn work(driver: &Driver, name: &str, shared: &Shared<'_>, budget: Duration) -> Outcome {
	let mut outcome = Outcome::new();
	let keyed = shared.knobs.keys > 0;
	let claim = format!(
		r#"CALL queue::claim("{}", "{}", {}, duration::seconds({}))"#,
		name, shared.queue, shared.knobs.batch, shared.knobs.lease_seconds
	);
	let deadline = Instant::now() + budget;

	loop {
		if Instant::now() >= deadline {
			break;
		}

		let stopped = !shared.producing.load(Ordering::Acquire);

		let call = Instant::now();
		let frames = driver.command_frames(&claim);
		let latency = call.elapsed();
		outcome.claim_calls += 1;

		let rows = parse_claim(&frames, keyed);
		if rows.is_empty() {
			outcome.empty_claims += 1;
			if stopped {
				break;
			}
			thread::yield_now();
			continue;
		}

		outcome.claim_latency.record(latency.as_nanos().max(1) as u64).ok();

		let claimed_at = shared.origin.elapsed();
		for row in &rows {
			let enqueued = shared.enqueued_at[row.seq as usize].load(Ordering::Acquire);
			if enqueued > 0 {
				let delay = (claimed_at.as_nanos() as u64).saturating_sub(enqueued);
				outcome.delay.record(delay.max(1)).ok();
			}
			outcome.claimed.push(Claimed {
				seq: row.seq,
				key: row.key.clone(),
				at: claimed_at,
			});
		}

		for row in &rows {
			driver.command_frames(&format!(r#"CALL queue::ack("{}", "ok", none)"#, row.token));
			outcome.acked += 1;

			let enqueued = shared.enqueued_at[row.seq as usize].load(Ordering::Acquire);
			if enqueued > 0 {
				let e2e = (shared.origin.elapsed().as_nanos() as u64).saturating_sub(enqueued);
				outcome.e2e.record(e2e.max(1)).ok();
			}
		}
	}

	outcome
}

fn produce(driver: &Driver, shared: &Shared<'_>, produced: &AtomicU64) {
	let capacity = shared.enqueued_at.len() as u64;
	let paced_from = Instant::now();
	let stop = paced_from + Duration::from_secs(shared.knobs.seconds);
	let mut seq = 0u64;

	while seq < capacity {
		let now = Instant::now();
		if now >= stop {
			break;
		}

		let target = paced_from + Duration::from_nanos(seq.saturating_mul(1_000_000_000) / shared.knobs.rate);
		if target > now {
			thread::sleep(target - now);
		}

		let count = INSERT_BATCH.min(capacity - seq);
		let stamp = (shared.origin.elapsed().as_nanos() as u64).max(1);
		for offset in 0..count {
			shared.enqueued_at[(seq + offset) as usize].store(stamp, Ordering::Release);
		}

		driver.command_frames(&insert_rql(shared.queue, seq, count, shared.knobs.keys));
		seq += count;
	}

	produced.store(seq, Ordering::Release);
	shared.producing.store(false, Ordering::Release);
}

fn verify(cell: Cell, knobs: &Knobs, outcomes: &[Outcome], acked: u64, enqueued: u64) {
	let cell_label = format!(
		"mode={} transport={} workers={} partitions={} keys={}",
		cell.mode, cell.transport, cell.workers, cell.partitions, knobs.keys
	);

	let mut seen = HashSet::new();
	let mut duplicates = Vec::new();
	for entry in outcomes.iter().flat_map(|outcome| outcome.claimed.iter()) {
		if !seen.insert(entry.seq) {
			duplicates.push(entry.seq);
		}
	}

	assert!(
		duplicates.is_empty(),
		"{cell_label}: {} items were handed to a second worker while a lease was live, first: {:?}",
		duplicates.len(),
		&duplicates[..duplicates.len().min(8)]
	);

	assert_eq!(
		acked,
		enqueued,
		"{cell_label}: acked {acked} of {enqueued} enqueued items, so items were lost or the {}s budget expired before the queue drained",
		knobs.budget.as_secs()
	);

	assert_eq!(
		seen.len() as u64,
		enqueued,
		"{cell_label}: claimed {} distinct items but {enqueued} were enqueued",
		seen.len()
	);

	if knobs.keys == 0 {
		return;
	}

	let mut by_key: HashMap<&str, Vec<&Claimed>> = HashMap::new();
	for entry in outcomes.iter().flat_map(|outcome| outcome.claimed.iter()) {
		by_key.entry(entry.key.as_str()).or_default().push(entry);
	}

	for (key, mut group) in by_key {
		group.sort_by_key(|entry| entry.at);
		for pair in group.windows(2) {
			assert!(
				pair[1].seq > pair[0].seq,
				"{cell_label}: key {key} was claimed out of order, item {} came after item {}",
				pair[1].seq,
				pair[0].seq
			);
		}
	}
}

fn run_once(engine: &StandardEngine, cell: Cell, knobs: &Knobs, token: Option<&str>, index: u64) -> Sample {
	let queue = format!("bench::jobs{index}");
	let setup = Driver::connect(Transport::Embedded, engine, knobs.format, Identity::Root, None);
	let _ = setup.execute(OperationKind::Admin, &create_queue_rql(&queue, cell.partitions, knobs.keys));

	let capacity = match cell.mode {
		Mode::Drain => knobs.depth,
		Mode::Steady => knobs.rate * knobs.seconds + INSERT_BATCH,
	};
	let enqueued_at: Vec<AtomicU64> = (0..capacity).map(|_| AtomicU64::new(0)).collect();

	if cell.mode == Mode::Drain {
		let mut seq = 0u64;
		while seq < knobs.depth {
			let count = INSERT_BATCH.min(knobs.depth - seq);
			setup.execute(OperationKind::Command, &insert_rql(&queue, seq, count, knobs.keys));
			seq += count;
		}
	}

	let steady = cell.mode == Mode::Steady;
	let producing = AtomicBool::new(steady);
	let produced = AtomicU64::new(knobs.depth);
	let budget = match cell.mode {
		Mode::Drain => knobs.budget,
		Mode::Steady => Duration::from_secs(knobs.seconds) + knobs.budget,
	};

	let shared = Shared {
		queue: &queue,
		knobs,
		origin: Instant::now(),
		enqueued_at: &enqueued_at,
		producing: &producing,
	};

	let ready = Arc::new(Barrier::new(cell.workers + usize::from(steady) + 1));
	let mut started = Instant::now();

	let outcomes: Vec<Outcome> = thread::scope(|scope| {
		let shared = &shared;
		let produced = &produced;

		if steady {
			let ready = Arc::clone(&ready);
			scope.spawn(move || {
				let driver =
					Driver::connect(cell.transport, engine, knobs.format, Identity::Root, token);
				ready.wait();
				produce(&driver, shared, produced);
			});
		}

		let handles: Vec<_> = (0..cell.workers)
			.map(|worker| {
				let ready = Arc::clone(&ready);
				scope.spawn(move || {
					let driver = Driver::connect(
						cell.transport,
						engine,
						knobs.format,
						Identity::Root,
						token,
					);
					let name = format!("w{worker}");
					ready.wait();
					work(&driver, &name, shared, budget)
				})
			})
			.collect();

		ready.wait();
		started = Instant::now();

		handles.into_iter().map(|handle| handle.join().expect("worker thread does not panic")).collect()
	});

	let elapsed = started.elapsed();
	let enqueued = produced.load(Ordering::Acquire);
	let acked = outcomes.iter().map(|outcome| outcome.acked).sum();

	verify(cell, knobs, &outcomes, acked, enqueued);
	let _ = setup.execute(OperationKind::Admin, &format!("DROP QUEUE {queue}"));

	let target = knobs.rate * knobs.seconds;
	if steady && enqueued * 10 < target * 9 {
		println!(
			"warning: producer enqueued {enqueued} of {target} items, so this cell measures the producer rather than the queue"
		);
	}

	Sample {
		acked,
		claim_calls: outcomes.iter().map(|outcome| outcome.claim_calls).sum(),
		empty_claims: outcomes.iter().map(|outcome| outcome.empty_claims).sum(),
		enqueued,
		elapsed,
		claim_latency: merge(outcomes.iter().map(|outcome| outcome.claim_latency.clone())),
		delay: merge(outcomes.iter().map(|outcome| outcome.delay.clone())),
		e2e: merge(outcomes.iter().map(|outcome| outcome.e2e.clone())),
	}
}

fn record(report: &mut BenchReport, label: &str, sample: &Sample, mode: Mode) {
	let jobs = match mode {
		Mode::Steady => &sample.e2e,
		Mode::Drain => &sample.claim_latency,
	};
	report.record(&format!("{label} section=jobs"), sample.acked, sample.elapsed, jobs);
	report.record(
		&format!("{label} section=claim empty={} enqueued={}", sample.empty_claims, sample.enqueued),
		sample.claim_calls,
		sample.elapsed,
		&sample.claim_latency,
	);

	if mode == Mode::Steady {
		report.record(&format!("{label} section=delay"), sample.acked, sample.elapsed, &sample.delay);
	}
}

fn print_repro(cell: Cell, knobs: &Knobs) {
	println!(
		"repro= make bench-queue MODES={} TRANSPORTS={} WORKERS={} PARTITIONS={} KEYS={}",
		cell.mode, cell.transport, cell.workers, cell.partitions, knobs.keys
	);
}

fn main() {
	let _ = default_provider().install_default();

	let transports = transports();
	let modes = modes();
	let workers = env_list_usize("WORKERS", &DEFAULT_WORKERS);
	let partitions = env_list_usize("PARTITIONS", &DEFAULT_PARTITIONS);
	let repeats = env_u64("REPEATS", DEFAULT_REPEATS);

	let knobs = Knobs {
		batch: env_u64("BATCH", DEFAULT_BATCH),
		depth: env_u64("DEPTH", DEFAULT_DEPTH),
		rate: env_u64("RATE", DEFAULT_RATE),
		seconds: env_u64("SECONDS", DEFAULT_SECONDS),
		keys: env_u64("KEYS", DEFAULT_KEYS),
		lease_seconds: env_u64("LEASE_SECONDS", DEFAULT_LEASE_SECONDS),
		budget: Duration::from_secs(env_u64("BUDGET_SECONDS", DEFAULT_BUDGET_SECONDS)),
		format: wire_format(),
	};

	assert!(!transports.is_empty(), "TRANSPORTS matched no known transport");
	assert!(!modes.is_empty(), "MODES matched no known mode");
	assert!(!workers.is_empty(), "WORKERS matched no worker count");
	assert!(!partitions.is_empty(), "PARTITIONS matched no partition count");
	assert!(repeats > 0, "REPEATS must be at least one");
	assert!(knobs.batch > 0, "BATCH must be at least one");
	assert!(knobs.depth > 0, "DEPTH must be at least one");
	assert!(knobs.rate > 0, "RATE must be at least one");
	assert!(knobs.seconds > 0, "SECONDS must be at least one");
	assert!(knobs.lease_seconds > 0, "LEASE_SECONDS must be at least one");

	let serve = transports.iter().any(|transport| transport.is_wire());
	let db = build_database(serve);
	db.admin_as_root("CREATE NAMESPACE bench", ()).expect("bench namespace is creatable");
	let token = mint_token(&db, serve);
	let engine = db.engine();

	let cells = modes.len() * transports.len() * workers.len() * partitions.len();
	println!(
		"matrix cells={} modes={} transports={} workers={} partitions={} repeats={} keys={} batch={}",
		cells,
		modes.len(),
		transports.len(),
		workers.len(),
		partitions.len(),
		repeats,
		knobs.keys,
		knobs.batch
	);

	let mut report = BenchReport::new("queue");
	let mut index = 0u64;

	for mode in &modes {
		for transport in &transports {
			for count in &workers {
				for partition in &partitions {
					let cell = Cell {
						mode: *mode,
						transport: *transport,
						workers: *count,
						partitions: *partition,
					};

					let mut samples = Vec::new();
					for _ in 0..repeats {
						index += 1;
						samples.push(run_once(engine, cell, &knobs, token.as_deref(), index));
					}

					let median =
						median_by_throughput(&samples, |sample| (sample.acked, sample.elapsed));
					let label = format!(
						"mode={} transport={} workers={} partitions={} keys={} batch={}",
						cell.mode,
						cell.transport,
						cell.workers,
						cell.partitions,
						knobs.keys,
						knobs.batch
					);
					record(&mut report, &label, median, *mode);
					print_repro(cell, &knobs);
				}
			}
		}
	}

	report.save();
}
