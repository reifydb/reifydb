// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

mod db;
mod measure;
mod workload;

use std::{
	fs,
	path::{Path, PathBuf},
};

use clap::{Parser, ValueEnum};
use reifydb::Clock;

use crate::{
	measure::Stats,
	workload::{Kind, Workload},
};

#[derive(Copy, Clone, Debug, ValueEnum)]
enum WorkloadArg {
	Synthetic,
	Pump,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum KindArg {
	Transactional,
	Deferred,
}

impl From<WorkloadArg> for Workload {
	fn from(w: WorkloadArg) -> Self {
		match w {
			WorkloadArg::Synthetic => Workload::Synthetic,
			WorkloadArg::Pump => Workload::Pump,
		}
	}
}

impl From<KindArg> for Kind {
	fn from(k: KindArg) -> Self {
		match k {
			KindArg::Transactional => Kind::Transactional,
			KindArg::Deferred => Kind::Deferred,
		}
	}
}

#[derive(Parser)]
#[command(about = "Benchmark transactional vs deferred view freshness and commit latency under many views")]
struct Cli {
	#[arg(long, value_enum, default_value = "synthetic")]
	workload: WorkloadArg,

	#[arg(long, value_enum, value_delimiter = ',', default_value = "transactional,deferred")]
	kind: Vec<KindArg>,

	#[arg(long, value_delimiter = ',', default_value = "1,10,50,100")]
	views: Vec<usize>,

	#[arg(long, default_value_t = 2000)]
	iterations: u64,

	#[arg(long, default_value_t = 500)]
	warmup: u64,

	#[arg(long, default_value_t = 1000)]
	flow_tick_ms: u64,

	#[arg(long, default_value_t = 2)]
	threads: u16,

	#[arg(long, env = "OPERATORS_DIR")]
	operators_dir: Option<PathBuf>,

	#[arg(long)]
	csv: Option<PathBuf>,
}

struct Row {
	workload: Workload,
	kind: Kind,
	n: usize,
	commit: Stats,
	fresh: Stats,
	timeouts: u64,
}

fn main() {
	let cli = Cli::parse();
	let workload: Workload = cli.workload.into();
	let operators_dir = match workload {
		Workload::Pump => {
			Some(cli.operators_dir.clone().unwrap_or_else(|| PathBuf::from("../chaindex/target/release")))
		}
		Workload::Synthetic => None,
	};
	let kinds: Vec<Kind> = cli.kind.iter().copied().map(Into::into).collect();
	let config = db::Config {
		flow_tick_ms: cli.flow_tick_ms,
		threads: cli.threads,
	};
	eprintln!(
		"config: flow_tick_ms={} threads={} iterations={} warmup={}",
		cli.flow_tick_ms, cli.threads, cli.iterations, cli.warmup
	);

	let clock = Clock::Real;
	let mut rows = Vec::new();
	for &n in &cli.views {
		for &kind in &kinds {
			eprintln!("running workload={:?} kind={} N={} ...", workload, kind.label(), n);
			let started = clock.instant();
			let mut db = db::build(workload, kind, n, operators_dir.clone(), &config);
			let outcome = measure::pass_a(&db, workload, cli.iterations, cli.warmup);
			db.stop().expect("db stop failed");
			eprintln!(
				"  done in {:.1}s ({} freshness timeouts)",
				started.elapsed().as_secs_f64(),
				outcome.timeouts
			);
			rows.push(Row {
				workload,
				kind,
				n,
				commit: outcome.commit,
				fresh: outcome.fresh,
				timeouts: outcome.timeouts,
			});
		}
	}

	print_table(&rows);
	if let Some(path) = cli.csv.as_deref() {
		write_csv(&rows, &config, path);
		eprintln!("wrote csv to {}", path.display());
	}
}

fn print_table(rows: &[Row]) {
	println!(
		"\n{:<12} {:>5} | {:>10} {:>10} {:>10} | {:>10} {:>10} {:>10} | {:>8}",
		"kind", "N", "commit_p50", "commit_p99", "cmt_p999", "fresh_p50", "fresh_p99", "frsh_p999", "timeouts"
	);
	println!("{}", "-".repeat(100));
	for r in rows {
		println!(
			"{:<12} {:>5} | {:>10} {:>10} {:>10} | {:>10} {:>10} {:>10} | {:>8}",
			r.kind.label(),
			r.n,
			r.commit.p50,
			r.commit.p99,
			r.commit.p999,
			r.fresh.p50,
			r.fresh.p99,
			r.fresh.p999,
			r.timeouts
		);
	}
	println!("\nlatencies in microseconds; commit = insert command return, fresh = write to probe-visible");
}

fn write_csv(rows: &[Row], config: &db::Config, path: &Path) {
	let mut out = String::from(
		"workload,kind,n,flow_tick_ms,threads,commit_p50_us,commit_p99_us,commit_p999_us,commit_max_us,fresh_p50_us,fresh_p99_us,fresh_p999_us,fresh_max_us,timeouts\n",
	);
	for r in rows {
		let workload = match r.workload {
			Workload::Synthetic => "synthetic",
			Workload::Pump => "pump",
		};
		out.push_str(&format!(
			"{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
			workload,
			r.kind.label(),
			r.n,
			config.flow_tick_ms,
			config.threads,
			r.commit.p50,
			r.commit.p99,
			r.commit.p999,
			r.commit.max,
			r.fresh.p50,
			r.fresh.p99,
			r.fresh.p999,
			r.fresh.max,
			r.timeouts
		));
	}
	fs::write(path, out).expect("failed to write csv");
}
