// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

mod data;
mod equity;
mod phase1;
mod phase2;
mod report;
mod solana;
mod solana1s;
mod sweep;
mod transform;

use std::{env::args, process::exit};

use reifydb::{allocator, column::compress::CompressionProfile};

use crate::{
	data::{Dataset, Variant},
	equity::EquityConfig,
	phase1::{equity_layouts, solana_layouts},
	phase2::Run,
	report::Row,
	solana::SolanaConfig,
	solana1s::SecondConfig,
};

allocator::set_global_allocator!();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
	One,
	Two,
	All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Selection {
	Equity,
	Solana,
	Solana1s,
	All,
}

struct Args {
	phase: Phase,
	dataset: Selection,
}

const USAGE: &str = "usage: ohlcv_bench <phase1|phase2|all> [--dataset equity|solana|solana1s|all]";

fn parse(mut input: impl Iterator<Item = String>) -> Result<Args, String> {
	let phase = match input.next().as_deref() {
		Some("phase1") => Phase::One,
		Some("phase2") => Phase::Two,
		Some("all") => Phase::All,
		Some(other) => return Err(format!("unknown phase `{other}`")),
		None => return Err("missing phase".to_string()),
	};
	let mut dataset = Selection::All;
	while let Some(flag) = input.next() {
		match flag.as_str() {
			"--dataset" => {
				dataset = match input.next().as_deref() {
					Some("equity") => Selection::Equity,
					Some("solana") => Selection::Solana,
					Some("solana1s") => Selection::Solana1s,
					Some("all") => Selection::All,
					Some(other) => return Err(format!("unknown dataset `{other}`")),
					None => return Err("missing value for --dataset".to_string()),
				}
			}
			other => return Err(format!("unknown argument `{other}`")),
		}
	}
	Ok(Args {
		phase,
		dataset,
	})
}

pub fn profile_name(profile: CompressionProfile) -> &'static str {
	match profile {
		CompressionProfile::Default => "default",
		#[cfg(feature = "pco")]
		CompressionProfile::Compact => "compact",
	}
}

fn profiles() -> Vec<CompressionProfile> {
	vec![
		CompressionProfile::Default,
		#[cfg(feature = "pco")]
		CompressionProfile::Compact,
	]
}

const EQUITY_ONE_BUCKET: u64 = 885_000_000_000_000_000;
const EQUITY_DAILY: u64 = 86_400_000_000_000;
const SOLANA_DAILY: u64 = 216_000;
const SOLANA_TICK_MS: u64 = 300_000;
const TXN_ROWS: usize = 450_000;

const EQ_RAW: &str = "CREATE SERIES bench::eq_raw { ts: int8, open: int8, high: int8, low: int8, close: int8, volume: uint8 } WITH { key: ts }";
const EQ_DT: &str = "CREATE SERIES bench::eq_dt { ts: datetime, open: int8, high: int8, low: int8, close: int8, volume: uint8 } WITH { key: ts, precision: nanosecond }";
const EQ_DERIVED: &str = "CREATE SERIES bench::eq_derived { ts: int8, close: int8, gap: int8, upper: uint8, lower: uint8, volume: uint8 } WITH { key: ts }";
const SOL_PART: &str = "CREATE SERIES bench::sol_part { slot: uint8, pool: uint4, open: int8, high: int8, low: int8, close: int8, volume: uint8 } WITH { key: slot, partition: { by: { pool } } }";
const SOL_FLAT: &str = "CREATE SERIES bench::sol_flat { slot: uint8, pool: uint4, open: int8, high: int8, low: int8, close: int8, volume: uint8 } WITH { key: slot }";

fn equity_runs() -> Vec<Run> {
	let run = |name, series, ddl, variant, width, layout| Run {
		name,
		series,
		ddl,
		variant,
		with_pool: false,
		width,
		layout,
		tick_ms: 250,
		txn_rows: TXN_ROWS,
	};
	vec![
		run("E1", "bench::eq_raw", EQ_RAW, Variant::Raw, EQUITY_ONE_BUCKET, "1 bucket"),
		run("E2", "bench::eq_dt", EQ_DT, Variant::Datetime, EQUITY_ONE_BUCKET, "1 bucket"),
		run("E3", "bench::eq_derived", EQ_DERIVED, Variant::Derived, EQUITY_ONE_BUCKET, "1 bucket"),
		run("E1-daily", "bench::eq_raw", EQ_RAW, Variant::Raw, EQUITY_DAILY, "daily buckets"),
	]
}

fn solana_runs() -> Vec<Run> {
	let run = |name, series, ddl, layout| Run {
		name,
		series,
		ddl,
		variant: Variant::Raw,
		with_pool: true,
		width: SOLANA_DAILY,
		layout,
		tick_ms: SOLANA_TICK_MS,
		txn_rows: TXN_ROWS,
	};
	vec![
		run("S1", "bench::sol_part", SOL_PART, "daily, by pool"),
		run("S2", "bench::sol_flat", SOL_FLAT, "daily, flat"),
	]
}

fn push(rows: &mut Vec<Row>, label: &str, result: Result<Row, String>) {
	match result {
		Ok(row) => rows.push(row),
		Err(e) => eprintln!("{label} failed: {e}"),
	}
}

fn phase1_equity(ds: &Dataset, rows: &mut Vec<Row>) {
	let (one, daily) = equity_layouts(ds);
	for (layout, variant) in [
		(&one, Variant::Raw),
		(&one, Variant::Derived),
		(&one, Variant::Datetime),
		(&one, Variant::Decimal),
		(&daily, Variant::Raw),
	] {
		for profile in profiles() {
			push(rows, "phase1 equity", phase1::measure(ds, layout, variant, profile));
		}
	}
}

fn phase1_solana(ds: &Dataset, cfg: &SolanaConfig, rows: &mut Vec<Row>) {
	for layout in solana_layouts(ds, cfg) {
		for variant in [Variant::Raw, Variant::Derived] {
			for profile in profiles() {
				push(rows, "phase1 solana", phase1::measure(ds, &layout, variant, profile));
			}
		}
	}
}

fn phase2(ds: &Dataset, runs: Vec<Run>, rows: &mut Vec<Row>) {
	for run in runs {
		for profile in profiles() {
			push(rows, run.name, phase2::measure(ds, &run, profile));
		}
	}
}

fn run(args: Args) -> Result<(), String> {
	if args.dataset == Selection::Solana1s {
		if args.phase == Phase::Two {
			return Err("solana1s runs phase1 only".to_string());
		}
		return sweep::run(&SecondConfig::default());
	}
	let one = matches!(args.phase, Phase::One | Phase::All);
	let two = matches!(args.phase, Phase::Two | Phase::All);
	let mut phase1_rows = Vec::new();
	let mut phase2_rows = Vec::new();
	if matches!(args.dataset, Selection::Equity | Selection::All) {
		let ds = data::equity(&EquityConfig::default())?;
		println!("equity: {} bars", ds.rows());
		if one {
			phase1_equity(&ds, &mut phase1_rows);
		}
		if two {
			phase2(&ds, equity_runs(), &mut phase2_rows);
		}
	}
	if matches!(args.dataset, Selection::Solana | Selection::All) {
		let cfg = SolanaConfig::default();
		let pools = solana::generate(&cfg);
		println!("{}", solana::summary(&pools));
		let ds = data::solana(&cfg, pools)?;
		if one {
			phase1_solana(&ds, &cfg, &mut phase1_rows);
		}
		if two {
			phase2(&ds, solana_runs(), &mut phase2_rows);
		}
	}
	if one {
		report::print("phase 1: compressor level (B/row and ratio against 48 B/row raw)", &phase1_rows);
	}
	if two {
		report::print("phase 2: end to end through a SERIES (B/row = user columns incl. header)", &phase2_rows);
	}
	let all: Vec<Row> = phase1_rows.into_iter().chain(phase2_rows).collect();
	let csv = phase2::root().join("results.csv");
	report::write_csv(&csv, &all)?;
	println!("\nwrote {}", csv.display());
	Ok(())
}

fn main() {
	let args = match parse(args().skip(1)) {
		Ok(args) => args,
		Err(err) => {
			eprintln!("{err}\n{USAGE}");
			exit(2);
		}
	};
	if let Err(err) = run(args) {
		eprintln!("ohlcv_bench failed: {err}");
		exit(1);
	}
}
