// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Session windows, the one `WindowKind` no sweep reached.
//!
//! Unlike the grid kinds, a session's boundaries depend on arrival history rather than on the
//! coordinate alone, so the same row lands in a different aggregate depending on what came before it
//! in its group. That history lives in a per-group tracker read from an in-batch map with a
//! transaction fallback and written back once at the end - the shape that produced both ring buffer
//! defects, and the reason batches are drawn wide here.
//!
//! Sealing is not driven: it is the same machinery the grid kinds already sweep, and modelling it
//! would restate that instead of the assignment this suite exists for.

pub mod oracle;

use rand::RngExt;
use reifydb_core::common::WindowKind;
use reifydb_testing_chaos::{
	corpus::Corpus,
	fuzz::{run_reported, split},
	operator::{
		drive as driver,
		scenario::{BatchSize, Scenario},
	},
};
use reifydb_value::value::duration::Duration;

use crate::{
	framework::{harness::Harness, workload::WindowWorkload},
	operators::window::{WindowSpec, build, grid::Fold, session::oracle::SessionOracle},
};

#[derive(Debug, Clone)]
pub struct Params {
	pub gap_ms: u64,
	pub fold: Fold,
	pub groups: i32,
	pub steps: u32,
	pub max_batch: u32,

	/// How far apart coordinates are drawn. Read against `gap_ms`: a span far wider than the gap
	/// rotates constantly, a span narrower than it keeps one session open and exercises extension
	/// and the refusal boundary instead.
	pub coord_span_ms: u64,
	pub remove_pct: u32,
	pub update_pct: u32,
}

pub fn drive(seed: u64, params: Params) -> Corpus {
	let spec = WindowSpec {
		kind: WindowKind::Session {
			gap: Duration::from_milliseconds(params.gap_ms as i64).expect("a drawn gap is representable"),
		},
		group_by: "g",
		aggregations: params.fold.rql(),
		lateness: None,
	};

	let mut harness = Harness::new(|runtime| build(&spec, runtime));
	let workload = WindowWorkload {
		groups: params.groups,
		coord_span_ms: params.coord_span_ms,
	};
	let mut model = SessionOracle::new(params.gap_ms, params.fold);

	driver::drive(
		seed,
		Scenario::mixed(params.steps)
			.with_batch(BatchSize::Geometric {
				p: 0.45,
				max: params.max_batch,
			})
			.with_mix(params.remove_pct, params.update_pct, 0)
			.with_coord_span(params.coord_span_ms),
		&mut harness,
		&workload,
		&mut model,
	)
	.assert_clean()
	.corpus
}

pub fn params(gap_ms: u64, coord_span_ms: u64, fold: Fold) -> Params {
	Params {
		gap_ms,
		fold,
		groups: 3,
		steps: 60,
		max_batch: 6,
		coord_span_ms,
		remove_pct: 20,
		update_pct: 30,
	}
}

pub fn random_params(seed: u64) -> (u64, Params) {
	let (mut rng, sequence_seed) = split(seed);
	let max_batch = rng.random_range(1..=8u32);
	let gap_ms = rng.random_range(0..=5_000u64);
	let params = Params {
		gap_ms,
		fold: match rng.random_range(0..3u32) {
			0 => Fold::Sum,
			1 => Fold::Min,
			_ => Fold::Max,
		},
		groups: rng.random_range(1..=5i32),
		max_batch,
		steps: rng.random_range(30..=90u32).min((320 / max_batch).max(30)),
		// Deliberately spans both sides of the gap so rotation and extension are both reached.
		coord_span_ms: rng.random_range(1_000..=20_000u64),
		remove_pct: rng.random_range(5..=35u32),
		update_pct: rng.random_range(20..=50u32),
	};
	(sequence_seed, params)
}

pub fn drive_random(seed: u64) {
	let (sequence_seed, params) = random_params(seed);
	let run = params.clone();
	run_reported("window_session_random_chaos", sequence_seed, &params, || {
		drive(sequence_seed, run);
	});
}
