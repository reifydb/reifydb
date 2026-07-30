// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod oracle;
pub mod workload;

use std::sync::Arc;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_core::common::JoinType;
use reifydb_engine::test_harness::TestEngine;
use reifydb_rql::expression::parse_expression;
use reifydb_sub_flow::{
	context::FlowContext,
	operator::join::operator::{JoinOperator, JoinSideConfig},
};
use reifydb_testing_chaos::{
	corpus::Corpus,
	fuzz::{pick, run_reported, split},
	operator::{
		drive::{self as driver, DriveOutcome},
		model::Model,
		scenario::{BatchSize, Scenario},
		workload::Workload,
	},
};
use reifydb_value::value::row_number::RowNumber;

use crate::{
	framework::harness::Harness,
	operators::join::{
		oracle::{HashOracle, LatestOracle, SnapshotOracle},
		workload::{
			JOIN_NODE, JoinRow, JoinWorkload, LEFT_COLUMNS, LEFT_NODE, RIGHT_COLUMNS, RIGHT_NODE, Side,
			schema,
		},
	},
};

/// One cell of the join matrix: which join type, and the two independent modifiers.
///
/// Three axes rather than one enum of named strategies, because that is how the operator reads them.
/// `JoinStrategy::from(join_type, latest)` picks the strategy and `snapshot` is then checked inside
/// whichever one it picked, so the modifiers multiply rather than enumerate. A flat list of names
/// hides which combinations exist and makes it easy to leave one out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Variant {
	pub outer: bool,
	pub latest: bool,

	/// Right-side arrivals update the operator's state but publish nothing, so only the left side
	/// drives the view. See `drive_static_right` for why that has to be tested against a right side
	/// that stops changing.
	pub snapshot: bool,
}

impl Variant {
	pub const fn inner() -> Self {
		Self {
			outer: false,
			latest: false,
			snapshot: false,
		}
	}

	pub const fn left() -> Self {
		Self {
			outer: true,
			..Self::inner()
		}
	}

	pub const fn with_latest(mut self) -> Self {
		self.latest = true;
		self
	}

	pub const fn with_snapshot(mut self) -> Self {
		self.snapshot = true;
		self
	}

	pub fn label(self) -> String {
		let mut out = String::from(match self.outer {
			true => "left",
			false => "inner",
		});
		if self.latest {
			out.push_str("+latest");
		}
		if self.snapshot {
			out.push_str("+snapshot");
		}
		out
	}

	fn join_type(self) -> JoinType {
		match self.outer {
			true => JoinType::Left,
			false => JoinType::Inner,
		}
	}

	fn left_outer(self) -> bool {
		self.outer
	}
}

/// Every combination, so adding a modifier cannot silently leave cells untested.
pub const MATRIX: [Variant; 8] = [
	Variant::inner(),
	Variant::inner().with_snapshot(),
	Variant::inner().with_latest(),
	Variant::inner().with_latest().with_snapshot(),
	Variant::left(),
	Variant::left().with_snapshot(),
	Variant::left().with_latest(),
	Variant::left().with_latest().with_snapshot(),
];

pub fn build(engine: &TestEngine, variant: Variant) -> JoinOperator {
	JoinOperator::new(
		JoinSideConfig {
			node: LEFT_NODE,
			exprs: parse_expression("k").expect("left key parses"),
			schema: schema(&LEFT_COLUMNS),
		},
		JoinSideConfig {
			node: RIGHT_NODE,
			exprs: parse_expression("k").expect("right key parses"),
			schema: schema(&RIGHT_COLUMNS),
		},
		JOIN_NODE,
		variant.join_type(),
		None,
		engine.executor(),
		variant.snapshot,
		false,
		variant.latest,
		None,
		None,
		Arc::new(FlowContext::default()),
	)
}

#[derive(Debug, Clone)]
pub struct Params {
	pub variant: Variant,

	/// How many distinct join keys the two sides draw from. A narrow domain is what makes a key
	/// carry several rows on each side, which is the only way the cartesian paths are reached.
	pub keys: i32,

	pub right_pct: u32,

	/// Share of rows drawn with an undefined key. Each strategy routes those to its own path: an
	/// inner join drops them, a left join publishes them as permanently unmatched.
	pub none_pct: u32,

	/// Share of updates that also move the row to a different key, which is the branch that splits
	/// an update into a retraction under the old key and an insertion under the new one.
	pub rekey_pct: u32,

	pub steps: u32,
	pub max_batch: u32,
	pub max_live: usize,
	pub remove_pct: u32,
	pub update_pct: u32,

	/// Size of the frozen right side loaded before the run, for `drive_static_right` only. The other
	/// entry points draw their right rows from the corpus and ignore this.
	pub static_right: u32,
}

/// The one configuration every fixed strategy sweep runs, differing only in which strategy it drives.
///
/// The corpus a run executes is drawn from the seed and these parameters, and nothing about the
/// strategy feeds back into the draw. So the four sweeps built from this are not four similar runs -
/// they are the SAME operation sequence put to four strategies, which is what makes a failure in one
/// directly comparable with what the other three did at the same step. Four hand-copied parameter
/// blocks would read the same today and drift apart on the first edit, and the property would be
/// gone with nothing to say so.
pub fn matched_params(variant: Variant) -> Params {
	Params {
		variant,
		keys: 3,
		right_pct: 50,
		none_pct: 10,
		rekey_pct: 30,
		steps: 60,
		max_batch: 5,
		max_live: 40,
		remove_pct: 25,
		update_pct: 30,
		static_right: 12,
	}
}

/// The configuration every matrix cell runs. Left-side traffic only, against the right side
/// `drive_static_right` loads and freezes, so all eight cells execute one shared corpus.
pub fn static_right_params(variant: Variant) -> Params {
	Params {
		variant,
		right_pct: 0,
		static_right: 12,
		..matched_params(variant)
	}
}

fn scenario(params: &Params) -> Scenario {
	// A join has no clock: no timer to fire, no horizon to cross, nothing in flight between steps.
	// `mixed` is the shape that carries no tick share and packs a step's operations into one change,
	// which is what an upstream flow hands it.
	Scenario::mixed(params.steps)
		.with_batch(BatchSize::Geometric {
			p: 0.45,
			max: params.max_batch,
		})
		.with_mix(params.remove_pct, params.update_pct, 0)
		.with_max_live(params.max_live)
}

pub fn drive(seed: u64, params: Params) -> Corpus {
	let oracle = params.variant;
	drive_with(seed, params, false, oracle).assert_clean().corpus
}

/// Drives one variant's operator against a DIFFERENT variant's oracle and hands back the divergence
/// that must follow.
///
/// This tests the suite, not the operator. Every sweep here is green, and green on its own is not
/// evidence: an oracle that described nothing, or a claim that was never compared, would look
/// exactly the same. Checking a corpus against the wrong strategy is the cheapest thing that has to
/// come back red, and it stays red without anyone having to break the tree to find out.
pub fn divergence_checked_as(seed: u64, params: Params, oracle: Variant) -> Option<String> {
	drive_with(seed, params, false, oracle).divergence
}

/// Both sides interleaved freely, handing back the outcome without asserting the oracle.
///
/// For a snapshot cell the oracle cannot judge the contents - suppressed right-side emissions mean
/// the view is behind on purpose - but the outcome still carries the coherence record, which is
/// judgeable under any reading.
pub fn drive_interleaved(seed: u64, params: Params) -> DriveOutcome {
	let oracle = params.variant;
	drive_with(seed, params, false, oracle)
}

/// The same corpus, but updates are allowed to move a key between defined and undefined.
///
/// `apply_join_update` routes an update to its undefined handler when EITHER the old or the new key
/// is undefined, and for the hash strategies that handler emits nothing and leaves the operator's
/// own state untouched. So a key that goes defined -> undefined leaves the joined rows it produced
/// still published, and one that goes undefined -> defined leaves the row unstored while the driver
/// counts it live. This sweep is the only one that generates those two transitions, kept apart from
/// the others so a failure here names that path rather than reading as a general join failure.
pub fn divergence_with_definedness_flips(seed: u64, params: Params) -> Option<String> {
	let oracle = params.variant;
	drive_with(seed, params, true, oracle).divergence
}

fn drive_with(seed: u64, params: Params, flip_definedness: bool, oracle: Variant) -> DriveOutcome {
	let variant = params.variant;
	let mut harness = Harness::with_engine(|engine, _| build(engine, variant));
	let workload = JoinWorkload {
		keys: params.keys,
		right_pct: params.right_pct,
		none_pct: params.none_pct,
		rekey_pct: params.rekey_pct,
		flip_definedness,
	};
	let scenario = scenario(&params);

	match (oracle.snapshot, oracle.latest) {
		(true, _) => driver::drive(
			seed,
			scenario,
			&mut harness,
			&workload,
			&mut SnapshotOracle::new(oracle.left_outer(), oracle.latest),
		),
		(false, false) => driver::drive(
			seed,
			scenario,
			&mut harness,
			&workload,
			&mut HashOracle::new(oracle.left_outer()),
		),
		(false, true) => driver::drive(
			seed,
			scenario,
			&mut harness,
			&workload,
			&mut LatestOracle::new(oracle.left_outer()),
		),
	}
}

/// Loads a fixed right side, then drives left-side traffic against it.
///
/// This is the shape `snapshot` is defined for. A snapshot join publishes nothing when a right row
/// arrives, but it keeps no record of what it already published, so a later left removal is computed
/// against whatever the right store holds AT THAT MOMENT. While the right side stops changing the
/// two agree and the mode is exactly "do the same work, skip the right-side emissions"; once it keeps
/// changing there is no longer a single table the operator could be describing. Freezing the right
/// side is what makes the cell testable rather than a guess at intent.
///
/// The load itself is asserted silent. That is not decoration: an inner join with no left rows yet
/// would emit nothing whether or not `snapshot` is set, so it is the one place a right-side emission
/// would be visible without also depending on the left side.
pub fn drive_static_right(seed: u64, params: Params) -> DriveOutcome {
	let variant = params.variant;
	let mut harness = Harness::with_engine(|engine, _| build(engine, variant));
	let workload = JoinWorkload {
		keys: params.keys,
		// The driver only ever draws left rows: the right side is the fixed set loaded below.
		right_pct: 0,
		none_pct: params.none_pct,
		rekey_pct: params.rekey_pct,
		flip_definedness: false,
	};
	let loaded = right_side(seed, &params);
	let emitted = harness.apply(workload.insert(&loaded)).expect("loading the right side must succeed");
	assert!(
		emitted.diffs.is_empty(),
		"{}: a right row arriving before any left row has nothing to join and must publish nothing, but \
		 the load emitted {} diffs",
		variant.label(),
		emitted.diffs.len()
	);

	match variant.latest {
		false => load_and_drive(seed, params, &mut harness, &workload, &loaded, HashOracle::new(variant.outer)),
		true => {
			load_and_drive(seed, params, &mut harness, &workload, &loaded, LatestOracle::new(variant.outer))
		}
	}
}

fn load_and_drive<M: Model<JoinRow>>(
	seed: u64,
	params: Params,
	harness: &mut Harness<JoinOperator>,
	workload: &JoinWorkload,
	loaded: &[JoinRow],
	mut model: M,
) -> DriveOutcome {
	for row in loaded {
		model.admit(row);
	}
	driver::drive(seed, scenario(&params), harness, workload, &mut model)
}

/// The frozen right side. Drawn from its own stream so it does not shift when the driver's draws
/// change, and numbered far above the driver's counter so the two can never collide.
fn right_side(seed: u64, params: &Params) -> Vec<JoinRow> {
	let mut rng = StdRng::seed_from_u64(seed ^ 0x5EED_57A7_1C21_6047);
	(0..params.static_right)
		.map(|index| JoinRow {
			side: Side::Right,
			number: RowNumber(1_000_000 + index as u64),
			key: Some(rng.random_range(1..=params.keys)),
			value: rng.random_range(1..100i64),
		})
		.collect()
}

/// The random sweep stays on the four non-snapshot cells: it interleaves both sides freely, which is
/// exactly the shape a snapshot join has no defined answer for.
const VARIANTS: [Variant; 4] =
	[Variant::inner(), Variant::left(), Variant::inner().with_latest(), Variant::left().with_latest()];

/// Deliberately includes 1: a single key puts every row on both sides into one bucket, which is
/// where the cartesian product is widest and where a per-key slot is rewritten most often.
const KEYS: [i32; 5] = [1, 2, 3, 8, 24];

pub fn random_params(seed: u64) -> (u64, Params) {
	let (mut rng, sequence_seed) = split(seed);
	let max_batch = rng.random_range(1..=8u32);
	let params = Params {
		variant: pick(&mut rng, &VARIANTS),
		keys: pick(&mut rng, &KEYS),
		// Never 0 and never 100: a side that never receives a row leaves the whole opposite-side
		// probe unreachable, and the sweep would spend its steps on an operator that cannot emit.
		right_pct: rng.random_range(20..=80u32),
		none_pct: rng.random_range(0..=25u32),
		rekey_pct: rng.random_range(0..=60u32),
		max_batch,
		steps: rng.random_range(30..=90u32).min((320 / max_batch).max(30)),
		max_live: rng.random_range(8..=60usize),
		remove_pct: rng.random_range(5..=35u32),
		update_pct: rng.random_range(5..=40u32),
		static_right: 0,
	};
	(sequence_seed, params)
}

pub fn drive_random(seed: u64) {
	let (sequence_seed, params) = random_params(seed);
	let run = params.clone();
	run_reported("join_random_chaos", sequence_seed, &params, || {
		drive(sequence_seed, run);
	});
}
