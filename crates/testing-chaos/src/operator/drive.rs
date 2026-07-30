// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_value::value::row_number::RowNumber;

use crate::{
	corpus::{Corpus, mix},
	operator::{compare::contains_all, model::Model, session::Session, subject::Subject, workload::Workload},
};

pub struct Params {
	pub steps: u32,
	pub max_batch: u32,
	pub coord_span_ms: u64,
	pub remove_pct: u32,
	pub update_pct: u32,
	pub seal_pct: u32,
	pub drain_at_ms: u64,
}

/// Feeds one corpus to an operator and to a model, and checks the operator's materialized view
/// against what the model says must, may, and must not be there.
///
/// The RNG draw order here is a compatibility surface. Per step it draws the branch roll, then within
/// a branch: one value for a seal, one for a remove, one plus the workload's single revalue draw for
/// an update, and one plus the workload's per-row draws for an insert. Pinned regressions record a
/// fingerprint of the resulting sequence, so adding or reordering a draw re-points every one of them
/// at a corpus that no longer contains the defect it names - and they would keep passing.
pub fn drive<S, W, M>(seed: u64, params: Params, subject: &mut S, workload: &W, model: &mut M) -> Corpus
where
	S: Subject,
	W: Workload,
	M: Model<W::Row>,
{
	let mut rng = StdRng::seed_from_u64(seed);
	let mut session = Session::new(subject);
	let mut live: Vec<W::Row> = Vec::new();
	let mut next_row = RowNumber(1);
	let mut watermark = 0u64;
	let mut trace: Vec<String> = Vec::new();
	// Fingerprints the operations themselves, not their rendered trace, so reformatting a trace line
	// cannot invalidate every pinned seed in the suite.
	let mut fingerprint = mix(0, seed);

	for step in 0..params.steps {
		let roll = rng.random_range(0..100);

		if roll < params.seal_pct {
			watermark = watermark.saturating_add(rng.random_range(1..=params.coord_span_ms / 2));
			trace.push(format!("step {step}: seal at {watermark}"));
			fingerprint = mix(mix(fingerprint, 1), watermark);
			session.tick(watermark).expect("tick must succeed");
			model.advance_ledger(watermark);
		} else if !live.is_empty() && roll < params.seal_pct + params.remove_pct {
			let idx = rng.random_range(0..live.len());
			let row = live.remove(idx);
			trace.push(format!("step {step}: remove {row:?}"));
			let lanes = workload.lanes(&row);
			fingerprint = mix(mix(mix(mix(fingerprint, 2), lanes.number), lanes.group), lanes.coord);
			model.retract(&row);
			session.apply(workload.remove(&row)).expect("apply must succeed");
		} else if !live.is_empty() && roll < params.seal_pct + params.remove_pct + params.update_pct {
			let idx = rng.random_range(0..live.len());
			let pre = live[idx].clone();
			let post = workload.revalue(&mut rng, &pre);
			live[idx] = post.clone();
			trace.push(format!("step {step}: update {pre:?} -> {post:?}"));
			let lanes = workload.lanes(&post);
			fingerprint = mix(mix(mix(mix(fingerprint, 3), lanes.number), lanes.coord), lanes.value);
			model.retract(&pre);
			model.admit(&post);
			session.apply(workload.update(&pre, &post)).expect("apply must succeed");
		} else {
			let count = rng.random_range(1..=params.max_batch);
			let mut batch: Vec<W::Row> = Vec::new();
			for _ in 0..count {
				batch.push(workload.sample(&mut rng, next_row));
				next_row = RowNumber(next_row.0 + 1);
			}
			trace.push(format!("step {step}: insert {batch:?}"));
			fingerprint = mix(fingerprint, 4);
			for row in &batch {
				let lanes = workload.lanes(row);
				fingerprint = mix(
					mix(mix(mix(fingerprint, lanes.number), lanes.group), lanes.coord),
					lanes.value,
				);
			}
			for row in &batch {
				if model.admit(row) {
					live.push(row.clone());
				}
			}
			session.apply(workload.insert(&batch)).expect("apply must succeed");
		}

		if !session.incoherent().is_empty() {
			dump(&trace);
			panic!(
				"step {step}: the operator emitted a diff stream that cannot be folded: {:?}",
				session.incoherent()
			);
		}

		let actual = session.projected(workload.projection());
		let required = model.live();
		let possible = model.all();

		if !contains_all(&actual, &required, workload.tolerances()) {
			dump(&trace);
			panic!(
				"step {step}: a row the model still requires is missing from the view or holds the \
				 wrong value.\n  actual: {actual:?}\n  required: {required:?}"
			);
		}
		if !contains_all(&possible, &actual, workload.tolerances()) {
			dump(&trace);
			panic!(
				"step {step}: the operator published a row the model never produced.\n  actual: \
				 {actual:?}\n  possible: {possible:?}"
			);
		}
	}

	model.advance_ledger(params.drain_at_ms);

	let ticks = session.drain(params.drain_at_ms, 256).expect("drain tick must succeed");

	let actual = session.projected(workload.projection());
	let expected = model.after_drain();
	if !(contains_all(&actual, &expected, workload.tolerances())
		&& contains_all(&expected, &actual, workload.tolerances()))
	{
		dump(&trace);
		panic!(
			"repeated ticks past every horizon must leave exactly what the model says survives; got \
			 {actual:?} after {ticks} ticks, expected {expected:?}"
		);
	}
	assert!(
		session.incoherent().is_empty(),
		"drain emitted an unfoldable diff stream: {:?}",
		session.incoherent()
	);

	Corpus::new(fingerprint, trace.len())
}

fn dump(trace: &[String]) {
	eprintln!("--- executed sequence ({} steps) ---", trace.len());
	for line in trace {
		eprintln!("{line}");
	}
	eprintln!("--- end sequence ---");
}
