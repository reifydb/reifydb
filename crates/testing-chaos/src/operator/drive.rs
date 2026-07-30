// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_value::value::row_number::RowNumber;

use crate::{
	corpus::{Corpus, mix},
	operator::{
		compare::contains_all, model::Model, scenario::Scenario, session::Session, subject::Subject,
		workload::Workload,
	},
};

/// Feeds one corpus to an operator and to a model, and checks the operator's materialized view
/// against what the model says must, may, and must not be there.
///
/// The RNG draw order is a compatibility surface. Per step it draws the branch roll, then within a
/// branch: one value for a tick, one for a remove, one plus the workload's single revalue draw for an
/// update, and the batch size plus the workload's per-row draws for an insert. The mutation primitives
/// draw only when enabled, which is what lets a scenario written without them produce the sequence it
/// always did. Pinned regressions record a fingerprint of that sequence, so adding or reordering a draw
/// re-points every one of them at a corpus that no longer contains the defect it names - and they would
/// keep passing.
pub fn drive<S, W, M>(seed: u64, scenario: Scenario, subject: &mut S, workload: &W, model: &mut M) -> Corpus
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

	for step in 0..scenario.steps {
		let roll = rng.random_range(0..100);

		if roll < scenario.tick_pct {
			watermark = watermark.saturating_add(rng.random_range(1..=scenario.coord_span_ms / 2));
			trace.push(format!("step {step}: seal at {watermark}"));
			fingerprint = mix(mix(fingerprint, 1), watermark);
			session.tick(watermark).expect("tick must succeed");
			model.advance_ledger(watermark);
		} else if !live.is_empty() && roll < scenario.tick_pct + scenario.remove_pct {
			let idx = rng.random_range(0..live.len());
			let row = live.remove(idx);
			trace.push(format!("step {step}: remove {row:?}"));
			let lanes = workload.lanes(&row);
			fingerprint = mix(mix(mix(mix(fingerprint, 2), lanes.number), lanes.group), lanes.coord);
			model.retract(&row);
			session.apply(workload.remove(&row)).expect("apply must succeed");
		} else if !live.is_empty() && roll < scenario.tick_pct + scenario.remove_pct + scenario.update_pct {
			let idx = rng.random_range(0..live.len());
			let pre = live[idx].clone();
			let post = workload.revalue(&mut rng, &pre);
			live[idx] = post.clone();
			trace.push(format!("step {step}: update {pre:?} -> {post:?}"));
			let lanes = workload.lanes(&post);
			fingerprint = mix(mix(mix(mix(fingerprint, 3), lanes.number), lanes.coord), lanes.value);
			model.retract(&pre);
			model.admit(&post);

			let split = Scenario::rolls(scenario.update_as_remove_insert)
				&& rng.random::<f64>() < scenario.update_as_remove_insert;
			if split {
				// The same transition as an update, carried by a different diff stream. An operator
				// that handles one path and not the other diverges only here.
				fingerprint = mix(fingerprint, 5);
				session.apply(workload.remove(&pre)).expect("apply must succeed");
				session.apply(workload.insert(std::slice::from_ref(&post)))
					.expect("apply must succeed");
			} else {
				session.apply(workload.update(&pre, &post)).expect("apply must succeed");

				let duplicate = Scenario::rolls(scenario.duplicate_update_burst)
					&& rng.random::<f64>() < scenario.duplicate_update_burst;
				if duplicate {
					// An upstream join re-emitting a row unchanged. The model sees a retract and
					// an admit of the same value, so a correct operator nets to no change; one
					// that adds on arrival rather than diffing counts it twice.
					fingerprint = mix(fingerprint, 6);
					model.retract(&post);
					model.admit(&post);
					session.apply(workload.update(&post, &post)).expect("apply must succeed");
				}
			}
		} else {
			let count = scenario.batch.draw(&mut rng);
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

			// Trimming the oldest keeps mutations concentrated on recent rows. Without a cap the
			// corpus grows and a remove or update almost never revisits a row twice, which is exactly
			// the pressure that surfaces re-publish and conflict defects.
			if let Some(cap) = scenario.max_live {
				while live.len() > cap {
					live.remove(0);
				}
			}
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

	model.advance_ledger(scenario.drain_at_ms);

	let ticks = session.drain(scenario.drain_at_ms, 256).expect("drain tick must succeed");

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
