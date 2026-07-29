// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_abi::operator::timer::TimerKind;
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_flow::{operator::Operator, transaction::timer::Timer};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::value::{Value, datetime::DateTime};

use crate::framework::{generator, harness::Harness, materialize::View};

/// The reference implementation an operator is differentially tested against.
///
/// The driver feeds the same corpus to the operator and to the model, then checks the
/// operator's materialized view against what the model says must, may, and must not be there.
pub trait Model {
	/// Routes an insert. Returns false when the model considers the row too late to be
	/// admitted, so the driver knows not to offer it for retraction later.
	fn admit(&mut self, group: i32, coord_ms: u64, value: i64) -> bool;

	fn retract(&mut self, group: i32, coord_ms: u64, value: i64);

	fn advance_ledger(&mut self, at_ms: u64);

	/// Rows the operator MUST be publishing: anything still inside its seal horizon.
	fn live(&self) -> Vec<Vec<Value>>;

	/// Rows the operator MAY be publishing. A window the model has closed can legitimately
	/// still be in the view until a tick withdraws it, so `live` alone cannot bound the
	/// view from above.
	fn all(&self) -> Vec<Vec<Value>>;

	/// Rows that must remain once the ledger has run past every horizon and the driver has
	/// ticked to quiescence. Empty for a window kind where everything eventually expires.
	fn after_drain(&self) -> Vec<Vec<Value>>;
}

pub struct Params {
	pub groups: i32,
	pub steps: u32,
	pub max_batch: u32,
	pub coord_span_ms: u64,
	pub remove_pct: u32,
	pub update_pct: u32,
	pub seal_pct: u32,
	pub drain_at_ms: u64,
}

fn seal_timer(at_ms: u64) -> Timer {
	Timer {
		at: DateTime::from_timestamp_millis(at_ms).unwrap(),
		kind: TimerKind::Seal,
		key: EncodedKey::new(Vec::new()),
	}
}

pub fn drive<O: Operator, M: Model>(seed: u64, params: Params, build: impl FnOnce(RuntimeContext) -> O, mut model: M) {
	let mut rng = StdRng::seed_from_u64(seed);
	let mut harness = Harness::new(build);
	let mut view = View::new();
	let mut live: Vec<(u64, i32, u64, i64)> = Vec::new();
	let mut next_row = 1u64;
	let mut watermark = 0u64;
	let mut trace: Vec<String> = Vec::new();

	for step in 0..params.steps {
		let roll = rng.random_range(0..100);

		if roll < params.seal_pct {
			watermark = watermark.saturating_add(rng.random_range(1..=params.coord_span_ms / 2));
			trace.push(format!("step {step}: seal at {watermark}"));
			let out = harness.on_timer(seal_timer(watermark)).expect("on_timer must succeed");
			model.advance_ledger(watermark);
			if let Some(change) = out {
				view.apply(&change);
			}
		} else if !live.is_empty() && roll < params.seal_pct + params.remove_pct {
			let idx = rng.random_range(0..live.len());
			let (number, group, coord_ms, value) = live.remove(idx);
			trace.push(format!("step {step}: remove row={number} g={group} coord={coord_ms} v={value}"));
			model.retract(group, coord_ms, value);
			let change = generator::remove(vec![generator::row(
				number,
				group,
				value,
				DateTime::from_timestamp_millis(coord_ms).unwrap(),
			)]);
			let out = harness.apply(change).expect("apply must succeed");
			view.apply(&out);
		} else if !live.is_empty() && roll < params.seal_pct + params.remove_pct + params.update_pct {
			// An update rewrites the VALUE and leaves the coordinate alone. Moving the
			// coordinate would be a different test: sliding pins an updated row to the windows
			// it was first indexed into rather than recomputing them from the new coordinate,
			// so a coordinate-moving update is not simply retract-then-admit and needs a model
			// of its own.
			let idx = rng.random_range(0..live.len());
			let (number, group, coord_ms, value) = live[idx];
			let replacement = rng.random_range(1..100i64);
			live[idx] = (number, group, coord_ms, replacement);
			trace.push(format!(
				"step {step}: update row={number} g={group} coord={coord_ms} v={value}->{replacement}"
			));
			model.retract(group, coord_ms, value);
			model.admit(group, coord_ms, replacement);
			let at = DateTime::from_timestamp_millis(coord_ms).unwrap();
			let change = generator::update(vec![(
				generator::row(number, group, value, at),
				generator::row(number, group, replacement, at),
			)]);
			let out = harness.apply(change).expect("apply must succeed");
			view.apply(&out);
		} else {
			let count = rng.random_range(1..=params.max_batch);
			let mut batch: Vec<(u64, i32, u64, i64)> = Vec::new();
			for _ in 0..count {
				batch.push((
					next_row,
					rng.random_range(1..=params.groups),
					rng.random_range(0..params.coord_span_ms),
					rng.random_range(1..100i64),
				));
				next_row += 1;
			}
			trace.push(format!("step {step}: insert {batch:?}"));
			let mut rows = Vec::new();
			for (number, group, coord_ms, value) in &batch {
				if model.admit(*group, *coord_ms, *value) {
					live.push((*number, *group, *coord_ms, *value));
				}
				rows.push(generator::row(
					*number,
					*group,
					*value,
					DateTime::from_timestamp_millis(*coord_ms).unwrap(),
				));
			}
			let out = harness.apply(generator::insert(rows)).expect("apply must succeed");
			view.apply(&out);
		}

		if !view.incoherent.is_empty() {
			dump(&trace);
			panic!(
				"step {step}: the operator emitted a diff stream that cannot be folded: {:?}",
				view.incoherent
			);
		}

		let actual = view.projected(&[0, 1]);
		let required = model.live();
		let possible = model.all();

		if !contains_all(&actual, &required) {
			dump(&trace);
			panic!("step {step}: a window that is still inside its seal horizon is missing or has \
				 the wrong total.\n  actual: {actual:?}\n  required: {required:?}");
		}
		if !contains_all(&possible, &actual) {
			dump(&trace);
			panic!("step {step}: the operator published a window the oracle never produced.\n  \
				 actual: {actual:?}\n  possible: {possible:?}");
		}
	}

	model.advance_ledger(params.drain_at_ms);

	let mut ticks = 0;
	loop {
		let before = view.len();
		let out = harness.on_timer(seal_timer(params.drain_at_ms)).expect("drain seal must succeed");
		if let Some(change) = out {
			view.apply(&change);
		}
		ticks += 1;
		if view.len() == before || view.is_empty() {
			break;
		}
		assert!(ticks < 256, "window expiry did not reach quiescence within {ticks} ticks");
	}

	let actual = view.projected(&[0, 1]);
	let expected = model.after_drain();
	if actual != expected {
		dump(&trace);
		panic!("repeated ticks past every horizon must leave exactly what the oracle says survives; \
			 got {actual:?} after {ticks} ticks, expected {expected:?}");
	}
	assert!(view.incoherent.is_empty(), "drain emitted an unfoldable diff stream: {:?}", view.incoherent);
}

fn contains_all(haystack: &[Vec<Value>], needles: &[Vec<Value>]) -> bool {
	let mut pool = haystack.to_vec();
	for needle in needles {
		match pool.iter().position(|candidate| candidate == needle) {
			Some(idx) => {
				pool.remove(idx);
			}
			None => return false,
		}
	}
	true
}

fn dump(trace: &[String]) {
	eprintln!("--- executed sequence ({} steps) ---", trace.len());
	for line in trace {
		eprintln!("{line}");
	}
	eprintln!("--- end sequence ---");
}
