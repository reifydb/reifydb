// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod oracle;
pub mod regression;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_abi::operator::timer::TimerKind;
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::common::{TimeDomain, WindowKind, WindowSize};
use reifydb_flow::transaction::timer::Timer;
use reifydb_value::value::{Value, datetime::DateTime, duration::Duration};

use crate::{
	framework::{generator, harness::Harness, materialize::View},
	operators::window::{WindowSpec, build, tumbling::oracle::Oracle},
};

pub struct Params {
	pub size_secs: u64,
	pub grace_secs: u64,
	pub groups: i32,
	pub steps: u32,
	pub max_batch: u32,
	pub coord_span_ms: u64,
	pub remove_pct: u32,
	pub seal_pct: u32,
}

pub fn drive(seed: u64, params: Params) {
	let mut rng = StdRng::seed_from_u64(seed);
	let size_ms = params.size_secs * 1_000;
	let grace_ms = params.grace_secs * 1_000;

	let spec = WindowSpec {
		kind: WindowKind::Tumbling {
			size: WindowSize::Duration(Duration::from_seconds(params.size_secs as i64).unwrap()),
		},
		domain: TimeDomain::Event,
		group_by: "g",
		aggregations: "total: math::sum(v)",
		grace: Duration::from_seconds(params.grace_secs as i64).unwrap(),
		lateness: Duration::default(),
	};

	let mut harness = Harness::new(|runtime| build(&spec, runtime));
	let mut view = View::new();
	let mut oracle = Oracle::new(size_ms, grace_ms);
	let mut live: Vec<(u64, i32, u64, i64)> = Vec::new();
	let mut next_row = 1u64;
	let mut watermark = 0u64;
	let mut trace: Vec<String> = Vec::new();

	for step in 0..params.steps {
		let roll = rng.random_range(0..100);

		if roll < params.seal_pct {
			watermark = watermark.saturating_add(rng.random_range(1..=params.coord_span_ms / 2));
			let at = DateTime::from_timestamp_millis(watermark).unwrap();
			let timer = Timer {
				at,
				kind: TimerKind::Seal,
				key: EncodedKey::new(Vec::new()),
			};
			trace.push(format!("step {step}: seal at {watermark}"));
			let out = harness.on_timer(timer).expect("on_timer must succeed");
			oracle.advance_ledger(watermark);
			if let Some(change) = out {
				view.apply(&change);
			}
		} else if !live.is_empty() && roll < params.seal_pct + params.remove_pct {
			let idx = rng.random_range(0..live.len());
			let (number, group, coord_ms, value) = live.remove(idx);
			trace.push(format!("step {step}: remove row={number} g={group} coord={coord_ms} v={value}"));
			oracle.retract_batch(&[(group, coord_ms, value)]);
			let change = generator::remove(vec![generator::row(
				number,
				group,
				value,
				DateTime::from_timestamp_millis(coord_ms).unwrap(),
			)]);
			let out = harness.apply(change).expect("apply must succeed");
			view.apply(&out);
		} else {
			let count = rng.random_range(1..=params.max_batch);
			let mut batch: Vec<(i32, u64, i64)> = Vec::new();
			let mut numbers: Vec<u64> = Vec::new();
			for _ in 0..count {
				batch.push((
					rng.random_range(1..=params.groups),
					rng.random_range(0..params.coord_span_ms),
					rng.random_range(1..100i64),
				));
				numbers.push(next_row);
				next_row += 1;
			}
			trace.push(format!("step {step}: insert {batch:?}"));
			let accepted = oracle.add_batch(&batch);
			let mut rows = Vec::new();
			for (idx, (group, coord_ms, value)) in batch.iter().enumerate() {
				if accepted[idx] {
					live.push((numbers[idx], *group, *coord_ms, *value));
				}
				rows.push(generator::row(
					numbers[idx],
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
		let live = oracle.live();
		let all = oracle.all();

		if !contains_all(&actual, &live) {
			dump(&trace);
			panic!("step {step}: a window that is still inside its seal horizon is missing or has \
				 the wrong total.\n  actual: {actual:?}\n  required: {live:?}");
		}
		if !contains_all(&all, &actual) {
			dump(&trace);
			panic!("step {step}: the operator published a window the oracle never produced.\n  \
				 actual: {actual:?}\n  possible: {all:?}");
		}
	}

	let drain_at = params.coord_span_ms + size_ms + grace_ms + 10_000;
	oracle.advance_ledger(drain_at);

	let mut ticks = 0;
	loop {
		let timer = Timer {
			at: DateTime::from_timestamp_millis(drain_at).unwrap(),
			kind: TimerKind::Seal,
			key: EncodedKey::new(Vec::new()),
		};
		let before = view.len();
		let out = harness.on_timer(timer).expect("drain seal must succeed");
		if let Some(change) = out {
			view.apply(&change);
		}
		ticks += 1;
		if view.len() == before || view.is_empty() {
			break;
		}
		assert!(ticks < 256, "window expiry did not reach quiescence within {ticks} ticks");
	}

	if !view.is_empty() {
		dump(&trace);
		panic!(
			"every window is past its seal horizon, so repeated ticks must withdraw every row; {} \
			 remain after {ticks} ticks",
			view.len()
		);
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
