// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "chaos")]

#[path = "chaos/framework/mod.rs"]
mod framework;
#[path = "chaos/operators/mod.rs"]
mod operators;

use reifydb_core::common::{TimeDomain, WindowKind, WindowSize};
use reifydb_testing_macro::chaos_test;
use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::{
	framework::{generator, harness::Harness},
	operators::window::{WindowSpec, build},
};

fn tumbling_sum() -> WindowSpec {
	WindowSpec {
		kind: WindowKind::Tumbling {
			size: WindowSize::Duration(Duration::from_seconds(60).unwrap()),
		},
		domain: TimeDomain::Event,
		group_by: "g",
		aggregations: "total: math::sum(v)",
		grace: Duration::default(),
		lateness: Duration::default(),
	}
}

#[test]
fn a_window_operator_can_be_built_and_driven() {
	let spec = tumbling_sum();
	let mut harness = Harness::new(|runtime| build(&spec, runtime));

	let at = DateTime::from_timestamp_millis(60_000).unwrap();
	let change = generator::insert(vec![generator::row(1, 1, 10, at), generator::row(2, 1, 5, at)]);

	let out = harness.apply(change).expect("apply must succeed");

	assert!(
		!out.diffs.is_empty(),
		"a window fed two rows in one group must emit at least one diff; got none, which means the \
		 operator was built but never routed the batch"
	);
}

chaos_test!(window_tumbling_sum_chaos, |seed| {
	operators::window::tumbling::drive(
		seed,
		operators::window::tumbling::Params {
			size_secs: 60,
			grace_secs: 0,
			groups: 4,
			steps: 40,
			max_batch: 5,
			coord_span_ms: 600_000,
			remove_pct: 30,
			update_pct: 20,
			seal_pct: 20,
		},
	);
});

chaos_test!(window_tumbling_grace_chaos, |seed| {
	operators::window::tumbling::drive(
		seed,
		operators::window::tumbling::Params {
			size_secs: 30,
			grace_secs: 45,
			groups: 3,
			steps: 60,
			max_batch: 4,
			coord_span_ms: 400_000,
			remove_pct: 25,
			update_pct: 15,
			seal_pct: 30,
		},
	);
});

chaos_test!(window_sliding_sum_chaos, |seed| {
	operators::window::sliding::drive(
		seed,
		operators::window::sliding::Params {
			size_secs: 60,
			slide_secs: 15,
			grace_secs: 0,
			groups: 4,
			steps: 40,
			max_batch: 5,
			coord_span_ms: 600_000,
			remove_pct: 30,
			update_pct: 20,
			seal_pct: 20,
		},
	);
});

chaos_test!(window_rolling_sum_chaos, |seed| {
	operators::window::rolling::drive(
		seed,
		operators::window::rolling::Params {
			size_secs: 60,
			grace_secs: 0,
			groups: 4,
			steps: 40,
			max_batch: 5,
			coord_span_ms: 600_000,
			remove_pct: 30,
			update_pct: 20,
			seal_pct: 20,
		},
	);
});

chaos_test!(window_rolling_grace_chaos, |seed| {
	operators::window::rolling::drive(
		seed,
		operators::window::rolling::Params {
			size_secs: 30,
			grace_secs: 45,
			groups: 3,
			steps: 60,
			max_batch: 4,
			coord_span_ms: 400_000,
			remove_pct: 25,
			update_pct: 15,
			seal_pct: 30,
		},
	);
});

chaos_test!(window_sliding_grace_chaos, |seed| {
	operators::window::sliding::drive(
		seed,
		operators::window::sliding::Params {
			size_secs: 30,
			slide_secs: 10,
			grace_secs: 45,
			groups: 3,
			steps: 60,
			max_batch: 4,
			coord_span_ms: 400_000,
			remove_pct: 25,
			update_pct: 15,
			seal_pct: 30,
		},
	);
});
