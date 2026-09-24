// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::{WindowKind, WindowSize},
	operator_with::{ApplyWith, WithSpan},
	state::timer::TimerKind,
};
use reifydb_flow::operator::state::seal::coord::Coord;
use reifydb_sdk::flow::operator::{extern_c::binding::operator::ExternCOperatorAdapter, windowed::plain::PlainDriver};
use reifydb_testing_sdk::{builders::TestChangeBuilder, harness::ExternCOperatorHarnessBuilder};
use reifydb_value::{
	factory::time::millis,
	value::{datetime::DateTime, diff_type::DiffType},
};

use crate::plain_rolling::{SumTumblingOnly, harness, input_row, render};

fn sliding(size: u64, slide: u64, lateness: u64) -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Sliding {
			size: WindowSize::Duration(millis(size)),
			slide: WindowSize::Duration(millis(slide)),
		}),
		lateness: Some(WithSpan::Duration(millis(lateness))),
		immutable: None,
		retention: None,
	}
}

fn at(ms: u64) -> u64 {
	DateTime::from_millis(ms as i64).to_order()
}

#[test]
fn a_row_lands_in_every_window_it_overlaps() {
	// a driver that pushed only one span would leave the overlapping window without the row
	let mut h = harness!(SumTumblingOnly, sliding(10, 5, 3_600_000)).expect("harness");

	let out = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 107, 1.0)).build()).expect("apply");

	assert_eq!(
		render(&out),
		vec![(DiffType::Insert, 1.0, at(100), at(110)), (DiffType::Insert, 1.0, at(105), at(115))]
	);
}

#[test]
fn a_remove_retracts_the_row_from_every_window_it_joined() {
	// a retraction that reached only one window would leave the row counted in the other
	let mut h = harness!(SumTumblingOnly, sliding(10, 5, 3_600_000)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 107, 1.0)).build()).expect("apply");
	h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 112, 2.0)).build()).expect("apply");

	let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 107, 1.0)).build()).expect("apply");

	assert_eq!(
		render(&out),
		vec![(DiffType::Update, 2.0, at(105), at(115)), (DiffType::Remove, 1.0, at(100), at(110))]
	);
}

#[test]
fn an_update_that_moves_the_coord_moves_the_row_between_windows() {
	// windows taken from the post row for both sides would leave the old windows holding the row
	let mut h = harness!(SumTumblingOnly, sliding(10, 5, 3_600_000)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 107, 1.0)).build()).expect("apply");

	let out = h
		.apply(TestChangeBuilder::new()
			.update(input_row(1, "BTC", 107, 1.0), input_row(1, "BTC", 117, 1.0))
			.build())
		.expect("apply");

	assert_eq!(
		render(&out),
		vec![
			(DiffType::Insert, 1.0, at(110), at(120)),
			(DiffType::Insert, 1.0, at(115), at(125)),
			(DiffType::Remove, 1.0, at(100), at(110)),
			(DiffType::Remove, 1.0, at(105), at(115)),
		]
	);
}

#[test]
fn a_late_row_is_dropped_only_from_its_sealed_windows() {
	// dropping the whole row once its oldest window seals would lose it from windows still open
	let mut h = harness!(SumTumblingOnly, sliding(10, 5, 10)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(113)).expect("watermark");

	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 97, 2.0)).build()).expect("apply");

	assert_eq!(render(&out), vec![(DiffType::Update, 3.0, at(95), at(105))]);
}

#[test]
fn a_row_arms_the_seal_timer_for_its_newest_window() {
	// a timer set for the oldest window fires before the newest seals and leaves its state behind
	let mut h = harness!(SumTumblingOnly, sliding(10, 5, 30)).expect("harness");

	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 107, 1.0)).build()).expect("apply");

	let seals: Vec<(DateTime, TimerKind)> =
		h.armed_timers().into_iter().filter(|t| t.key == b"").map(|t| (t.due, t.kind)).collect();
	assert_eq!(seals, vec![(DateTime::from_millis(146), TimerKind::Seal)]);
}

#[test]
fn a_plain_operator_accepts_a_sliding_view() {
	// a create check that still demanded tumbling would refuse every sliding view with FLOW_066
	let built = harness!(SumTumblingOnly, sliding(10, 5, 3_600_000));

	assert!(built.is_ok(), "a sliding view must build, got: {:?}", built.err().map(|err| err.to_string()));
}
