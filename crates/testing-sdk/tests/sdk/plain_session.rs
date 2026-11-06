// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::WindowKind,
	operator_with::{ApplyWith, WithSpan},
	state::timer::TimerKind,
};
use reifydb_sdk::flow::operator::windowed::plain::PlainDriver;
use reifydb_testing_sdk::{builders::TestChangeBuilder, in_process::harness::InProcessOperatorHarnessBuilder};
use reifydb_value::{
	factory::time::millis,
	value::{datetime::DateTime, diff_type::DiffType},
};

use crate::plain_rolling::{SumTumblingOnly, harness, input_row, render};

fn session(gap: u64, lateness: Option<u64>) -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Session {
			gap: millis(gap),
		}),
		lateness: lateness.map(|n| WithSpan::Duration(millis(n))),
		immutable: None,
		retention: None,
	}
}

fn at(ms: u64) -> u64 {
	DateTime::from_millis(ms as i64).to_order()
}

#[test]
fn a_row_within_the_gap_after_the_last_extends_the_session_forward() {
	// an end taken from the last row instead of last plus gap would close the window on its newest row
	let mut h = harness!(SumTumblingOnly, session(10, None)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");

	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 105, 2.0)).build()).expect("apply");

	assert_eq!(render(&out), vec![(DiffType::Update, 3.0, at(100), at(115))]);
}

#[test]
fn a_row_within_the_gap_before_the_start_extends_the_session_backwards() {
	// a session keyed by its start would move to a new group when the start moves and publish a second row
	let mut h = harness!(SumTumblingOnly, session(10, None)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");

	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 95, 2.0)).build()).expect("apply");

	assert_eq!(render(&out), vec![(DiffType::Update, 3.0, at(95), at(110))]);
}

#[test]
fn a_row_more_than_one_gap_after_the_last_opens_a_new_session() {
	// a rotation that kept the old id would fold the new row into the closed session
	let mut h = harness!(SumTumblingOnly, session(10, None)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");

	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 111, 2.0)).build()).expect("apply");

	assert_eq!(render(&out), vec![(DiffType::Insert, 2.0, at(111), at(121))]);
}

#[test]
fn a_row_exactly_one_gap_from_the_last_starts_a_new_session() {
	// Session spans are half-open, so a row exactly one gap after the last starts a new session.
	let mut h = harness!(SumTumblingOnly, session(10, None)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");

	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 110, 2.0)).build()).expect("apply");

	assert_eq!(render(&out), vec![(DiffType::Insert, 2.0, at(110), at(120))]);
}

#[test]
fn a_row_more_than_one_gap_before_the_start_opens_its_own_session() {
	// a row a gap away from every live row is its own session, and it must neither join nor move the other one
	let mut h = harness!(SumTumblingOnly, session(10, None)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");

	let refused = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 89, 2.0)).build()).expect("apply");
	let after = h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 105, 4.0)).build()).expect("apply");

	assert_eq!(render(&refused), vec![(DiffType::Insert, 2.0, at(89), at(99))]);
	assert_eq!(render(&after), vec![(DiffType::Update, 5.0, at(100), at(115))]);
}

#[test]
fn a_remove_after_a_backward_move_and_a_rotation_hits_the_first_session() {
	// a remove must find the first session after a rotation, and once 95 leaves its start must shrink to 100
	let mut h = harness!(SumTumblingOnly, session(10, None)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");
	h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 95, 2.0)).build()).expect("apply");
	h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 200, 4.0)).build()).expect("apply");

	let out = h.apply(TestChangeBuilder::new().remove(input_row(2, "BTC", 95, 2.0)).build()).expect("apply");

	assert_eq!(render(&out), vec![(DiffType::Update, 1.0, at(100), at(110))]);
}

#[test]
fn an_update_after_a_rotation_changes_the_first_session_not_the_current_one() {
	// an update routed by assigning its time would miss the first session and lose the new value
	let mut h = harness!(SumTumblingOnly, session(10, None)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");
	h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 95, 2.0)).build()).expect("apply");
	h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 200, 4.0)).build()).expect("apply");

	let out = h
		.apply(TestChangeBuilder::new()
			.update(input_row(2, "BTC", 95, 2.0), input_row(2, "BTC", 95, 5.0))
			.build())
		.expect("apply");

	assert_eq!(render(&out), vec![(DiffType::Update, 6.0, at(95), at(110))]);
}

#[test]
fn a_session_arms_its_seal_at_last_plus_gap_plus_one_millisecond() {
	// a timer armed from the start fires while the session can still extend and seals it early
	let mut h = harness!(SumTumblingOnly, session(10, None)).expect("harness");

	h.apply(TestChangeBuilder::new()
		.insert(input_row(1, "BTC", 100, 1.0))
		.insert(input_row(2, "BTC", 107, 2.0))
		.build())
		.expect("apply");

	let seals: Vec<(DateTime, TimerKind)> =
		h.armed_timers().into_iter().filter(|t| t.key == b"").map(|t| (t.due, t.kind)).collect();
	assert_eq!(seals, vec![(DateTime::from_millis(118), TimerKind::Seal)]);
}

#[test]
fn a_session_is_open_at_last_plus_gap_and_sealed_one_millisecond_later() {
	// a gate on anything but the last row either extends a reaped session or refuses a row into a live one
	let feed = || {
		TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 100, 1.0))
			.insert(input_row(2, "BTC", 107, 2.0))
			.build()
	};
	let mut open = harness!(SumTumblingOnly, session(10, None)).expect("harness");
	open.apply(feed()).expect("apply");
	open.on_timer(DateTime::from_millis(117), TimerKind::Seal, b"").expect("timer");
	let mut sealed = harness!(SumTumblingOnly, session(10, None)).expect("harness");
	sealed.apply(feed()).expect("apply");
	sealed.on_timer(DateTime::from_millis(118), TimerKind::Seal, b"").expect("timer");

	let landed = open.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 108, 4.0)).build()).expect("apply");
	let dropped =
		sealed.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 108, 4.0)).build()).expect("apply");

	assert_eq!(render(&landed), vec![(DiffType::Update, 7.0, at(100), at(118))]);
	assert_eq!(render(&dropped), vec![]);
}

#[test]
fn a_session_seals_by_its_last_row_not_its_start() {
	// expiry anchored at the start would reap a session whose last row is still inside the gap
	let mut h = harness!(SumTumblingOnly, session(10, None)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");
	h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 105, 2.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(112)).expect("watermark");

	let out = h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 110, 4.0)).build()).expect("apply");

	assert_eq!(render(&out), vec![(DiffType::Update, 7.0, at(100), at(120))]);
}

#[test]
fn an_update_that_moves_a_row_leaves_its_old_session_and_joins_by_its_new_time() {
	// a moved row kept in its old session leaves both sums wrong, and the old session must shrink to the rows left
	let mut h = harness!(SumTumblingOnly, session(10, None)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");
	h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 105, 2.0)).build()).expect("apply");
	h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 200, 4.0)).build()).expect("apply");

	let out = h
		.apply(TestChangeBuilder::new()
			.update(input_row(2, "BTC", 105, 2.0), input_row(2, "BTC", 203, 2.0))
			.build())
		.expect("apply");

	assert_eq!(
		render(&out),
		vec![(DiffType::Update, 1.0, at(100), at(110)), (DiffType::Update, 6.0, at(200), at(213))]
	);
}

#[test]
fn an_update_to_a_time_a_gap_from_every_live_row_leaves_its_session_and_opens_a_new_one() {
	// 150 is more than a gap from 100 and from 200, so a fresh run puts the moved row in a session of its own.
	let mut h = harness!(SumTumblingOnly, session(10, None)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");
	h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 105, 2.0)).build()).expect("apply");
	h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 200, 4.0)).build()).expect("apply");

	let out = h
		.apply(TestChangeBuilder::new()
			.update(input_row(2, "BTC", 105, 2.0), input_row(2, "BTC", 150, 5.0))
			.build())
		.expect("apply");

	assert_eq!(
		render(&out),
		vec![(DiffType::Insert, 5.0, at(150), at(160)), (DiffType::Update, 1.0, at(100), at(110))]
	);
}

#[test]
fn a_remove_of_a_row_in_a_sealed_session_is_dropped() {
	// a remove whose session is sealed must be dropped, never retracted from the live session
	let mut h = harness!(SumTumblingOnly, session(10, None)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");
	h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 200, 2.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(150)).expect("watermark");

	let removed = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");
	let after = h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 205, 4.0)).build()).expect("apply");

	assert_eq!(render(&removed), vec![]);
	assert_eq!(render(&after), vec![(DiffType::Update, 6.0, at(200), at(215))]);
}

#[test]
fn the_driver_builds_a_session_with_a_lateness() {
	// refusing a lateness here would split the sdk from the native session window, which honours it
	let built = harness!(SumTumblingOnly, session(10, Some(1)));

	assert!(built.is_ok(), "a session lateness must build, got: {:?}", built.err().map(|e| e.to_string()));
}

#[test]
fn the_driver_builds_a_session_with_no_or_zero_lateness() {
	// refusing a declared zero would reject a session whose lateness adds nothing to its seal
	for lateness in [None, Some(0)] {
		let built = harness!(SumTumblingOnly, session(10, lateness));

		assert!(
			built.is_ok(),
			"lateness {lateness:?} must build, got: {:?}",
			built.err().map(|e| e.to_string())
		);
	}
}

#[test]
fn the_driver_refuses_a_session_with_a_zero_gap() {
	// a zero gap gives every session an empty span, so create must fail instead of publishing zero-width rows
	let err = harness!(SumTumblingOnly, session(0, None)).err().expect("create must fail");

	assert!(err.to_string().contains("FLOW_079"), "expected FLOW_079, got: {err}");
}

#[test]
fn an_update_of_a_row_no_session_holds_files_its_new_value_as_an_insert() {
	// an update whose pre was never indexed must still land its post value, as tumbling and RQL do
	let mut h = harness!(SumTumblingOnly, session(10, None)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");

	let out = h
		.apply(TestChangeBuilder::new()
			.update(input_row(9, "BTC", 104, 2.0), input_row(9, "BTC", 104, 3.0))
			.build())
		.expect("apply");

	assert_eq!(render(&out), vec![(DiffType::Update, 4.0, at(100), at(114))]);
}

#[test]
fn an_update_that_changes_the_group_moves_the_row_between_groups() {
	// an update kept in place across a group change leaves the row counted under its old group
	let mut h = harness!(SumTumblingOnly, session(10, None)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");
	h.apply(TestChangeBuilder::new().insert(input_row(2, "ETH", 100, 2.0)).build()).expect("apply");

	let out = h
		.apply(TestChangeBuilder::new()
			.update(input_row(1, "BTC", 100, 1.0), input_row(1, "ETH", 100, 1.0))
			.build())
		.expect("apply");

	assert_eq!(
		render(&out),
		vec![(DiffType::Update, 3.0, at(100), at(110)), (DiffType::Remove, 1.0, at(100), at(110))]
	);
}

#[test]
fn a_refilled_session_publishes_an_insert() {
	// Downstream already dropped the removed session, so an update retracting it corrupts every consumer.
	let mut h = InProcessOperatorHarnessBuilder::<PlainDriver<SumTumblingOnly>>::new()
		.with(session(10, None))
		.build()
		.expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");
	assert_eq!(render(&out), vec![(DiffType::Remove, 1.0, at(100), at(110))], "precondition");

	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 105, 2.0)).build()).expect("apply");

	let kinds: Vec<(DiffType, f64)> = render(&out).into_iter().map(|(kind, sum, _, _)| (kind, sum)).collect();
	assert_eq!(kinds, vec![(DiffType::Insert, 2.0)]);
}

#[test]
fn an_emptied_session_publishes_nothing_on_seal() {
	// A second removal of a session downstream already dropped is a retraction of nothing.
	let mut h = harness!(SumTumblingOnly, session(10, None)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");
	assert_eq!(render(&out), vec![(DiffType::Remove, 1.0, at(100), at(110))], "precondition");
	h.advance_watermark(DateTime::from_millis(10_000)).expect("watermark");

	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "ETH", 10_000, 4.0)).build()).expect("apply");

	assert_eq!(
		render(&out),
		vec![(DiffType::Insert, 4.0, at(10_000), at(10_010))],
		"only the new session publishes"
	);
}
