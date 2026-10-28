// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::thread::sleep;

use reifydb_cdc::produce::watermark::CdcProducerWatermark;
use reifydb_core::{common::CommitVersion, interface::cdc::CdcChange};
use reifydb_runtime::context::clock::Clock;
use reifydb_store_cdc::{storage::CdcStorage, store::CdcStore};
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::duration::Duration;

fn deletes_committed_by(test: &TestEngine, rql: &str) -> Vec<bool> {
	let before = test.current_version().unwrap();
	test.command(rql);
	let after = test.current_version().unwrap();

	let watermark = test.ioc().try_resolve::<CdcProducerWatermark>().unwrap();
	let deadline = Clock::Real.instant() + Duration::from_seconds_const(5);
	while watermark.get() < after {
		assert!(Clock::Real.instant() < deadline, "the cdc producer did not reach {after:?} in time");
		sleep(Duration::from_milliseconds_const(10).to_std());
	}

	let cdc = test.ioc().try_resolve::<CdcStore>().unwrap();
	let mut deletes = Vec::new();
	for version in (before.0 + 1)..=after.0 {
		if let Some(record) = cdc.read(CommitVersion(version)).unwrap() {
			for change in &record.changes {
				if let CdcChange::Delete {
					visible,
					..
				} = change
				{
					deletes.push(*visible);
				}
			}
		}
	}
	deletes
}

fn ringbuffer(with: &str) -> TestEngine {
	let test = TestEngine::new();
	test.admin("CREATE NAMESPACE test");
	test.admin(&format!("CREATE RINGBUFFER test::rb {{ v: int4 }} WITH {{ {with} }}"));
	test.command("INSERT test::rb [{ v: 1 }]");
	test
}

fn deletes_after_push_out(with: &str) -> Vec<bool> {
	let test = ringbuffer(with);
	deletes_committed_by(&test, "INSERT test::rb [{ v: 2 }]")
}

#[test]
fn a_capacity_push_out_is_a_visible_delete_by_default() {
	// Without the key the push-out must stay a visible delete, otherwise every existing downstream flow changes.
	assert_eq!(deletes_after_push_out("capacity: 1"), vec![true]);
}

#[test]
fn an_explicit_visible_push_out_is_a_visible_delete() {
	// Explicit visible must equal the default, otherwise the two spellings diverge.
	assert_eq!(deletes_after_push_out("capacity: 1, eviction: { capacity: visible }"), vec![true]);
}

#[test]
fn a_silent_push_out_is_an_unobserved_delete() {
	// A visible delete here would retract the pushed-out row from every downstream flow.
	assert_eq!(deletes_after_push_out("capacity: 1, eviction: { capacity: silent }"), vec![false]);
}

#[test]
fn a_user_delete_on_a_silent_ringbuffer_stays_visible() {
	// Silent applies to capacity push-out only; a swallowed user delete would leave stale rows downstream.
	let test = ringbuffer("capacity: 2, eviction: { capacity: silent }");
	assert_eq!(deletes_committed_by(&test, "DELETE test::rb FILTER v == 1"), vec![true]);
}

#[test]
fn a_silent_push_out_still_drops_the_row_from_the_ring() {
	// Silent hides the delete from flows only; the ring itself must still hold at most capacity rows.
	let test = ringbuffer("capacity: 1, eviction: { capacity: silent }");
	test.command("INSERT test::rb [{ v: 2 }]");
	let frames = test.query("FROM test::rb");
	assert_eq!(frames[0].row_count(), 1);
}

fn tracked_delete_test_outcome(with: &str) -> String {
	let test = ringbuffer(with);
	test.admin(
		"CREATE TEST test::push_out { INSERT test::rb [{ v: 2 }]; testing::ringbuffers::changed(\"test::rb\") | FILTER { op == \"delete\" } | ASSERT { false } }",
	);
	let frames = test.admin("RUN TESTS test | map { outcome }");
	frames[0].rows().map(|row| row.get::<String>("outcome").unwrap().unwrap()).collect::<Vec<_>>().join(",")
}

#[test]
fn a_capacity_push_out_is_a_tracked_delete_in_the_transaction_by_default() {
	// The assert must fail, otherwise the default push-out no longer reaches in-transaction change readers.
	assert_eq!(tracked_delete_test_outcome("capacity: 1"), "fail");
}

#[test]
fn a_silent_push_out_is_not_a_tracked_delete_in_the_transaction() {
	// A tracked delete here would reach transaction-time readers that silent must hide it from.
	assert_eq!(tracked_delete_test_outcome("capacity: 1, eviction: { capacity: silent }"), "pass");
}
