// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	actors::pending::PendingLayers,
	interface::catalog::flow::OperatorId,
	key::operator::state::{GroupId, KeyspaceId, OperatorStateKey},
};
use reifydb_flow::transaction::{
	DeferredParams, FlowTransaction,
	deferred::DeferredTransaction,
	substrate::{FlowSubstrate, apply_operator_state},
	watermark::*,
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::{factory::time::at_millis, value::identity::IdentityId};

const SOURCE_A: OperatorId = OperatorId(1);
const SOURCE_B: OperatorId = OperatorId(2);

fn deferred(engine: &TestEngine, clock: MockClock) -> DeferredTransaction {
	let parent = engine.begin_admin(IdentityId::system()).unwrap();
	let version = parent.version();
	DeferredTransaction::new(DeferredParams {
		version,
		pending: PendingLayers::empty(),
		query: Some(parent.multi.begin_query().unwrap()),
		state_query: Some(parent.multi.begin_query().unwrap()),
		catalog: Catalog::testing(),
		interceptors: Interceptors::new(),
		clock: Clock::Mock(clock),
		substrate: FlowSubstrate::with_dictionary(
			engine.inner().dictionary_allocators(),
			engine.inner().operator_state(),
		),
	})
}

fn commit_pending(engine: &TestEngine, txn: &mut impl FlowTransaction) {
	// Persists the pending writes so a cold instance resolves them as a restarted process would.
	let pending = txn.take_pending();
	apply_operator_state(&engine.inner().operator_state(), &pending);
}

#[test]
fn the_source_watermark_never_moves_backwards() {
	// The per-source watermark is a running max over #time. Late rows arrive with older stamps
	// routinely; dragging it backwards would move derived cutoffs back and re-open horizons
	// that have already sealed.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine, MockClock::from_millis(0));

	SourceWatermarks::advance(SOURCE_A, &mut txn, at_millis(5_000)).unwrap();
	SourceWatermarks::advance(SOURCE_A, &mut txn, at_millis(3_000)).unwrap();

	assert_eq!(SourceWatermarks::source_watermark(SOURCE_A, &mut txn).unwrap(), at_millis(5_000));
}

#[test]
fn the_flow_watermark_tracks_the_slowest_source() {
	// The flow watermark is the min across sources so a fast source can never seal a slow
	// source's state.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine, MockClock::from_millis(0));
	let sources = [SOURCE_A, SOURCE_B];

	SourceWatermarks::advance(SOURCE_A, &mut txn, at_millis(10_000)).unwrap();
	SourceWatermarks::advance(SOURCE_B, &mut txn, at_millis(2_000)).unwrap();
	assert_eq!(SourceWatermarks::flow_watermark(&sources, &mut txn).unwrap(), at_millis(2_000));

	SourceWatermarks::advance(SOURCE_A, &mut txn, at_millis(20_000)).unwrap();
	assert_eq!(
		SourceWatermarks::flow_watermark(&sources, &mut txn).unwrap(),
		at_millis(2_000),
		"the fast source must not advance the flow watermark past the slow one"
	);

	SourceWatermarks::advance(SOURCE_B, &mut txn, at_millis(12_000)).unwrap();
	assert_eq!(SourceWatermarks::flow_watermark(&sources, &mut txn).unwrap(), at_millis(12_000));
}

#[test]
fn a_restart_resumes_from_the_last_advance_not_from_an_earlier_one() {
	// Every advance must reach the store; resuming behind the live value re-seals rows already sealed.
	let engine = TestEngine::new();

	let mut txn = deferred(&engine, MockClock::from_millis(0));
	SourceWatermarks::advance(SOURCE_A, &mut txn, at_millis(5_400)).unwrap();
	SourceWatermarks::advance(SOURCE_A, &mut txn, at_millis(5_900)).unwrap();
	commit_pending(&engine, &mut txn);

	let mut cold_txn = deferred(&engine, MockClock::from_millis(0));
	assert_eq!(
		SourceWatermarks::source_watermark(SOURCE_A, &mut cold_txn).unwrap(),
		at_millis(5_900),
		"a second advance inside the same second must persist too"
	);
}

#[test]
fn an_empty_source_hydrates_to_zero_not_to_now() {
	// A source that has never produced a row hydrates to zero, never to the clock. Hydrating to
	// now would compute cutoffs over the whole backlog on restart and seal state before the
	// first row is processed.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine, MockClock::from_millis(500_000));

	assert_eq!(SourceWatermarks::source_watermark(SOURCE_A, &mut txn).unwrap(), at_millis(0));
	assert_eq!(SourceWatermarks::flow_watermark(&[SOURCE_A], &mut txn).unwrap(), at_millis(0));
}

#[test]
fn the_watermark_is_the_min_merge_of_stamped_arrivals_never_the_clock() {
	// Replay determinism: the flow watermark derives from stamped row time in every domain -
	// processing time is event time over arrival stamps. The clock starts far ahead of the
	// data and moves again mid-test, so a clock read anywhere in the merge shows up as
	// 100_000 or 150_000 where 5_000 is expected.
	let engine = TestEngine::new();
	let clock = MockClock::from_millis(100_000);
	let mut txn = deferred(&engine, clock.clone());
	let sources = [SOURCE_A, SOURCE_B];

	SourceWatermarks::advance(SOURCE_A, &mut txn, at_millis(10_000)).unwrap();
	SourceWatermarks::advance(SOURCE_B, &mut txn, at_millis(5_000)).unwrap();
	assert_eq!(
		SourceWatermarks::flow_watermark(&sources, &mut txn).unwrap(),
		at_millis(5_000),
		"the watermark must be the min-merge of the arrival-derived sources, not the clock"
	);

	clock.advance_millis(50_000);

	assert_eq!(
		SourceWatermarks::flow_watermark(&sources, &mut txn).unwrap(),
		at_millis(5_000),
		"with no new data the watermark must hold however far the clock runs"
	);
}

#[test]
fn the_source_watermark_key_round_trips() {
	// A drifted encoding would make hydration read an absent key and silently restart every
	// watermark at zero, which reads as a healthy cold start rather than as lost state.
	let key = source_watermark_key();
	let (group, keyspace, suffix) =
		OperatorStateKey::decode_inner(key.as_slice()).expect("the key must decode as inner state");

	assert_eq!(group, GroupId::ROOT);
	assert_eq!(keyspace, KeyspaceId::SOURCE_WATERMARK);
	assert!(suffix.is_empty());
}
