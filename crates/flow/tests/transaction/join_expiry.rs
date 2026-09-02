// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::row::operator::state::OperatorState;
use reifydb_core::{
	actors::pending::PendingLayers,
	interface::catalog::flow::OperatorId,
	key::operator::{
		keyspace::join::{JoinRowExpiryState as JoinRowExpiry, join_expiry_due_key},
		state::{GroupId, KeyspaceId, OperatorStateKey},
	},
};
use reifydb_flow::transaction::{
	DeferredParams, FlowTransaction,
	deferred::DeferredTransaction,
	join_expiry::{JoinDueEntry, JoinRowExpiryExtension, join_expiry_key},
	substrate::{FlowSubstrate, apply_operator_state, classify_pending, operator_writes},
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_store_operator::types::OperatorWrite;
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::{
	factory::time::at_millis,
	util::hash::Hash128,
	value::{identity::IdentityId, row_number::RowNumber},
};

const NODE: OperatorId = OperatorId(1);

fn group() -> GroupId {
	GroupId::hashed(Hash128(7))
}

fn other() -> GroupId {
	GroupId::hashed(Hash128(8))
}

const LEFT: u8 = 0;

const RIGHT: u8 = 1;

fn deferred(engine: &TestEngine) -> DeferredTransaction {
	let parent = engine.begin_admin(IdentityId::system()).unwrap();
	let version = parent.version();
	DeferredTransaction::new(DeferredParams {
		version,
		pending: PendingLayers::empty(),
		query: Some(parent.multi.begin_query().unwrap()),
		state_query: Some(parent.multi.begin_query().unwrap()),
		catalog: Catalog::testing(),
		interceptors: Interceptors::new(),
		clock: Clock::Mock(MockClock::from_millis(0)),
		substrate: FlowSubstrate::with_dictionary(
			engine.inner().dictionary_allocators(),
			engine.inner().operator_state(),
		),
	})
}

fn arm_in(txn: &mut DeferredTransaction, group: GroupId, side: u8, row_number: u64, millis: u64) {
	txn.join_expiry_arm(NODE, group, side, RowNumber(row_number), at_millis(millis)).unwrap();
}

fn arm(txn: &mut DeferredTransaction, side: u8, row_number: u64, millis: u64) {
	arm_in(txn, group(), side, row_number, millis);
}

fn clear(txn: &mut DeferredTransaction, side: u8, row_number: u64) {
	txn.join_expiry_clear(NODE, group(), side, RowNumber(row_number)).unwrap();
}

fn entry(group: GroupId, side: u8, row_number: u64, millis: u64) -> JoinDueEntry {
	JoinDueEntry {
		at: at_millis(millis),
		group,
		side,
		row_number: RowNumber(row_number),
	}
}

fn commit(engine: &TestEngine, txn: &mut DeferredTransaction) {
	let pending = txn.take_pending();
	apply_operator_state(&engine.inner().operator_state(), &pending);
}

fn commit_writes(engine: &TestEngine, txn: &mut DeferredTransaction) -> Vec<OperatorWrite> {
	let pending = txn.take_pending();
	let store = engine.inner().operator_state();
	let deferred = classify_pending(&store, &pending);
	let writes = operator_writes(&pending, &deferred);
	apply_operator_state(&store, &pending);
	writes
}

fn write_shapes(writes: &[OperatorWrite]) -> Vec<(KeyspaceId, &'static str)> {
	// Pending order is key order, which the two keyspaces interleave, so the shape must be compared sorted.
	let mut shapes: Vec<(KeyspaceId, &'static str)> = writes
		.iter()
		.map(|write| {
			let (key, kind) = match write {
				OperatorWrite::Insert {
					key,
					..
				} => (key, "insert"),
				OperatorWrite::Replace {
					key,
					..
				} => (key, "replace"),
				OperatorWrite::Remove {
					key,
					..
				} => (key, "remove"),
			};
			let (_, keyspace, _) = OperatorStateKey::decode_inner(key.as_slice())
				.expect("an operator write must name a keyspace");
			(keyspace, kind)
		})
		.collect();
	shapes.sort_by_key(|(keyspace, kind)| (keyspace.0, *kind));
	shapes
}

#[test]
fn an_armed_join_expiry_writes_both_its_group_row_and_its_root_due_row() {
	// Either row alone is useless: a deadline nothing scans, or a due entry naming a row that no longer exists.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 42, 5_000);

	commit(&engine, &mut txn);

	let store = engine.inner().operator_state();
	let stored = store
		.get(NODE, &join_expiry_key(group(), LEFT, RowNumber(42)).into_encoded())
		.expect("the group scoped join expiry must be durable");
	assert_eq!(
		JoinRowExpiry::decode_state(&stored).unwrap().at,
		at_millis(5_000),
		"the group scoped row carries the instant the point read answers with"
	);
	assert!(
		store.get(NODE, &join_expiry_due_key(at_millis(5_000), group(), LEFT, RowNumber(42)).into_encoded())
			.is_some(),
		"and the root due row must be keyed by that exact instant"
	);
	assert_eq!(txn.join_expiry_min(NODE).unwrap(), Some(at_millis(5_000)));
}

#[test]
fn a_cleared_join_expiry_takes_its_root_due_row_with_it() {
	// A due row left standing frees a row that is already gone, and re-frees whatever number is minted next.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 42, 5_000);
	commit(&engine, &mut txn);

	clear(&mut txn, LEFT, 42);
	commit(&engine, &mut txn);

	let store = engine.inner().operator_state();
	assert!(
		store.get(NODE, &join_expiry_key(group(), LEFT, RowNumber(42)).into_encoded()).is_none(),
		"the group scoped row must go"
	);
	assert!(
		store.get(NODE, &join_expiry_due_key(at_millis(5_000), group(), LEFT, RowNumber(42)).into_encoded())
			.is_none(),
		"and so must the root due row that pointed at it"
	);
	assert_eq!(txn.join_expiry_min(NODE).unwrap(), None);
}

#[test]
fn a_join_expiry_removed_after_its_row_committed_is_invisible_to_the_minimum() {
	// Without shadowing, the minimum names a join expiry whose row is gone and the timer frees nothing.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 5_000);
	arm(&mut txn, LEFT, 2, 9_000);
	commit(&engine, &mut txn);

	clear(&mut txn, LEFT, 1);

	assert!(
		engine.inner()
			.operator_state()
			.get(NODE, &join_expiry_due_key(at_millis(5_000), group(), LEFT, RowNumber(1)).into_encoded())
			.is_some(),
		"precondition: the removed join expiry's due row is still durable"
	);
	assert_eq!(
		txn.join_expiry_min(NODE).unwrap(),
		Some(at_millis(9_000)),
		"a pending remove must hide its own committed row from the minimum"
	);
	assert_eq!(
		txn.join_expiry_at(NODE, group(), LEFT, RowNumber(1)).unwrap(),
		None,
		"and the point read must agree with it"
	);
}

#[test]
fn an_expiry_moved_later_in_the_batch_wins_over_the_committed_earlier_one() {
	// A stale earlier expiry arms the timer before the row's real deadline and frees a live row: silent data loss.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 5_000);
	commit(&engine, &mut txn);

	arm(&mut txn, LEFT, 1, 20_000);

	assert!(
		engine.inner()
			.operator_state()
			.get(NODE, &join_expiry_due_key(at_millis(5_000), group(), LEFT, RowNumber(1)).into_encoded())
			.is_some(),
		"precondition: the store still carries the earlier due row"
	);
	assert_eq!(
		txn.join_expiry_min(NODE).unwrap(),
		Some(at_millis(20_000)),
		"the batch's own later expiry must win, not the committed earlier one"
	);
	assert_eq!(txn.join_expiry_at(NODE, group(), LEFT, RowNumber(1)).unwrap(), Some(at_millis(20_000)));
}

#[test]
fn a_re_arm_removes_the_due_row_of_the_instant_it_left_behind() {
	// A re-arm that only writes the new due row leaves the old one due first, freeing the row a retention early.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 5_000);
	commit(&engine, &mut txn);

	arm(&mut txn, LEFT, 1, 20_000);
	commit(&engine, &mut txn);

	let store = engine.inner().operator_state();
	assert!(
		store.get(NODE, &join_expiry_due_key(at_millis(5_000), group(), LEFT, RowNumber(1)).into_encoded())
			.is_none(),
		"the due row of the instant that was moved away from must not survive the move"
	);
	assert!(
		store.get(NODE, &join_expiry_due_key(at_millis(20_000), group(), LEFT, RowNumber(1)).into_encoded())
			.is_some(),
		"only the new instant is due"
	);
	assert_eq!(
		txn.join_due_page(NODE, at_millis(10_000), 16, None).unwrap().due,
		Vec::new(),
		"and nothing is due before it"
	);
}

#[test]
fn a_move_later_never_hides_an_untouched_neighbour_that_is_now_the_earliest() {
	// A set must shadow exactly its own row; shadowing wider loses the neighbour that inherited the deadline.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 5_000);
	arm(&mut txn, LEFT, 2, 9_000);
	commit(&engine, &mut txn);

	arm(&mut txn, LEFT, 1, 20_000);

	assert_eq!(
		txn.join_expiry_min(NODE).unwrap(),
		Some(at_millis(9_000)),
		"the untouched neighbour is now the earliest and must be the one that answers"
	);
}

#[test]
fn an_operator_with_more_pending_removes_than_one_page_still_finds_the_true_minimum() {
	// A fixed page bound returns a join expiry the batch already removed, and every extra remove pushes it further.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	for row_number in 1u64..=8 {
		arm(&mut txn, LEFT, row_number, row_number * 1_000);
	}
	commit(&engine, &mut txn);

	for row_number in 1u64..=5 {
		clear(&mut txn, LEFT, row_number);
	}

	assert_eq!(
		txn.join_expiry_min(NODE).unwrap(),
		Some(at_millis(6_000)),
		"five removes must be stepped over to reach the sixth join expiry"
	);
}

#[test]
fn an_operator_whose_every_join_expiry_was_removed_reports_no_minimum_at_all() {
	// A leftover minimum re-arms a timer on an empty operator, which keeps its groups out of reclamation forever.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	for row_number in 1u64..=4 {
		arm(&mut txn, LEFT, row_number, row_number * 1_000);
	}
	commit(&engine, &mut txn);

	for row_number in 1u64..=4 {
		clear(&mut txn, LEFT, row_number);
	}

	assert_eq!(txn.join_expiry_min(NODE).unwrap(), None);
}

#[test]
fn the_due_page_folds_a_batchs_own_due_join_expiry_in_with_the_committed_ones() {
	// A join expiry armed and already due in this batch has no committed row, so the store alone never frees it.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 3_000);
	commit(&engine, &mut txn);

	arm(&mut txn, LEFT, 2, 4_000);
	arm(&mut txn, LEFT, 3, 90_000);

	let page = txn.join_due_page(NODE, at_millis(5_000), 16, None).unwrap();

	assert_eq!(
		page.due,
		vec![entry(group(), LEFT, 2, 4_000), entry(group(), LEFT, 1, 3_000)],
		"the committed and the batch's own due join expiry both come back, newest due first"
	);
	assert!(!page.more);
	assert_eq!(
		txn.join_expiry_min(NODE).unwrap(),
		Some(at_millis(3_000)),
		"and the minimum still reaches past the fire to the earliest of them"
	);
}

#[test]
fn the_due_page_never_returns_a_join_expiry_the_batch_already_removed() {
	// A removed join expiry handed back as due frees its row a second time, against state that is already gone.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 3_000);
	arm(&mut txn, LEFT, 2, 4_000);
	commit(&engine, &mut txn);

	clear(&mut txn, LEFT, 1);

	let page = txn.join_due_page(NODE, at_millis(5_000), 16, None).unwrap();

	assert_eq!(page.due, vec![entry(group(), LEFT, 2, 4_000)]);
	assert!(!page.more);
}

#[test]
fn the_due_page_stops_at_the_fire_and_never_reaches_past_it() {
	// A page that overshoots the watermark frees rows that are still inside their own retention.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 5_000);
	arm(&mut txn, LEFT, 2, 5_001);
	commit(&engine, &mut txn);

	let page = txn.join_due_page(NODE, at_millis(5_000), 16, None).unwrap();

	assert_eq!(
		page.due,
		vec![entry(group(), LEFT, 1, 5_000)],
		"the boundary instant is due and the next one is not"
	);
	assert!(!page.more);
}

#[test]
fn a_due_page_narrower_than_the_operator_resumes_from_its_own_cursor() {
	// Resuming from the start re-reads every remove the batch has piled up, which is what made this quadratic.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	for row_number in 1u64..=6 {
		arm(&mut txn, LEFT, row_number, row_number * 1_000);
	}
	commit(&engine, &mut txn);

	let first = txn.join_due_page(NODE, at_millis(10_000), 2, None).unwrap();
	assert_eq!(first.due, vec![entry(group(), LEFT, 6, 6_000), entry(group(), LEFT, 5, 5_000)]);
	assert!(first.more, "four join expiries are still due and the page must say so");

	let second = txn.join_due_page(NODE, at_millis(10_000), 2, first.resume.as_ref()).unwrap();
	assert_eq!(
		second.due,
		vec![entry(group(), LEFT, 4, 4_000), entry(group(), LEFT, 3, 3_000)],
		"the cursor must resume past the join expiries the first page handed out, with nothing removed"
	);
	assert!(second.more);

	let third = txn.join_due_page(NODE, at_millis(10_000), 2, second.resume.as_ref()).unwrap();
	assert_eq!(third.due, vec![entry(group(), LEFT, 2, 2_000), entry(group(), LEFT, 1, 1_000)]);
	assert!(!third.more, "the last page must not claim another one follows it");
}

#[test]
fn the_due_read_is_answered_by_the_root_time_index_not_by_a_filtered_full_scan() {
	// A read that scanned first and filtered after would spend its whole budget on rows that are not due yet.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	for row_number in 1u64..=3 {
		arm(&mut txn, LEFT, row_number, row_number * 1_000);
	}
	for row_number in 4u64..=60 {
		arm(&mut txn, LEFT, row_number, 900_000 + row_number);
	}
	commit(&engine, &mut txn);

	let page = txn.join_due_page(NODE, at_millis(5_000), 2, None).unwrap();

	assert_eq!(
		page.due,
		vec![entry(group(), LEFT, 3, 3_000), entry(group(), LEFT, 2, 2_000)],
		"the budget must be spent on due rows, never on the fifty seven the index is there to skip"
	);
	assert!(page.more, "and the third due row must still be reachable");
	assert_eq!(
		txn.join_due_page(NODE, at_millis(5_000), 2, page.resume.as_ref()).unwrap().due,
		vec![entry(group(), LEFT, 1, 1_000)]
	);
}

#[test]
fn a_join_expiry_only_ever_written_in_this_batch_is_read_back_without_a_committed_row() {
	// A merge that consulted only the store would report no join expiries for a batch that just armed them.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);

	arm(&mut txn, LEFT, 1, 5_000);

	assert_eq!(txn.join_expiry_min(NODE).unwrap(), Some(at_millis(5_000)));
	assert_eq!(txn.join_expiry_at(NODE, group(), LEFT, RowNumber(1)).unwrap(), Some(at_millis(5_000)));
}

#[test]
fn a_due_page_hands_every_group_back_under_its_own_identity() {
	// One read now spans every group, so an entry attributed to the wrong one frees a live row in that group.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm_in(&mut txn, group(), LEFT, 1, 9_000);
	arm_in(&mut txn, other(), LEFT, 1, 1_000);
	commit(&engine, &mut txn);

	assert_eq!(
		txn.join_expiry_min(NODE).unwrap(),
		Some(at_millis(1_000)),
		"the minimum is the operator's earliest, whichever group holds it"
	);
	assert_eq!(
		txn.join_due_page(NODE, at_millis(10_000), 16, None).unwrap().due,
		vec![entry(group(), LEFT, 1, 9_000), entry(other(), LEFT, 1, 1_000)],
		"and each due entry names the group whose row it frees"
	);
	assert_eq!(
		txn.join_expiry_at(NODE, group(), LEFT, RowNumber(1)).unwrap(),
		Some(at_millis(9_000)),
		"the point read stays group scoped and must not answer with a neighbour's instant"
	);
}

#[test]
fn re_arming_a_join_expiry_the_store_already_holds_classifies_as_a_replace() {
	// An insert claimed for a key the store already carries inflates a census bucket that must stay flat.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 5_000);
	assert_eq!(
		write_shapes(&commit_writes(&engine, &mut txn)),
		vec![(KeyspaceId::JOIN_ROW_EXPIRY, "insert"), (KeyspaceId::JOIN_EXPIRY_DUE, "insert")],
		"precondition: the first arming has no committed row and must insert both of them"
	);

	arm(&mut txn, LEFT, 1, 20_000);

	assert_eq!(
		write_shapes(&commit_writes(&engine, &mut txn)),
		vec![
			(KeyspaceId::JOIN_ROW_EXPIRY, "replace"),
			(KeyspaceId::JOIN_EXPIRY_DUE, "insert"),
			(KeyspaceId::JOIN_EXPIRY_DUE, "remove"),
		],
		"a re-arm replaces the group scoped row in place and moves the due row to its new instant"
	);

	let store = engine.inner().operator_state();
	assert_eq!(
		JoinRowExpiry::decode_state(
			&store.get(NODE, &join_expiry_key(group(), LEFT, RowNumber(1)).into_encoded()).unwrap()
		)
		.unwrap()
		.at,
		at_millis(20_000)
	);
	assert_eq!(
		txn.join_due_page(NODE, at_millis(90_000), 16, None).unwrap().due,
		vec![entry(group(), LEFT, 1, 20_000)],
		"and the operator still owns exactly one join expiry"
	);
}

#[test]
fn a_re_arm_raised_on_a_later_transaction_is_a_replace_too() {
	// Each flow batch runs on its own transaction, so the pre-image must come from the store, not a pending.
	let engine = TestEngine::new();
	let mut first = deferred(&engine);
	arm(&mut first, LEFT, 1, 5_000);
	commit(&engine, &mut first);

	let mut second = deferred(&engine);
	arm(&mut second, LEFT, 1, 20_000);

	assert_eq!(
		write_shapes(&commit_writes(&engine, &mut second)),
		vec![
			(KeyspaceId::JOIN_ROW_EXPIRY, "replace"),
			(KeyspaceId::JOIN_EXPIRY_DUE, "insert"),
			(KeyspaceId::JOIN_EXPIRY_DUE, "remove"),
		],
		"a fresh transaction must still see the committed join expiry as present"
	);
	assert_eq!(
		second.join_due_page(NODE, at_millis(90_000), 16, None).unwrap().due,
		vec![entry(group(), LEFT, 1, 20_000)]
	);
}

#[test]
fn the_two_sides_of_one_row_number_never_share_a_due_row() {
	// Both keyspaces key on side before row, so a side byte that failed to separate them would let one side's clear
	// free the other side's row while its retention still runs.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 3_000);
	arm(&mut txn, RIGHT, 1, 4_000);
	commit(&engine, &mut txn);

	assert_eq!(
		txn.join_due_page(NODE, at_millis(5_000), 16, None).unwrap().due,
		vec![entry(group(), RIGHT, 1, 4_000), entry(group(), LEFT, 1, 3_000)],
		"one row number holds one expiry per side, not one between them"
	);

	clear(&mut txn, LEFT, 1);

	assert_eq!(
		txn.join_due_page(NODE, at_millis(5_000), 16, None).unwrap().due,
		vec![entry(group(), RIGHT, 1, 4_000)],
		"clearing one side must leave the other side's expiry standing"
	);
}

#[test]
fn an_expiry_past_the_high_bit_of_its_nanosecond_encoding_is_not_due_before_one_below_it() {
	// Read as a signed integer the later instant goes negative and sorts earliest, which frees a row whose
	// retention has not run out.
	const BELOW_HIGH_BIT: u64 = 9_223_372_036_854;
	const ABOVE_HIGH_BIT: u64 = 9_223_372_036_855;

	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, BELOW_HIGH_BIT);
	arm(&mut txn, LEFT, 2, ABOVE_HIGH_BIT);
	commit(&engine, &mut txn);

	assert_eq!(
		txn.join_due_page(NODE, at_millis(BELOW_HIGH_BIT), 16, None).unwrap().due,
		vec![entry(group(), LEFT, 1, BELOW_HIGH_BIT)],
		"the instant above the high bit is later than the fire and must stay out of the page"
	);

	assert_eq!(
		txn.join_due_page(NODE, at_millis(ABOVE_HIGH_BIT), 16, None).unwrap().due,
		vec![entry(group(), LEFT, 2, ABOVE_HIGH_BIT), entry(group(), LEFT, 1, BELOW_HIGH_BIT)],
		"raising the fire past the high bit must order the two the way their instants run"
	);
}
