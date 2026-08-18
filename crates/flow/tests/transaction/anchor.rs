// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::row::operator::state::OperatorState;
use reifydb_core::{
	actors::pending::PendingLayers, interface::catalog::flow::OperatorId, key::operator_state::GroupId,
};
use reifydb_flow::transaction::{
	DeferredParams, FlowTransaction,
	anchor::{SealAnchor, SealAnchorExtension, anchor_key},
	deferred::DeferredTransaction,
	state::StateExtension,
	substrate::{FlowSubstrate, apply_operator_state},
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::{
	factory::time::at_millis,
	value::{identity::IdentityId, row_number::RowNumber},
};

const NODE: OperatorId = OperatorId(1);

const GROUP: GroupId = GroupId(7);

const LEFT: u8 = 0;

fn deferred(engine: &TestEngine) -> DeferredTransaction {
	let parent = engine.begin_admin(IdentityId::system()).unwrap();
	let version = parent.version();
	DeferredTransaction::new(DeferredParams {
		version,
		pending: PendingLayers::empty(),
		query: parent.multi.begin_query().unwrap(),
		state_query: parent.multi.begin_query().unwrap(),
		catalog: Catalog::testing(),
		interceptors: Interceptors::new(),
		clock: Clock::Mock(MockClock::from_millis(0)),
		substrate: FlowSubstrate {
			operators: Some(engine.inner().operator_state()),
			..FlowSubstrate::default()
		},
	})
}

fn arm(txn: &mut DeferredTransaction, side: u8, row_number: u64, millis: u64) {
	let row = SealAnchor {
		expiry: at_millis(millis),
	}
	.encode_state()
	.unwrap();
	txn.state_set(NODE, &anchor_key(GROUP, side, RowNumber(row_number)), row).unwrap();
}

fn clear(txn: &mut DeferredTransaction, side: u8, row_number: u64) {
	txn.state_remove(NODE, &anchor_key(GROUP, side, RowNumber(row_number))).unwrap();
}

fn commit(engine: &TestEngine, txn: &mut DeferredTransaction) {
	let pending = txn.take_pending();
	apply_operator_state(&engine.inner().operator_state(), &pending);
}

#[test]
fn an_armed_anchor_reaches_the_typed_table_rather_than_the_opaque_key_value_rows() {
	// Routed to the wrong table an anchor is an opaque blob again, seekable by nothing but a full group scan.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 42, 5_000);

	commit(&engine, &mut txn);

	let store = engine.inner().operator_state();
	assert_eq!(
		store.anchor_get(NODE, GROUP, LEFT, RowNumber(42)),
		Some(at_millis(5_000)),
		"the anchor must be readable by its typed tuple"
	);
	assert_eq!(store.anchors_by_expiry(NODE, GROUP, 8).len(), 1);
	assert_eq!(txn.anchor_min(NODE, GROUP).unwrap(), Some(at_millis(5_000)));
}

#[test]
fn an_anchor_removed_after_its_row_committed_is_invisible_to_the_minimum() {
	// Without shadowing the minimum names an anchor whose row is gone, and the group arms a timer sealing nothing.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 5_000);
	arm(&mut txn, LEFT, 2, 9_000);
	commit(&engine, &mut txn);

	clear(&mut txn, LEFT, 1);

	assert!(
		engine.inner().operator_state().anchor_get(NODE, GROUP, LEFT, RowNumber(1)).is_some(),
		"precondition: the removed anchor's row is still in the table"
	);
	assert_eq!(
		txn.anchor_min(NODE, GROUP).unwrap(),
		Some(at_millis(9_000)),
		"a pending remove must hide its own committed row from the minimum"
	);
	assert_eq!(
		txn.anchor_at(NODE, GROUP, LEFT, RowNumber(1)).unwrap(),
		None,
		"and the point read must agree with it"
	);
}

#[test]
fn an_expiry_moved_later_in_the_batch_wins_over_the_committed_earlier_one() {
	// A stale earlier expiry arms the timer before the row's real deadline and seals a live row: silent data loss.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 5_000);
	commit(&engine, &mut txn);

	arm(&mut txn, LEFT, 1, 20_000);

	assert_eq!(
		engine.inner().operator_state().anchor_get(NODE, GROUP, LEFT, RowNumber(1)),
		Some(at_millis(5_000)),
		"precondition: the table still carries the earlier expiry"
	);
	assert_eq!(
		txn.anchor_min(NODE, GROUP).unwrap(),
		Some(at_millis(20_000)),
		"the batch's own later expiry must win, not the committed earlier one"
	);
	assert_eq!(txn.anchor_at(NODE, GROUP, LEFT, RowNumber(1)).unwrap(), Some(at_millis(20_000)));
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
		txn.anchor_min(NODE, GROUP).unwrap(),
		Some(at_millis(9_000)),
		"the untouched neighbour is now the earliest and must be the one that answers"
	);
}

#[test]
fn a_group_with_more_pending_removes_than_one_page_still_finds_the_true_minimum() {
	// A fixed page bound returns an anchor the batch already removed, and every extra remove pushes it further.
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
		txn.anchor_min(NODE, GROUP).unwrap(),
		Some(at_millis(6_000)),
		"five removes must be stepped over to reach the sixth anchor"
	);
}

#[test]
fn a_group_whose_every_anchor_was_removed_reports_no_minimum_at_all() {
	// A leftover minimum re-arms a timer on an empty group, which keeps it out of reclamation forever.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	for row_number in 1u64..=4 {
		arm(&mut txn, LEFT, row_number, row_number * 1_000);
	}
	commit(&engine, &mut txn);

	for row_number in 1u64..=4 {
		clear(&mut txn, LEFT, row_number);
	}

	assert_eq!(txn.anchor_min(NODE, GROUP).unwrap(), None);
}

#[test]
fn the_seal_page_folds_a_batchs_own_due_anchor_in_with_the_committed_ones() {
	// An anchor armed and already due in this batch has no committed row, so the table alone never seals it.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 3_000);
	commit(&engine, &mut txn);

	arm(&mut txn, LEFT, 2, 4_000);
	arm(&mut txn, LEFT, 3, 90_000);

	let page = txn.anchor_seal_page(NODE, GROUP, at_millis(5_000), 16).unwrap();

	assert_eq!(
		page.due,
		vec![(LEFT, RowNumber(1)), (LEFT, RowNumber(2))],
		"both the committed and the batch's own due anchor must be returned, earliest first"
	);
	assert_eq!(page.next, Some(at_millis(90_000)), "and the next arming is the earliest anchor past the fire");
	assert!(!page.more);
}

#[test]
fn the_seal_page_never_returns_an_anchor_the_batch_already_removed() {
	// A removed anchor handed back as due frees its row a second time, against state that is already gone.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 3_000);
	arm(&mut txn, LEFT, 2, 4_000);
	commit(&engine, &mut txn);

	clear(&mut txn, LEFT, 1);

	let page = txn.anchor_seal_page(NODE, GROUP, at_millis(5_000), 16).unwrap();

	assert_eq!(page.due, vec![(LEFT, RowNumber(2))]);
	assert_eq!(page.next, None);
	assert!(!page.more);
}

#[test]
fn a_seal_page_narrower_than_the_group_reports_that_it_has_more_to_give() {
	// An under-reporting page leaves due anchors armed below the watermark, which the wheel refuses to accept.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	for row_number in 1u64..=6 {
		arm(&mut txn, LEFT, row_number, row_number * 1_000);
	}
	commit(&engine, &mut txn);

	let first = txn.anchor_seal_page(NODE, GROUP, at_millis(10_000), 2).unwrap();
	assert_eq!(first.due, vec![(LEFT, RowNumber(1)), (LEFT, RowNumber(2))]);
	assert!(first.more, "four anchors are still due and the page must say so");

	for (side, row_number) in &first.due {
		clear(&mut txn, *side, row_number.0);
	}

	let second = txn.anchor_seal_page(NODE, GROUP, at_millis(10_000), 2).unwrap();
	assert_eq!(
		second.due,
		vec![(LEFT, RowNumber(3)), (LEFT, RowNumber(4))],
		"the next page must resume past the anchors the first one consumed"
	);
	assert!(second.more);
}

#[test]
fn an_anchor_only_ever_written_in_this_batch_is_read_back_without_a_committed_row() {
	// A merge that consulted only the table would report no anchors for a batch that just armed them.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);

	arm(&mut txn, LEFT, 1, 5_000);

	assert_eq!(txn.anchor_min(NODE, GROUP).unwrap(), Some(at_millis(5_000)));
	assert_eq!(txn.anchor_at(NODE, GROUP, LEFT, RowNumber(1)).unwrap(), Some(at_millis(5_000)));
}

#[test]
fn one_groups_anchors_never_answer_for_another() {
	// A shadow range that spilled would let a neighbouring group's earlier anchor seal this group's rows.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 9_000);
	let other = anchor_key(GroupId(8), LEFT, RowNumber(1));
	let row = SealAnchor {
		expiry: at_millis(1_000),
	}
	.encode_state()
	.unwrap();
	txn.state_set(NODE, &other, row).unwrap();
	commit(&engine, &mut txn);

	assert_eq!(txn.anchor_min(NODE, GROUP).unwrap(), Some(at_millis(9_000)));
	assert_eq!(txn.anchor_min(NODE, GroupId(8)).unwrap(), Some(at_millis(1_000)));
}
