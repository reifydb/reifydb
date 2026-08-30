// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::row::operator::state::OperatorState;
use reifydb_core::{
	actors::pending::PendingLayers, interface::catalog::flow::OperatorId, key::operator::state::GroupId,
};
use reifydb_flow::transaction::{
	DeferredParams, FlowTransaction,
	deferred::DeferredTransaction,
	join_expiry::{JoinRowExpiry, JoinRowExpiryExtension, join_expiry_key},
	state::StateExtension,
	substrate::{FlowSubstrate, apply_operator_state, classify_pending, operator_writes},
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_store_operator::types::OperatorWrite;
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

fn arm(txn: &mut DeferredTransaction, side: u8, row_number: u64, millis: u64) {
	let row = JoinRowExpiry {
		at: at_millis(millis),
	}
	.encode_state()
	.unwrap();
	txn.state_set(NODE, &join_expiry_key(GROUP, side, RowNumber(row_number)), row).unwrap();
}

fn clear(txn: &mut DeferredTransaction, side: u8, row_number: u64) {
	txn.state_remove(NODE, &join_expiry_key(GROUP, side, RowNumber(row_number))).unwrap();
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

#[test]
fn an_armed_join_expiry_reaches_the_typed_table_rather_than_the_opaque_key_value_rows() {
	// Routed to the wrong table a join expiry is an opaque blob again, seekable by nothing but a full group scan.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 42, 5_000);

	commit(&engine, &mut txn);

	let store = engine.inner().operator_state();
	assert_eq!(
		store.join_expiry_get(NODE, GROUP, LEFT, RowNumber(42)),
		Some(at_millis(5_000)),
		"the join expiry must be readable by its typed tuple"
	);
	assert_eq!(store.join_expiries_by_time(NODE, GROUP, 8).len(), 1);
	assert_eq!(txn.join_expiry_min(NODE, GROUP).unwrap(), Some(at_millis(5_000)));
}

#[test]
fn a_join_expiry_removed_after_its_row_committed_is_invisible_to_the_minimum() {
	// Without shadowing the minimum names a join expiry whose row is gone, and the group arms a timer freeing
	// nothing.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 5_000);
	arm(&mut txn, LEFT, 2, 9_000);
	commit(&engine, &mut txn);

	clear(&mut txn, LEFT, 1);

	assert!(
		engine.inner().operator_state().join_expiry_get(NODE, GROUP, LEFT, RowNumber(1)).is_some(),
		"precondition: the removed join expiry's row is still in the table"
	);
	assert_eq!(
		txn.join_expiry_min(NODE, GROUP).unwrap(),
		Some(at_millis(9_000)),
		"a pending remove must hide its own committed row from the minimum"
	);
	assert_eq!(
		txn.join_expiry_at(NODE, GROUP, LEFT, RowNumber(1)).unwrap(),
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

	assert_eq!(
		engine.inner().operator_state().join_expiry_get(NODE, GROUP, LEFT, RowNumber(1)),
		Some(at_millis(5_000)),
		"precondition: the table still carries the earlier expiry"
	);
	assert_eq!(
		txn.join_expiry_min(NODE, GROUP).unwrap(),
		Some(at_millis(20_000)),
		"the batch's own later expiry must win, not the committed earlier one"
	);
	assert_eq!(txn.join_expiry_at(NODE, GROUP, LEFT, RowNumber(1)).unwrap(), Some(at_millis(20_000)));
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
		txn.join_expiry_min(NODE, GROUP).unwrap(),
		Some(at_millis(9_000)),
		"the untouched neighbour is now the earliest and must be the one that answers"
	);
}

#[test]
fn a_group_with_more_pending_removes_than_one_page_still_finds_the_true_minimum() {
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
		txn.join_expiry_min(NODE, GROUP).unwrap(),
		Some(at_millis(6_000)),
		"five removes must be stepped over to reach the sixth join expiry"
	);
}

#[test]
fn a_group_whose_every_join_expiry_was_removed_reports_no_minimum_at_all() {
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

	assert_eq!(txn.join_expiry_min(NODE, GROUP).unwrap(), None);
}

#[test]
fn the_due_page_folds_a_batchs_own_due_join_expiry_in_with_the_committed_ones() {
	// A join expiry armed and already due in this batch has no committed row, so the table alone never frees it.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 3_000);
	commit(&engine, &mut txn);

	arm(&mut txn, LEFT, 2, 4_000);
	arm(&mut txn, LEFT, 3, 90_000);

	let page = txn.join_due_page(NODE, GROUP, at_millis(5_000), 16).unwrap();

	assert_eq!(
		page.due,
		vec![(LEFT, RowNumber(1)), (LEFT, RowNumber(2))],
		"both the committed and the batch's own due join expiry must be returned, earliest first"
	);
	assert_eq!(page.next, Some(at_millis(90_000)), "and the next arming is the earliest join expiry past the fire");
	assert!(!page.more);
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

	let page = txn.join_due_page(NODE, GROUP, at_millis(5_000), 16).unwrap();

	assert_eq!(page.due, vec![(LEFT, RowNumber(2))]);
	assert_eq!(page.next, None);
	assert!(!page.more);
}

#[test]
fn a_due_page_narrower_than_the_group_reports_that_it_has_more_to_give() {
	// An under-reporting page leaves due join expiries armed below the watermark, which the wheel refuses to
	// accept.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	for row_number in 1u64..=6 {
		arm(&mut txn, LEFT, row_number, row_number * 1_000);
	}
	commit(&engine, &mut txn);

	let first = txn.join_due_page(NODE, GROUP, at_millis(10_000), 2).unwrap();
	assert_eq!(first.due, vec![(LEFT, RowNumber(1)), (LEFT, RowNumber(2))]);
	assert!(first.more, "four join expiries are still due and the page must say so");

	for (side, row_number) in &first.due {
		clear(&mut txn, *side, row_number.0);
	}

	let second = txn.join_due_page(NODE, GROUP, at_millis(10_000), 2).unwrap();
	assert_eq!(
		second.due,
		vec![(LEFT, RowNumber(3)), (LEFT, RowNumber(4))],
		"the next page must resume past the join expiries the first one consumed"
	);
	assert!(second.more);
}

#[test]
fn a_join_expiry_only_ever_written_in_this_batch_is_read_back_without_a_committed_row() {
	// A merge that consulted only the table would report no join expiries for a batch that just armed them.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);

	arm(&mut txn, LEFT, 1, 5_000);

	assert_eq!(txn.join_expiry_min(NODE, GROUP).unwrap(), Some(at_millis(5_000)));
	assert_eq!(txn.join_expiry_at(NODE, GROUP, LEFT, RowNumber(1)).unwrap(), Some(at_millis(5_000)));
}

#[test]
fn one_groups_join_expiries_never_answer_for_another() {
	// A shadow range that spilled would let a neighbouring group's earlier join expiry free this group's rows.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 9_000);
	let other = join_expiry_key(GroupId(8), LEFT, RowNumber(1));
	let row = JoinRowExpiry {
		at: at_millis(1_000),
	}
	.encode_state()
	.unwrap();
	txn.state_set(NODE, &other, row).unwrap();
	commit(&engine, &mut txn);

	assert_eq!(txn.join_expiry_min(NODE, GROUP).unwrap(), Some(at_millis(9_000)));
	assert_eq!(txn.join_expiry_min(NODE, GroupId(8)).unwrap(), Some(at_millis(1_000)));
}

#[test]
fn re_arming_a_join_expiry_the_store_already_holds_classifies_as_a_replace() {
	// An insert claimed for a tuple the table already carries inflates a census bucket that must stay flat.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	arm(&mut txn, LEFT, 1, 5_000);
	assert!(
		matches!(commit_writes(&engine, &mut txn).as_slice(), [OperatorWrite::JoinExpiryInsert { .. }]),
		"precondition: the first arming has no committed row and must classify as an insert"
	);

	arm(&mut txn, LEFT, 1, 20_000);
	let writes = commit_writes(&engine, &mut txn);

	match writes.as_slice() {
		[
			OperatorWrite::JoinExpiryReplace {
				operator,
				group,
				side,
				row_num,
				at,
			},
		] => {
			assert_eq!(*operator, NODE);
			assert_eq!(*group, GROUP);
			assert_eq!(*side, LEFT);
			assert_eq!(*row_num, RowNumber(1));
			assert_eq!(*at, at_millis(20_000), "the replace must carry the batch's own new expiry");
		}
		other => panic!("a re-arm over a committed join expiry must classify as a replace, got {other:?}"),
	}

	let store = engine.inner().operator_state();
	assert_eq!(store.join_expiry_get(NODE, GROUP, LEFT, RowNumber(1)), Some(at_millis(20_000)));
	assert_eq!(
		store.join_expiries_by_time(NODE, GROUP, 8).len(),
		1,
		"a re-arm moves the one join expiry, it never leaves a second behind"
	);
	assert_eq!(
		store.join_expiry_census().iter().find(|entry| entry.operator == NODE).map(|entry| entry.keys),
		Some(1),
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
	let writes = commit_writes(&engine, &mut second);

	match writes.as_slice() {
		[
			OperatorWrite::JoinExpiryReplace {
				at,
				..
			},
		] => assert_eq!(*at, at_millis(20_000)),
		other => {
			panic!("a fresh transaction must still see the committed join expiry as present, got {other:?}")
		}
	}
	assert_eq!(engine.inner().operator_state().join_expiries_by_time(NODE, GROUP, 8).len(), 1);
}
