// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator_state::GroupId,
};
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	persistent::OperatorPersistentTier,
	sqlite::SqliteOperatorStorage,
	store::OperatorStore,
	types::{OperatorBatch, OperatorSealAnchor, OperatorWrite},
};
use reifydb_value::value::{datetime::DateTime, duration::Duration, row_number::RowNumber};

const OP_A: OperatorId = OperatorId(1);
const OP_B: OperatorId = OperatorId(2);
const GROUP_A: GroupId = GroupId(10);
const GROUP_B: GroupId = GroupId(11);
const SIDE: u8 = 0;
const FLOW: FlowId = FlowId(7);

fn flushed_store() -> (OperatorStore, SqliteOperatorStorage, SqliteTempPathGuard) {
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock.clone());
	let spawner = actor_system.spawner();
	let (storage, guard) = SqliteOperatorStorage::in_memory();
	let store = OperatorStore::standard(OperatorStoreConfig {
		commit: Default::default(),
		persistent: Some(OperatorPersistentConfig::opened(OperatorPersistentTier::Sqlite(storage.clone()))
			.flush_interval(Duration::from_hours_const(1))),
		spawner,
		clock,
	});
	(store, storage, guard)
}

fn store_at(config: SqliteConfig) -> OperatorStore {
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock.clone());
	let spawner = actor_system.spawner();
	OperatorStore::standard(OperatorStoreConfig {
		commit: Default::default(),
		persistent: Some(OperatorPersistentConfig::sqlite(config).flush_interval(Duration::from_hours_const(1))),
		spawner,
		clock,
	})
}

fn key(suffix: u8) -> EncodedKey {
	let mut bytes = 7u64.to_be_bytes().to_vec();
	bytes.push(0x10);
	bytes.push(suffix);
	EncodedKey::new(bytes)
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn body(row: &EncodedPodRow) -> String {
	String::from_utf8(row.body().to_vec()).expect("test bodies are utf8")
}

fn bodies(batch: &OperatorBatch) -> Vec<String> {
	batch.items.iter().map(|(_, row)| body(row)).collect()
}

fn scan(store: &OperatorStore, operator: OperatorId) -> OperatorBatch {
	store.range_batch(operator, EncodedKeyRange::new(Bound::Unbounded, Bound::Unbounded), 64)
}

#[test]
fn a_buffered_write_shadows_the_flushed_row_for_the_same_key() {
	let (store, storage, _guard) = flushed_store();
	storage.set(OP_A, key(1), row("durable"));

	store.set(OP_A, key(1), row("buffered"));

	let found = store.get(OP_A, &key(1)).expect("the key exists in both layers");
	assert_eq!(
		body(&found),
		"buffered",
		"the buffer holds the newer value; falling through to sqlite serves the pre-flush row and \
		 silently loses the write"
	);

	let batch = scan(&store, OP_A);
	assert_eq!(
		bodies(&batch),
		vec!["buffered".to_string()],
		"the merge must collapse the two layers onto one key, otherwise the caller sees the key twice \
		 with conflicting payloads"
	);
}

#[test]
fn a_buffered_tombstone_hides_the_flushed_row_from_every_read() {
	let (store, storage, _guard) = flushed_store();
	storage.set(OP_A, key(1), row("durable"));

	store.remove(OP_A, &key(1));

	assert!(
		store.get(OP_A, &key(1)).is_none(),
		"a removed key must read as missing; reading through to sqlite resurrects the row until the \
		 next flush"
	);
	assert!(
		!store.contains(OP_A, &key(1)),
		"contains must honour the tombstone too, otherwise an operator branches on a key it just deleted"
	);
	assert!(
		scan(&store, OP_A).items.is_empty(),
		"the scan must drop the tombstoned key, otherwise a deleted row is replayed downstream"
	);
	assert!(
		storage.get(OP_A, &key(1)).is_some(),
		"the row is only masked, not deleted; the flusher still has to erase it"
	);
}

#[test]
fn paging_interleaved_layers_yields_every_key_once_in_order() {
	let (store, storage, _guard) = flushed_store();
	for suffix in [1u8, 3, 5] {
		storage.set(OP_A, key(suffix), row(&format!("durable-{suffix}")));
	}
	for suffix in [2u8, 4, 6] {
		store.set(OP_A, key(suffix), row(&format!("buffered-{suffix}")));
	}

	let mut seen: Vec<String> = Vec::new();
	let mut flags: Vec<bool> = Vec::new();
	let mut start = Bound::Unbounded;
	loop {
		let batch = store.range_batch(OP_A, EncodedKeyRange::new(start, Bound::Unbounded), 2);
		flags.push(batch.has_more);
		let last = batch.items.last().map(|(key, _)| key.clone());
		seen.extend(bodies(&batch));
		match last {
			Some(key) if batch.has_more => start = Bound::Excluded(key),
			_ => break,
		}
	}

	assert_eq!(
		seen,
		vec![
			"durable-1".to_string(),
			"buffered-2".to_string(),
			"durable-3".to_string(),
			"buffered-4".to_string(),
			"durable-5".to_string(),
			"buffered-6".to_string(),
		],
		"the cursor must interleave the two layers in key order and emit each key exactly once; a \
		 mis-advanced page boundary shows up here as a duplicate or a hole"
	);
	assert_eq!(
		flags,
		vec![true, true, false],
		"has_more must stay honest across page boundaries: a false in the middle truncates the scan, a \
		 true at the end makes the caller loop forever"
	);
}

#[test]
fn a_page_whose_flushed_rows_are_all_hidden_keeps_pulling_until_the_scan_is_exhausted() {
	let (store, storage, _guard) = flushed_store();
	for suffix in 1u8..=6 {
		storage.set(OP_A, key(suffix), row(&format!("durable-{suffix}")));
	}
	for suffix in 1u8..=4 {
		store.remove(OP_A, &key(suffix));
	}

	let batch = store.range_batch(OP_A, EncodedKeyRange::new(Bound::Unbounded, Bound::Unbounded), 2);

	assert_eq!(
		bodies(&batch),
		vec!["durable-5".to_string(), "durable-6".to_string()],
		"the first two sqlite pages are entirely tombstoned, so the cursor has to pull a third; stopping \
		 at the first page returns an empty batch and the caller concludes the operator has no state"
	);
	assert!(
		!batch.has_more,
		"the scan reached the end of both layers, and a stray has_more sends the caller round again \
		 for a page that does not exist"
	);
}

#[test]
fn a_scan_stays_inside_its_operator_when_a_neighbour_holds_the_same_keys() {
	let (store, storage, _guard) = flushed_store();
	for suffix in [1u8, 2, 3] {
		storage.set(OP_A, key(suffix), row(&format!("a-durable-{suffix}")));
		storage.set(OP_B, key(suffix), row(&format!("b-durable-{suffix}")));
		store.set(OP_A, key(suffix + 10), row(&format!("a-buffered-{suffix}")));
		store.set(OP_B, key(suffix + 10), row(&format!("b-buffered-{suffix}")));
	}

	let batch = scan(&store, OP_A);

	assert_eq!(
		bodies(&batch),
		vec![
			"a-durable-1".to_string(),
			"a-durable-2".to_string(),
			"a-durable-3".to_string(),
			"a-buffered-1".to_string(),
			"a-buffered-2".to_string(),
			"a-buffered-3".to_string(),
		],
		"a scan that leaks the neighbouring operator feeds one operator's state into another's compute"
	);
}

#[test]
fn a_due_scan_fills_its_page_even_when_buffered_removals_hide_the_earliest_anchors() {
	let (store, storage, _guard) = flushed_store();
	for (row_number, millis) in [(1u64, 100u64), (2, 200), (3, 300), (4, 400), (5, 500)] {
		storage.anchor_set(OP_A, GROUP_A, SIDE, RowNumber(row_number), DateTime::from_millis(millis));
	}

	store.anchor_remove(OP_A, GROUP_A, SIDE, RowNumber(1));
	store.anchor_remove(OP_A, GROUP_A, SIDE, RowNumber(2));

	let due = store.anchors_due(OP_A, GROUP_A, DateTime::from_millis(1_000), 3);

	assert_eq!(
		due,
		vec![
			OperatorSealAnchor {
				side: SIDE,
				row_number: RowNumber(3),
				expiry: DateTime::from_millis(300),
			},
			OperatorSealAnchor {
				side: SIDE,
				row_number: RowNumber(4),
				expiry: DateTime::from_millis(400),
			},
			OperatorSealAnchor {
				side: SIDE,
				row_number: RowNumber(5),
				expiry: DateTime::from_millis(500),
			},
		],
		"the buffered removals sit at the front of the expiry order, so a sqlite fetch sized to the limit \
		 returns three rows of which two are hidden; the seal loop would then drain one anchor per tick \
		 and never catch up"
	);
}

#[test]
fn a_buffered_expiry_reorders_the_flushed_anchors() {
	let (store, storage, _guard) = flushed_store();
	storage.anchor_set(OP_A, GROUP_A, SIDE, RowNumber(1), DateTime::from_millis(100));
	storage.anchor_set(OP_A, GROUP_A, SIDE, RowNumber(2), DateTime::from_millis(200));
	storage.anchor_set(OP_A, GROUP_A, SIDE, RowNumber(3), DateTime::from_millis(300));

	store.anchor_set(OP_A, GROUP_A, SIDE, RowNumber(3), DateTime::from_millis(50));

	let anchors = store.anchors_by_expiry(OP_A, GROUP_A, 3);

	assert_eq!(
		anchors.iter().map(|anchor| anchor.row_number).collect::<Vec<RowNumber>>(),
		vec![RowNumber(3), RowNumber(1), RowNumber(2)],
		"the buffered expiry must supersede the flushed one and re-sort the scan; keeping the sqlite \
		 ordering seals the rows in the wrong order and drops the moved anchor to the back of the page"
	);
	assert_eq!(
		anchors[0].expiry,
		DateTime::from_millis(50),
		"the served expiry must be the buffered one, not the stale flushed deadline"
	);
	assert_eq!(
		store.anchor_get(OP_A, GROUP_A, SIDE, RowNumber(3)),
		Some(DateTime::from_millis(50)),
		"a point read of an overridden anchor must agree with the scan"
	);
}

#[test]
fn a_buffered_state_drop_masks_sqlite_while_later_writes_survive() {
	let (store, storage, _guard) = flushed_store();
	storage.set(OP_A, key(1), row("durable"));
	storage.set(OP_B, key(1), row("neighbour"));
	storage.anchor_set(OP_A, GROUP_A, SIDE, RowNumber(1), DateTime::from_millis(100));

	store.drop_operator_state(OP_A);

	assert!(
		store.get(OP_A, &key(1)).is_none(),
		"the drop must mask the rows it has not yet erased, otherwise a recreated operator reads the \
		 dead operator's state"
	);
	assert!(!store.contains(OP_A, &key(1)));
	assert!(scan(&store, OP_A).items.is_empty(), "a scan must be masked by the drop just like a point read");
	assert!(
		store.anchors_by_expiry(OP_A, GROUP_A, 8).is_empty(),
		"dropping operator state takes its anchors with it, so the anchor scan must be masked too"
	);
	assert!(
		storage.get(OP_A, &key(1)).is_some(),
		"the drop is still only buffered; the mask is what makes it look applied"
	);
	assert!(
		store.get(OP_B, &key(1)).is_some(),
		"the mask is scoped to one operator, otherwise one drop blinds the whole store"
	);

	store.set(OP_A, key(2), row("after"));

	let found = store.get(OP_A, &key(2)).expect("a write recorded after the drop must be visible");
	assert_eq!(
		body(&found),
		"after",
		"the drop clears the buffer when recorded, so what is left in it was written afterwards and \
		 must survive both the mask and the flush"
	);
	assert_eq!(
		bodies(&scan(&store, OP_A)),
		vec!["after".to_string()],
		"the scan must serve post-drop writes while still hiding the flushed rows"
	);
}

#[test]
fn a_checkpoint_is_served_from_the_buffer_before_the_flush_and_from_sqlite_after_it() {
	let (store, storage, _guard) = flushed_store();

	store.checkpoint_set(FLOW, CommitVersion(41));

	assert!(
		storage.checkpoint_get(FLOW).is_none(),
		"the checkpoint must not touch sqlite on the commit path; a synchronous write here is what \
		 the commit buffer exists to remove"
	);
	assert_eq!(
		store.checkpoint_get(FLOW),
		Some(CommitVersion(41)),
		"the store must serve the buffered checkpoint, otherwise a restart-free reader sees the older \
		 durable version and the flow re-runs work it already committed"
	);

	store.checkpoint_set(FLOW, CommitVersion(42));
	assert_eq!(
		store.checkpoint_get(FLOW),
		Some(CommitVersion(42)),
		"a later checkpoint must supersede the buffered one rather than queue behind it"
	);

	assert!(store.flush_pending_blocking(), "the checkpoint batch must reach the flusher");

	assert_eq!(
		storage.checkpoint_get(FLOW),
		Some(CommitVersion(42)),
		"the flush must make the latest checkpoint durable; a lost flush silently rewinds the flow on \
		 the next boot"
	);
	assert_eq!(
		store.checkpoint_get(FLOW),
		Some(CommitVersion(42)),
		"with the buffer drained the read falls through to sqlite and must agree with what was written"
	);
}

#[test]
fn the_memory_tier_serves_the_checkpoint_it_was_given_and_forgets_a_deleted_one() {
	let store = OperatorStore::testing_memory();

	store.checkpoint_set(FLOW, CommitVersion(9));
	assert_eq!(
		store.checkpoint_get(FLOW),
		Some(CommitVersion(9)),
		"the memory tier is the only home the checkpoint has in memory mode"
	);

	assert!(store.flush_pending_blocking(), "the memory tier has nothing to flush and must still succeed");
	assert_eq!(
		store.checkpoint_get(FLOW),
		Some(CommitVersion(9)),
		"a flush must not discard the checkpoint on a tier that never persists it"
	);

	store.checkpoint_delete(FLOW);
	assert!(
		store.checkpoint_get(FLOW).is_none(),
		"a dropped flow must leave no checkpoint behind, otherwise a flow recreated under the same id \
		 resumes from a stranger's version"
	);
}

#[test]
fn a_checkpoint_delete_masks_the_flushed_row_and_then_erases_it() {
	let (store, storage, _guard) = flushed_store();
	store.checkpoint_set(FLOW, CommitVersion(42));
	assert!(store.flush_pending_blocking(), "the checkpoint must be durable before the delete is tested");

	store.checkpoint_delete(FLOW);

	assert!(
		store.checkpoint_get(FLOW).is_none(),
		"the buffered tombstone must hide the durable row; reading through to sqlite resumes a \
		 dropped flow from the version it died at"
	);
	assert!(
		storage.checkpoint_get(FLOW).is_some(),
		"the delete is only buffered so far; the mask is what makes it look applied"
	);

	assert!(store.flush_pending_blocking(), "the checkpoint tombstone must reach the flusher");

	assert!(
		storage.checkpoint_get(FLOW).is_none(),
		"the tombstone must execute as a DELETE, otherwise the row outlives the flow and pins CDC \
		 retention forever"
	);
	assert!(store.checkpoint_get(FLOW).is_none(), "the drained buffer must not resurrect the deleted row");
}

#[test]
fn a_freshly_opened_store_boots_from_the_flushed_checkpoint_and_the_state_it_rode_with() {
	let (config, _guard) = SqliteConfig::in_memory();

	let store = store_at(config.clone());
	store.apply_batch_with_checkpoints(
		&[OperatorWrite::Set {
			operator: OP_A,
			key: key(1),
			row: row("state"),
		}],
		&[(FLOW, CommitVersion(77))],
		&[],
	);
	assert!(store.flush_pending_blocking(), "only a flushed checkpoint can be booted from");

	let booted = store_at(config);

	assert_eq!(
		booted.checkpoint_get(FLOW),
		Some(CommitVersion(77)),
		"boot must read the durable checkpoint out of the operator store; falling back to the seed \
		 replays every slice the flow ever consumed"
	);
	let state = booted.get(OP_A, &key(1)).expect("the state of the checkpointed slice must be durable too");
	assert_eq!(
		body(&state),
		"state",
		"the checkpoint may never be durable without the state it was earned by, otherwise the flow \
		 resumes past state it never wrote"
	);
}

#[test]
fn the_durable_floor_is_the_smallest_checkpoint_across_flows() {
	let (store, _storage, _guard) = flushed_store();
	store.checkpoint_set(FlowId(1), CommitVersion(90));
	store.checkpoint_set(FlowId(2), CommitVersion(10));
	store.checkpoint_set(FlowId(3), CommitVersion(50));
	assert!(store.flush_pending_blocking(), "only a flushed checkpoint contributes to the floor");

	assert_eq!(
		store.checkpoint_floor(),
		Some(CommitVersion(10)),
		"the floor must be the minimum across every flow; taking the newest or the first-seen row lets \
		 retention delete cdc entries flow 2 has not consumed yet"
	);
}

#[test]
fn the_floor_ignores_a_buffered_checkpoint_until_the_flush_makes_it_durable() {
	let (store, _storage, _guard) = flushed_store();
	store.checkpoint_set(FLOW, CommitVersion(10));
	assert!(store.flush_pending_blocking(), "the older checkpoint has to be durable before this is a test");

	store.checkpoint_set(FLOW, CommitVersion(80));

	assert_eq!(
		store.checkpoint_floor(),
		Some(CommitVersion(10)),
		"the floor must stay at the flushed version while the newer one is still buffered; advancing \
		 it here lets retention reap versions 10..80, which a crash would send the flow straight back to"
	);
	assert_eq!(
		store.checkpoint_get(FLOW),
		Some(CommitVersion(80)),
		"the layered read still serves the buffered value; only the retention floor is deliberately \
		 behind it"
	);

	assert!(store.flush_pending_blocking(), "the newer checkpoint must reach the flusher");

	assert_eq!(
		store.checkpoint_floor(),
		Some(CommitVersion(80)),
		"once the checkpoint is durable the floor must advance, otherwise cdc retention is pinned at \
		 the first checkpoint the database ever wrote and nothing is reaped again"
	);
}

#[test]
fn an_empty_checkpoint_table_yields_no_floor_and_therefore_pins_nothing() {
	let (store, _storage, _guard) = flushed_store();

	assert!(
		store.checkpoint_floor().is_none(),
		"no rows means no pin; returning a zero floor here would stop cdc truncation forever on a \
		 database that runs no flows"
	);
	assert!(
		OperatorStore::testing_memory().checkpoint_floor().is_none(),
		"the memory tier must agree, otherwise memory-mode retention behaves differently from disk"
	);
}

#[test]
fn the_floor_covers_a_flow_whose_only_checkpoint_is_still_buffered() {
	let (store, _storage, _guard) = flushed_store();
	store.checkpoint_set(FlowId(1), CommitVersion(100));
	assert!(store.flush_pending_blocking(), "the older flow needs a durable row to raise the floor");

	store.checkpoint_set(FlowId(2), CommitVersion(5));

	assert_eq!(
		store.checkpoint_floor(),
		Some(CommitVersion(5)),
		"flow 2 has no durable row yet, so a floor taken from sqlite alone reports 100 and lets \
		 retention reap versions 5..100; on the next boot flow 2 resumes from the migration base and \
		 the cdc entries it needs are gone"
	);
}

#[test]
fn the_checkpoint_list_shows_buffered_writes_and_hides_buffered_deletes() {
	let (store, _storage, _guard) = flushed_store();
	store.checkpoint_set(FlowId(1), CommitVersion(1));
	store.checkpoint_set(FlowId(2), CommitVersion(2));
	assert!(store.flush_pending_blocking(), "the durable half of the merge must exist first");

	store.checkpoint_delete(FlowId(1));
	store.checkpoint_set(FlowId(3), CommitVersion(3));

	assert_eq!(
		store.checkpoint_list(),
		vec![FlowId(2), FlowId(3)],
		"the list must merge the buffer over sqlite: dropping the buffered flow 3 leaves an orphan the \
		 bootstrap reap can never see, and keeping the tombstoned flow 1 makes the reap delete a row \
		 that is already gone"
	);
}

#[test]
fn the_memory_tier_reports_the_same_floor_and_list_as_the_sqlite_tier() {
	let store = OperatorStore::testing_memory();
	store.checkpoint_set(FlowId(1), CommitVersion(30));
	store.checkpoint_set(FlowId(2), CommitVersion(7));

	assert_eq!(
		store.checkpoint_floor(),
		Some(CommitVersion(7)),
		"the memory tier floor must be the minimum across flows, not the last one written"
	);
	assert_eq!(store.checkpoint_list(), vec![FlowId(1), FlowId(2)]);

	store.checkpoint_delete(FlowId(2));

	assert_eq!(
		store.checkpoint_floor(),
		Some(CommitVersion(30)),
		"a deleted flow must stop contributing to the floor, otherwise a dropped flow pins retention \
		 for the life of the process"
	);
}

#[test]
fn a_group_anchor_drop_does_not_mask_a_sibling_group() {
	let (store, storage, _guard) = flushed_store();
	storage.anchor_set(OP_A, GROUP_A, SIDE, RowNumber(1), DateTime::from_millis(100));
	storage.anchor_set(OP_A, GROUP_B, SIDE, RowNumber(2), DateTime::from_millis(200));
	storage.set(OP_A, key(1), row("durable"));

	store.anchors_remove_group(OP_A, GROUP_A);

	assert!(
		store.anchors_by_expiry(OP_A, GROUP_A, 8).is_empty(),
		"the dropped group must read empty even though its rows are still in sqlite"
	);
	assert!(store.anchor_get(OP_A, GROUP_A, SIDE, RowNumber(1)).is_none());
	assert_eq!(
		store.anchors_by_expiry(OP_A, GROUP_B, 8),
		vec![OperatorSealAnchor {
			side: SIDE,
			row_number: RowNumber(2),
			expiry: DateTime::from_millis(200),
		}],
		"a sibling group keeps its anchors; masking by operator alone would disarm every timer the \
		 operator owns"
	);
	assert!(store.get(OP_A, &key(1)).is_some(), "an anchor drop must never mask operator state");
}

#[test]
fn a_zero_length_row_stays_present_in_the_buffer_and_never_reads_as_absent() {
	// A pod row with no body is zero bytes end to end, and marker keyspaces store exactly that; if the
	// buffer collapsed BufferedState::Row(empty) onto Absent, every marker would vanish on the write path.
	let (store, _storage, _guard) = flushed_store();

	store.set(OP_A, key(1), EncodedPodRow::new(&[]));

	let found = store.get(OP_A, &key(1)).expect("a zero-length row is present, not absent");
	assert_eq!(found.len(), 0, "the row must round-trip at its written width");
	assert!(store.contains(OP_A, &key(1)));
	assert!(store.get(OP_A, &key(2)).is_none(), "an unwritten key stays absent, which is the other case");

	store.remove(OP_A, &key(1));
	assert!(store.get(OP_A, &key(1)).is_none(), "and a tombstone still reads absent");
	assert!(!store.contains(OP_A, &key(1)));
}

#[test]
fn a_zero_length_row_survives_the_sqlite_blob_column_distinctly_from_absence() {
	// sqlite3_bind_zeroblob stores a zero-length BLOB rather than NULL, and a zero-length blob reads back
	// as an empty slice; a driver that mapped it to NULL would turn every flushed marker into a missing key.
	let (_store, storage, _guard) = flushed_store();

	storage.set(OP_A, key(1), EncodedPodRow::new(&[]));

	let found = storage.get(OP_A, &key(1)).expect("a flushed zero-length row is present, not absent");
	assert_eq!(found.len(), 0);
	assert!(storage.contains(OP_A, &key(1)));
	assert!(storage.get(OP_A, &key(2)).is_none());

	let scanned = storage.range_batch(OP_A, EncodedKeyRange::new(Bound::Unbounded, Bound::Unbounded), 64);
	assert_eq!(scanned.items.len(), 1, "the range scan must yield the empty row, not skip it");
	assert_eq!(scanned.items[0].1.len(), 0);
}
