// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, BTreeSet};

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	interface::{
		catalog::{
			id::QueueId,
			queue::{Queue, QueueItemState, QueueItemStatus, decode_queue_item_state},
		},
		store::SingleVersionRange,
	},
	key::queue::{QueueDueKey, QueueItemStateKey, QueueKeyActiveKey},
};
use reifydb_engine::queue::hydrate::hydrate_queues;
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::{queue::scheduling::queue_partition_counters, transaction::Transaction};
use reifydb_value::value::{Value, frame::frame::Frame, partition::Partition, row_number::RowNumber};

const BY_REGION: &str =
	"CREATE QUEUE test::jobs { id: int4, region: utf8 } WITH { partition: { by: { region } }, fifo: {} }";

const ORDERED_BY_REGION: &str = "CREATE QUEUE test::jobs { id: int4, region: utf8, tenant: utf8 } WITH { partition: { by: { region } }, fifo: { ordered_by: tenant } }";

fn engine_with_queue(declaration: &str) -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(declaration);
	t
}

fn queue_id(t: &TestEngine) -> QueueId {
	let catalog = t.inner().catalog();
	let mut query_txn = t.inner().begin_query(TestEngine::identity()).unwrap();
	let mut txn = Transaction::Query(&mut query_txn);
	let namespace = catalog.find_namespace_by_name(&mut txn, "test").unwrap().unwrap();
	catalog.find_queue_by_name(&mut txn, namespace.id(), "jobs").unwrap().unwrap().id
}

fn states(t: &TestEngine, queue: QueueId) -> BTreeMap<RowNumber, (Partition, QueueItemState)> {
	let store = t.inner().single().read_store();
	SingleVersionRange::range_batch(&store, QueueItemStateKey::queue_scan(queue).encode(), 1024)
		.unwrap()
		.items
		.iter()
		.map(|item| {
			let key = QueueItemStateKey::decode(&item.key).unwrap();
			(key.row, (key.partition, decode_queue_item_state(EncodedPodRow::view(&item.bytes)).unwrap()))
		})
		.collect()
}

fn claim(t: &TestEngine, worker: &str, max_n: u32) -> Vec<Frame> {
	t.command(&format!(r#"CALL queue::claim("{worker}", "test::jobs", {max_n}, duration::seconds(30))"#))
}

fn column(frames: &[Frame], name: &str) -> Vec<Value> {
	let frame = frames.first().expect("claim must always return a frame");
	let column = frame.column(name).unwrap().unwrap();
	(0..frame.row_count()).map(|i| column.get_value(i)).collect()
}

fn items(frames: &[Frame]) -> Vec<u64> {
	column(frames, "item")
		.into_iter()
		.map(|v| match v {
			Value::Uint8(n) => n,
			other => panic!("item must be Uint8, got {other:?}"),
		})
		.collect()
}

fn ids(frames: &[Frame]) -> BTreeSet<i32> {
	column(frames, "id")
		.into_iter()
		.map(|v| match v {
			Value::Int4(n) => n,
			other => panic!("id must be Int4, got {other:?}"),
		})
		.collect()
}

fn ack_all(t: &TestEngine, frames: &[Frame]) {
	for token in column(frames, "token") {
		match token {
			Value::Utf8(token) => t.command(&format!(r#"CALL queue::ack("{token}")"#)),
			other => panic!("token must be Utf8, got {other:?}"),
		};
	}
}

#[test]
fn test_two_partitions_keep_their_scheduling_state_apart() {
	// If two partition values shared a FIFO, their due and counter rows would land on one key.
	let t = engine_with_queue(BY_REGION);
	t.command(r#"INSERT test::jobs [{ id: 1, region: "eu" }, { id: 2, region: "us" }, { id: 3, region: "eu" }]"#);
	let queue = queue_id(&t);

	let placed = states(&t, queue);
	assert_eq!(placed[&RowNumber(1)].0, placed[&RowNumber(3)].0, "one value must stay in one FIFO");
	assert_ne!(placed[&RowNumber(1)].0, placed[&RowNumber(2)].0, "two values must not share a FIFO");

	let eu = placed[&RowNumber(1)].0;
	let us = placed[&RowNumber(2)].0;
	let store = t.inner().single().read_store();
	let due = |partition| {
		SingleVersionRange::range_batch(&store, QueueDueKey::partition_scan(queue, partition).encode(), 16)
			.unwrap()
			.items
			.len()
	};
	assert_eq!((due(eu), due(us)), (2, 1), "each FIFO must index only its own due items");

	let mut counters = queue_partition_counters(t.inner().single(), queue).unwrap();
	counters.sort_by_key(|(_, counters)| counters.depth);
	assert_eq!(counters.len(), 2);
	assert_eq!((counters[0].0, counters[0].1.depth), (us, 1));
	assert_eq!((counters[1].0, counters[1].1.depth), (eu, 2));
}

#[test]
fn test_an_unpartitioned_queue_stays_one_fifo_across_every_transition() {
	// Any key landing outside the one partition would strand work where claim and drop never look.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }]");
	let queue = queue_id(&t);

	let counters = queue_partition_counters(t.inner().single(), queue).unwrap();
	assert_eq!(counters.len(), 1, "an unpartitioned queue must have exactly one counter row");
	assert_eq!(counters[0].0, Queue::UNPARTITIONED);
	assert_eq!(counters[0].1.depth, 5);

	let frames = claim(&t, "w", 2);
	assert_eq!(items(&frames), vec![1, 2], "one FIFO hands out items in enqueue order");
	for (partition, _) in states(&t, queue).values() {
		assert_eq!(*partition, Queue::UNPARTITIONED);
	}
}

#[test]
fn test_a_claim_reaches_every_partition_with_depth_from_every_start_point() {
	// Each worker starts at its own hash, so a walk that does not wrap would miss partitions for some of them.
	let t = engine_with_queue(BY_REGION);
	let regions: Vec<String> = (0..8).map(|i| format!("r{i}")).collect();
	for (i, region) in regions.iter().enumerate() {
		t.command(&format!(r#"INSERT test::jobs [{{ id: {i}, region: "{region}" }}]"#));
	}
	ack_all(&t, &claim(&t, "drain", 8));

	for worker in 0..32 {
		let id = 100 + worker;
		let region = &regions[worker as usize % regions.len()];
		t.command(&format!(r#"INSERT test::jobs [{{ id: {id}, region: "{region}" }}]"#));

		let frames = claim(&t, &format!("worker-{worker}"), 4);
		assert_eq!(ids(&frames), BTreeSet::from([id]), "worker {worker} missed the only partition with depth");
		ack_all(&t, &frames);
	}
}

#[test]
fn test_a_single_claim_drains_across_partitions_up_to_max_n() {
	// A walk stopping at the first partition would under-deliver; one ignoring max_n would over-deliver.
	let t = engine_with_queue(BY_REGION);
	let rows: Vec<String> = (0..12).map(|i| format!(r#"{{ id: {i}, region: "r{}" }}"#, i % 6)).collect();
	t.command(&format!("INSERT test::jobs [{}]", rows.join(", ")));

	let first = claim(&t, "w1", 7);
	assert_eq!(items(&first).len(), 7);
	let second = claim(&t, "w2", 100);
	assert_eq!(items(&second).len(), 5);
	assert!(ids(&first).is_disjoint(&ids(&second)), "an item must never be handed out twice");
}

#[test]
fn test_a_claim_pages_past_one_page_of_partition_counters() {
	// A walk that stopped at the first page of counters would strand every partition beyond it.
	let t = engine_with_queue(BY_REGION);
	let rows: Vec<String> = (0..1100).map(|i| format!(r#"{{ id: {i}, region: "r{i}" }}"#)).collect();
	t.command(&format!("INSERT test::jobs [{}]", rows.join(", ")));
	let queue = queue_id(&t);

	assert_eq!(queue_partition_counters(t.inner().single(), queue).unwrap().len(), 1100);
	assert_eq!(items(&claim(&t, "w", 5000)).len(), 1100, "every partition must be reachable in one claim");
}

#[test]
fn test_ordered_by_blocks_within_a_partition_but_not_across_partitions() {
	// The rule is one active item per key in each FIFO: a key in one partition must not hold another back.
	let t = engine_with_queue(ORDERED_BY_REGION);
	t.command(
		r#"INSERT test::jobs [
			{ id: 1, region: "eu", tenant: "a" },
			{ id: 2, region: "eu", tenant: "a" },
			{ id: 3, region: "us", tenant: "a" }
		]"#,
	);
	let queue = queue_id(&t);

	let placed = states(&t, queue);
	assert_eq!(placed[&RowNumber(1)].1.status, QueueItemStatus::Ready);
	assert_eq!(placed[&RowNumber(2)].1.status, QueueItemStatus::Parked, "the second item of a key must wait");
	assert_eq!(placed[&RowNumber(3)].1.status, QueueItemStatus::Ready, "another partition must not be blocked");
	assert_eq!(placed[&RowNumber(1)].1.key_hash, placed[&RowNumber(3)].1.key_hash);

	let frames = claim(&t, "w", 10);
	assert_eq!(ids(&frames), BTreeSet::from([1, 3]), "the parked sibling must not be delivered");

	assert_eq!(ids(&claim(&t, "w", 10)), BTreeSet::new(), "the head is still leased, so its key stays blocked");

	ack_all(&t, &frames);
	assert_eq!(ids(&claim(&t, "w", 10)), BTreeSet::from([2]), "acking the head must promote its own successor");
}

#[test]
fn test_key_chains_are_scoped_to_the_partition() {
	// A chain key without the partition would make one key's chain span two FIFOs and break promotion.
	let t = engine_with_queue(ORDERED_BY_REGION);
	t.command(r#"INSERT test::jobs [{ id: 1, region: "eu", tenant: "a" }, { id: 2, region: "us", tenant: "a" }]"#);
	let queue = queue_id(&t);

	let placed = states(&t, queue);
	let store = t.inner().single().read_store();
	for row in [RowNumber(1), RowNumber(2)] {
		let (partition, state) = &placed[&row];
		let chain = SingleVersionRange::range_batch(
			&store,
			QueueKeyActiveKey::key_scan(queue, *partition, state.key_hash).encode(),
			16,
		)
		.unwrap();
		assert_eq!(chain.items.len(), 1, "each partition must hold its own chain for the key");
	}
}

#[test]
fn test_hydration_places_a_partitioned_row_where_the_insert_did() {
	// A hydrated row in another partition would appear as a second copy of already scheduled work.
	let t = engine_with_queue(BY_REGION);
	t.command(r#"INSERT test::jobs [{ id: 1, region: "eu" }, { id: 2, region: "us" }]"#);
	let queue = queue_id(&t);

	assert_eq!(
		hydrate_queues(t.inner()).unwrap(),
		0,
		"every stored row is already scheduled where hydration looks"
	);
	assert_eq!(states(&t, queue).len(), 2);
}

#[test]
fn test_a_none_partition_value_is_one_partition_and_hydrates_in_place() {
	// A none cell must hash the same from the batch path and the stored row, or hydration splits its FIFO.
	let t = engine_with_queue(
		"CREATE QUEUE test::jobs { id: int4, region: Option(utf8) } WITH { partition: { by: { region } }, fifo: {} }",
	);
	t.command(r#"INSERT test::jobs [{ id: 1, region: none }, { id: 2, region: none }, { id: 3, region: "eu" }]"#);
	let queue = queue_id(&t);

	let placed = states(&t, queue);
	assert_eq!(placed[&RowNumber(1)].0, placed[&RowNumber(2)].0);
	assert_ne!(placed[&RowNumber(1)].0, placed[&RowNumber(3)].0);
	assert_eq!(hydrate_queues(t.inner()).unwrap(), 0);
}

#[test]
fn test_drop_clears_the_scheduling_state_of_every_partition() {
	// Drop can no longer enumerate lanes, so a scan that missed a partition would leak its rows for good.
	let t = engine_with_queue(BY_REGION);
	t.command(r#"INSERT test::jobs [{ id: 1, region: "eu" }, { id: 2, region: "us" }, { id: 3, region: "ap" }]"#);
	let queue = queue_id(&t);

	t.admin("DROP QUEUE test::jobs");

	assert!(states(&t, queue).is_empty(), "item state must not outlive the queue");
	assert!(queue_partition_counters(t.inner().single(), queue).unwrap().is_empty());
}
