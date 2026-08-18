// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, Bound};

use reifydb_cdc::rebuild::rebuild_changes;
use reifydb_core::{
	interface::{
		catalog::storage::StorageId,
		cdc::{Cdc, SystemChange},
		change::{Change, ChangeOrigin, Diff},
	},
	key::{Key, row::RowKey},
	value::column::columns::Columns,
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{identity::IdentityId, row_number::RowNumber};

struct Lcg(u64);

impl Lcg {
	fn below(&mut self, bound: u64) -> u64 {
		self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
		(self.0 >> 33) % bound
	}
}

fn read_all(t: &TestEngine) -> Vec<Cdc> {
	t.cdc_store().read_range(Bound::Unbounded, Bound::Unbounded, 10_000).expect("cdc read").items
}

fn rebuilt(t: &TestEngine, cdc: &Cdc) -> Vec<Change> {
	let mut query = t.begin_query(IdentityId::system()).expect("query transaction");
	rebuild_changes(cdc, &t.catalog(), &mut Transaction::Query(&mut query)).expect("rebuild")
}

fn render_row(columns: &Columns, index: usize) -> String {
	let mut out = format!("row={}", columns.row_numbers()[index].0);
	if let Some(created_at) = columns.created_at().get(index) {
		out.push_str(&format!(" created_at={:?}", created_at));
	}
	if let Some(updated_at) = columns.updated_at().get(index) {
		out.push_str(&format!(" updated_at={:?}", updated_at));
	}
	if let Some(time) = columns.time().get(index) {
		out.push_str(&format!(" time={:?}", time));
	}
	for (name, value) in columns.names.iter().zip(columns.get_row(index)) {
		out.push_str(&format!(" {}={:?}", name.text(), value));
	}
	out
}

fn render_diff(diff: &Diff) -> Vec<String> {
	match diff {
		Diff::Insert {
			post,
			..
		} => (0..post.row_count()).map(|i| format!("insert {}", render_row(post, i))).collect(),
		Diff::Update {
			pre,
			post,
			..
		} => (0..post.row_count())
			.map(|i| format!("update pre({}) post({})", render_row(pre, i), render_row(post, i)))
			.collect(),
		Diff::Remove {
			pre,
			..
		} => (0..pre.row_count()).map(|i| format!("remove {}", render_row(pre, i))).collect(),
	}
}

fn canonical(changes: &[Change]) -> BTreeMap<String, Vec<String>> {
	let mut by_origin: BTreeMap<String, Vec<Change>> = BTreeMap::new();
	for change in changes {
		let origin = match &change.origin {
			ChangeOrigin::Object(object) => format!("{object:?}"),
			ChangeOrigin::Flow(operator) => {
				panic!("a persisted change must never carry a flow origin, got {operator:?}")
			}
		};
		by_origin.entry(origin).or_default().push(change.clone());
	}
	by_origin
		.into_iter()
		.map(|(origin, group)| {
			let merged = Change::merge(group).expect("merge same-origin changes");
			let mut rows: Vec<String> = merged.diffs.iter().flat_map(render_diff).collect();
			rows.sort();
			(origin, rows)
		})
		.filter(|(_, rows)| !rows.is_empty())
		.collect()
}

fn assert_round_trip(t: &TestEngine, label: &str) -> usize {
	t.await_cdc();
	let mut compared = 0;
	for cdc in read_all(t) {
		let original = canonical(&cdc.changes);
		let rebuilt = canonical(&rebuilt(t, &cdc));
		assert_eq!(rebuilt, original, "{label}: rebuild diverged at commit version {}", cdc.version.0);
		compared += original.values().map(Vec::len).sum::<usize>();
	}
	assert!(compared > 0, "{label}: nothing was compared, the workload produced no row changes");
	compared
}

#[test]
fn rebuild_round_trips_table_inserts_updates_and_deletes() {
	// Covers all three diff kinds, a multi-column shape and none values on a non-partitioned table.
	let t = TestEngine::new();
	t.admin("create namespace test");
	t.admin("create table test::items { id: int4, label: Option(utf8), score: Option(float8) }");

	t.command("insert test::items [{ id: 1, label: 'a', score: 1.5 }, { id: 2, label: 'b', score: 2.5 }]");
	t.command("insert test::items [{ id: 3 }]");
	t.mock_clock().advance_millis(1_000);
	t.command("update test::items { label: 'z' } filter { id == 1 }");
	t.command("delete test::items filter { id == 2 }");

	assert_round_trip(&t, "table inserts, updates and deletes");
}

#[test]
fn rebuild_round_trips_insert_then_update_in_one_commit() {
	// A same-commit insert+update reaches the delta log as one insert, so only the consolidated original matches.
	let t = TestEngine::new();
	t.admin("create namespace test");
	t.admin("create table test::items { id: int4, label: Option(utf8) }");

	t.command("insert test::items [{ id: 1, label: 'a' }]; update test::items { label: 'b' } filter { id == 1 }");

	assert_round_trip(&t, "insert then update in one commit");
}

#[test]
fn rebuild_round_trips_update_then_delete_in_one_commit() {
	// The surviving delete must carry the committed pre-image, never the mid-transaction one.
	let t = TestEngine::new();
	t.admin("create namespace test");
	t.admin("create table test::items { id: int4, label: Option(utf8) }");
	t.command("insert test::items [{ id: 1, label: 'a' }]");
	t.mock_clock().advance_millis(500);

	t.command("update test::items { label: 'b' } filter { id == 1 }; delete test::items filter { id == 1 }");

	assert_round_trip(&t, "update then delete in one commit");
}

#[test]
fn rebuild_round_trips_partitioned_table() {
	// A partitioned table writes PartitionedRowKey instead of RowKey; both must name the same object.
	let t = TestEngine::new();
	t.admin("create namespace test");
	t.admin("create table test::events { id: int4, region: utf8, amount: int8 } with { partition: { by: { region } } }");

	t.command(
		"insert test::events [{ id: 1, region: 'eu', amount: 10 }, { id: 2, region: 'us', amount: 20 }, { id: 3, region: 'eu', amount: 30 }]",
	);
	t.mock_clock().advance_millis(250);
	t.command("update test::events { amount: 99 } filter { id == 1 }");
	t.command("delete test::events filter { id == 3 }");

	assert_round_trip(&t, "partitioned table");
}

#[test]
fn rebuild_round_trips_ringbuffer_including_capacity_eviction() {
	// Overrunning the capacity emits a remove alongside the inserts, so one commit carries two diff kinds.
	let t = TestEngine::new();
	t.admin("create namespace test");
	t.admin("create ringbuffer test::rb { id: int4, data: utf8 } with { capacity: 3 }");

	t.command("insert test::rb [{ id: 1, data: 'a' }, { id: 2, data: 'b' }]");
	t.mock_clock().advance_millis(100);
	t.command("insert test::rb [{ id: 3, data: 'c' }, { id: 4, data: 'd' }]");

	assert_round_trip(&t, "ringbuffer with eviction");
}

#[test]
fn rebuild_round_trips_multiple_objects_in_one_commit() {
	// Two objects written under one commit must come back as two separate per-origin changes.
	let t = TestEngine::new();
	t.admin("create namespace test");
	t.admin("create table test::alpha { id: int4, label: utf8 }");
	t.admin("create table test::beta { id: int4, amount: int8 }");

	t.command("insert test::alpha [{ id: 1, label: 'a' }]; insert test::beta [{ id: 1, amount: 7 }]");

	let compared = assert_round_trip(&t, "multiple objects in one commit");
	assert!(compared >= 2, "the two-object commit must contribute at least two rows");
}

#[test]
fn rebuild_round_trips_randomised_workload_seeded_1234() {
	// A fixed seed keeps the operation mix deterministic; a real RNG would make a failure unreproducible.
	let t = TestEngine::new();
	t.admin("create namespace test");
	t.admin("create table test::plain { id: int4, label: Option(utf8), score: Option(float8) }");
	t.admin("create table test::split { id: int4, region: utf8, amount: int8 } with { partition: { by: { region } } }");

	let mut rng = Lcg(1234);
	let mut live_plain: Vec<u64> = Vec::new();
	let mut live_split: Vec<u64> = Vec::new();
	let mut next_id = 1u64;

	for _ in 0..60 {
		t.mock_clock().advance_millis(10);
		match rng.below(4) {
			0 => {
				let id = next_id;
				next_id += 1;
				if rng.below(2) == 0 {
					t.command(&format!("insert test::plain [{{ id: {id} }}]"));
				} else {
					t.command(&format!(
						"insert test::plain [{{ id: {id}, label: 'l{id}', score: {id}.5 }}]"
					));
				}
				live_plain.push(id);
			}
			1 => {
				let id = next_id;
				next_id += 1;
				let region = if rng.below(2) == 0 {
					"eu"
				} else {
					"us"
				};
				t.command(&format!(
					"insert test::split [{{ id: {id}, region: '{region}', amount: {id} }}]"
				));
				live_split.push(id);
			}
			2 => {
				if live_plain.is_empty() {
					continue;
				}
				let id = live_plain[rng.below(live_plain.len() as u64) as usize];
				t.command(&format!("update test::plain {{ label: 'u{id}' }} filter {{ id == {id} }}"));
			}
			_ => {
				if live_split.is_empty() {
					continue;
				}
				let index = rng.below(live_split.len() as u64) as usize;
				let id = live_split.remove(index);
				t.command(&format!("delete test::split filter {{ id == {id} }}"));
			}
		}
	}

	let compared = assert_round_trip(&t, "randomised workload");
	assert!(compared >= 40, "the randomised workload must exercise at least 40 rows, got {compared}");
}

#[test]
fn rebuild_maps_a_view_row_key_to_the_view_object() {
	// A view row key that decoded to a table would resurrect the identity gap this rebuild depends on being closed.
	let t = TestEngine::new();
	t.admin("create namespace test");
	t.admin("create table test::items { id: int4, label: utf8 }");
	t.command("insert test::items [{ id: 1, label: 'a' }]");
	t.await_cdc();

	let table_commit =
		read_all(&t).into_iter().find(|cdc| !cdc.changes.is_empty()).expect("the insert must produce a change");
	let post = match table_commit.system_changes.iter().find(|change| {
		matches!(change, SystemChange::Insert { .. }) && matches!(Key::decode(change.key()), Some(Key::Row(_)))
	}) {
		Some(SystemChange::Insert {
			post,
			..
		}) => post.clone(),
		other => panic!("expected a row insert system change, got {other:?}"),
	};

	let view_commit = Cdc::new(
		table_commit.version,
		table_commit.timestamp,
		Vec::new(),
		vec![SystemChange::Insert {
			key: RowKey::encoded(StorageId::view(7), RowNumber(1)),
			post,
		}],
	);

	let changes = rebuilt(&t, &view_commit);
	assert_eq!(changes.len(), 1, "one view row must rebuild into exactly one change");
	assert_eq!(format!("{:?}", changes[0].origin), "Object(View(ViewId(7)))");
	assert_eq!(
		canonical(&changes).into_values().flatten().collect::<Vec<_>>(),
		canonical(&table_commit.changes).into_values().flatten().collect::<Vec<_>>(),
		"the same row bytes must decode to the same columns whichever object owns them"
	);
}

#[test]
fn rebuild_invents_a_change_for_queue_rows() {
	// Queue rows reach the delta log but never the change list, so the rebuild is a strict superset here.
	let t = TestEngine::new();
	t.admin("create namespace test");
	t.admin("create queue test::jobs { id: int4, payload: utf8 } with { fifo: {} }");
	t.command("insert test::jobs [{ id: 1, payload: 'a' }]");
	t.await_cdc();

	let mut queue_rows = 0;
	for cdc in read_all(&t) {
		assert!(canonical(&cdc.changes).is_empty(), "a queue write must emit no change today");
		queue_rows +=
			canonical(&rebuilt(&t, &cdc)).keys().filter(|origin| origin.starts_with("Queue")).count();
	}
	assert_eq!(queue_rows, 1, "the rebuild must surface the queue row that the change list omits");
}

#[test]
fn rebuild_round_trips_partitioned_series() {
	// A partitioned series lands under PartitionedRowKey, so its sequence must become the row number.
	let t = TestEngine::new();
	t.admin("create namespace test");
	t.admin(
		"create series test::metrics { ts: datetime, region: utf8, value: int2 } with { key: ts, partition: { by: { region } } }",
	);

	t.command("insert test::metrics [{ ts: '2026-01-01T00:00:00Z', region: 'eu', value: 7 }]");
	t.mock_clock().advance_millis(50);
	t.command("insert test::metrics [{ ts: '2026-01-01T00:01:00Z', region: 'us', value: 9 }]");

	assert_round_trip(&t, "partitioned series");
}

#[test]
fn rebuild_drops_series_rows_written_under_a_series_row_key() {
	// SeriesRowKey shares KeyKind::Row with a different layout, so series rows stay outside this rebuild.
	let t = TestEngine::new();
	t.admin("create namespace test");
	t.admin("create series test::metrics { ts: datetime, value: int2 } with { key: ts }");
	t.command("insert test::metrics [{ ts: '2026-01-01T00:00:00Z', value: 7 }]");
	t.await_cdc();

	let mut series_rows = 0;
	for cdc in read_all(&t) {
		series_rows += canonical(&cdc.changes).keys().filter(|origin| origin.starts_with("Series")).count();
		assert!(canonical(&rebuilt(&t, &cdc)).is_empty(), "a series row key must not decode as a plain row");
	}
	assert_eq!(series_rows, 1, "the series insert must be present in the change list it is missing from");
}
