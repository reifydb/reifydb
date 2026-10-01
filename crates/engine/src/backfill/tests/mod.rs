// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, num::NonZeroU64, sync::Arc};

use arrow_array::RecordBatch;
use reifydb_catalog::{interceptor::CatalogCacheInterceptor, system::ids::vtable::TABLES};
use reifydb_codec::row::shape::RowFamily;
use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	event::EventBus,
	interface::{
		catalog::{id::TableId, object::ObjectId, view::View},
		change::{Change, ChangeOrigin, Diff},
	},
	key::row::RowKey,
	row::row_shape_from_columns,
};
#[cfg(feature = "testing")]
use reifydb_flow::backfill::testing::{InstalledScanHooks, ScanHooks};
use reifydb_flow::{
	backfill::Scan,
	operator::sink::{encode_row_at_index, shape_field_columns},
};
#[cfg(feature = "testing")]
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_test_harness::engine::create_test_admin_transaction;
use reifydb_transaction::{
	dictionary::DictionaryAllocatorRegistry,
	interceptor::interceptors::Interceptors,
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction, query::QueryTransaction},
};
use reifydb_value::{
	Result,
	error::{Diagnostic, Error},
	params::Params,
	value::{
		Value,
		column_view::ColumnView,
		datetime::DateTime,
		identity::IdentityId,
		row_number::RowNumber,
		system_columns::{
			SystemColumn, require_created_at, require_row_numbers, require_time, require_updated_at,
			system_column, user_columns,
		},
	},
};

use crate::{
	backfill::{TransactionScan, run},
	vm::{Admin, Command, executor::Executor},
};

const DROPPED: [&str; 2] = ["#commit_version", "#partition"];

#[derive(Clone, Copy)]
enum Kind {
	Table,
	View,
	RingBuffer,
	Series,
	Queue,
	Dictionary,
}

#[derive(Clone)]
struct Db {
	executor: Executor,
	multi: MultiTransaction,
	single: SingleTransaction,
	event_bus: EventBus,
	allocators: DictionaryAllocatorRegistry,
}

impl Db {
	fn new() -> Self {
		// Every txn of a test must share one store; the seed txn only lends its store handles.
		let mut seed = create_test_admin_transaction();
		let db = Self {
			executor: Executor::testing(),
			multi: seed.multi.clone(),
			single: seed.single.clone(),
			event_bus: seed.event_bus.clone(),
			allocators: seed.dictionary_allocators().unwrap(),
		};
		seed.rollback().unwrap();
		db
	}

	fn interceptors(&self) -> Interceptors {
		// Without the cache fill, later txns never see committed DDL and transactional views stop updating.
		let mut interceptors = Interceptors::new();
		interceptors.pre_publish.add(Arc::new(CatalogCacheInterceptor::new(&self.executor.catalog)));
		interceptors
	}

	fn admin(&self) -> AdminTransaction {
		let mut txn = AdminTransaction::new(
			self.multi.clone(),
			self.single.clone(),
			self.event_bus.clone(),
			self.interceptors(),
			IdentityId::system(),
		)
		.unwrap();
		txn.set_dictionary_allocators(self.allocators.clone());
		txn
	}

	fn command(&self) -> CommandTransaction {
		CommandTransaction::new(
			self.multi.clone(),
			self.single.clone(),
			self.event_bus.clone(),
			self.interceptors(),
			IdentityId::system(),
		)
		.unwrap()
	}

	fn query(&self) -> QueryTransaction {
		QueryTransaction::new(self.multi.begin_query().unwrap(), self.single.clone(), IdentityId::system())
	}

	fn exec(&self, txn: &mut AdminTransaction, rql: &str) {
		let r = self.executor.admin(
			txn,
			Admin {
				rql,
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{rql}: {e:?}");
		}
	}

	fn exec_command(&self, txn: &mut CommandTransaction, rql: &str) {
		let r = self.executor.command(
			txn,
			Command {
				rql,
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{rql}: {e:?}");
		}
	}

	fn commit(&self, statements: &[&str]) -> CommitVersion {
		let mut txn = self.admin();
		for rql in statements {
			self.exec(&mut txn, rql);
		}
		txn.commit().unwrap()
	}

	fn from(&self, tx: &mut Transaction<'_>, source: &str) -> Vec<RecordBatch> {
		let rql = format!("FROM {source}");
		let r = self.executor.rql(tx, &rql, Params::default());
		if let Some(e) = r.error {
			panic!("{rql}: {e:?}");
		}
		r.frames.into_iter().map(|frame| frame.batch).collect()
	}

	fn id(&self, tx: &mut Transaction<'_>, kind: Kind, name: &str) -> ObjectId {
		let catalog = &self.executor.catalog;
		let ns = catalog.find_namespace_by_name(tx, "ns").unwrap().unwrap().id();
		match kind {
			Kind::Table => ObjectId::table(catalog.find_table_by_name(tx, ns, name).unwrap().unwrap().id),
			Kind::View => ObjectId::view(catalog.find_view_by_name(tx, ns, name).unwrap().unwrap().id()),
			Kind::RingBuffer => {
				ObjectId::ringbuffer(catalog.find_ringbuffer_by_name(tx, ns, name).unwrap().unwrap().id)
			}
			Kind::Series => {
				ObjectId::series(catalog.find_series_by_name(tx, ns, name).unwrap().unwrap().id)
			}
			Kind::Queue => ObjectId::queue(catalog.find_queue_by_name(tx, ns, name).unwrap().unwrap().id),
			Kind::Dictionary => {
				ObjectId::dictionary(catalog.find_dictionary_by_name(tx, ns, name).unwrap().unwrap().id)
			}
		}
	}

	fn view_def(&self, tx: &mut Transaction<'_>, name: &str) -> View {
		let catalog = &self.executor.catalog;
		let ns = catalog.find_namespace_by_name(tx, "ns").unwrap().unwrap().id();
		catalog.find_view_by_name(tx, ns, name).unwrap().unwrap()
	}

	fn mirror(&self, source: &str, view: &str) -> CommitVersion {
		// The deferred flow actor never runs in-crate, so the view must get the source's rows verbatim here.
		let mut txn = self.admin();
		let def = self.view_def(&mut Transaction::Admin(&mut txn), view);
		let storage = def.storage_id();
		let shape = row_shape_from_columns(RowFamily::Table, def.columns());
		let current = self.from(&mut Transaction::Admin(&mut txn), &format!("ns::{source}"));
		let stale = self.from(&mut Transaction::Admin(&mut txn), &format!("ns::{view}"));
		let mut kept = BTreeSet::new();
		for batch in &current {
			let fields = shape_field_columns(batch, &shape);
			for (index, &number) in require_row_numbers(batch).unwrap().iter().enumerate() {
				let (_, bytes) = encode_row_at_index(batch, index, &shape, number, &fields).unwrap();
				txn.set(&RowKey::new(storage, number), bytes).unwrap();
				kept.insert(number);
			}
		}
		for batch in &stale {
			for &number in require_row_numbers(batch).unwrap() {
				if !kept.contains(&number) {
					txn.remove(&RowKey::new(storage, number)).unwrap();
				}
			}
		}
		txn.commit().unwrap()
	}

	fn backfill(&self, tx: Transaction<'_>, sources: &[ObjectId], batch_size: u64) -> (Result<()>, Vec<Change>) {
		let mut changes = Vec::new();
		let result = run(self.executor.services(), tx, &set(sources), size(batch_size), |_, change| {
			changes.push(change);
			Ok(())
		});
		(result, changes)
	}

	fn snapshot(&self, tx: Transaction<'_>, sources: &[ObjectId], batch_size: u64) -> (CommitVersion, Vec<Change>) {
		let version = tx.version();
		let (result, changes) = self.backfill(tx, sources, batch_size);
		result.unwrap();
		(version, changes)
	}

	fn raw_scan<'a>(&self, tx: Transaction<'a>) -> TransactionScan<'a> {
		TransactionScan {
			services: self.executor.services().clone(),
			tx,
			opened: None,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Row {
	number: RowNumber,
	values: Vec<Value>,
	created: DateTime,
	updated: DateTime,
	time: Option<DateTime>,
}

fn size(rows: u64) -> NonZeroU64 {
	NonZeroU64::new(rows).unwrap()
}

fn set(sources: &[ObjectId]) -> BTreeSet<ObjectId> {
	sources.iter().copied().collect()
}

fn text(value: &str) -> Value {
	Value::Utf8(value.to_string())
}

fn pairs(rows: &[(i32, i32)]) -> Vec<Vec<Value>> {
	rows.iter().map(|&(a, b)| vec![Value::Int4(a), Value::Int4(b)]).collect()
}

fn names(batch: &RecordBatch) -> Vec<String> {
	batch.schema_ref().fields().iter().map(|field| field.name().clone()).collect()
}

fn without(batch: &RecordBatch, dropped: &[&str]) -> RecordBatch {
	let kept: Vec<usize> = batch
		.schema_ref()
		.fields()
		.iter()
		.enumerate()
		.filter(|(_, field)| !dropped.contains(&field.name().as_str()))
		.map(|(index, _)| index)
		.collect();
	batch.project(&kept).unwrap()
}

fn rows_of(batch: &RecordBatch) -> Vec<Row> {
	if batch.num_rows() == 0 {
		return Vec::new();
	}
	let numbers = require_row_numbers(batch).unwrap();
	let created = require_created_at(batch).unwrap();
	let updated = require_updated_at(batch).unwrap();
	let times = system_column(batch, SystemColumn::Time).map(|_| require_time(batch).unwrap());
	let users: Vec<ColumnView<'_>> = user_columns(batch)
		.map(|(field, array)| ColumnView::try_from((array, field.as_ref())).unwrap())
		.collect();
	(0..batch.num_rows())
		.map(|index| Row {
			number: numbers[index],
			values: users.iter().map(|view| view.get_value(index)).collect(),
			created: created[index],
			updated: updated[index],
			time: times.map(|times| times[index]),
		})
		.collect()
}

fn post(change: &Change) -> &RecordBatch {
	assert_eq!(change.diffs.len(), 1, "a snapshot chunk must be exactly one diff");
	match &change.diffs[0] {
		Diff::Insert {
			post,
			..
		} => post,
		other => panic!("a snapshot chunk must be an insert, got {other:?}"),
	}
}

fn chunk_sizes(changes: &[Change]) -> Vec<usize> {
	changes.iter().map(|change| post(change).num_rows()).collect()
}

fn snapshot_rows(changes: &[Change]) -> Vec<Row> {
	let mut rows: Vec<Row> = changes.iter().flat_map(|change| rows_of(post(change))).collect();
	rows.sort();
	rows
}

fn user_values(changes: &[Change]) -> Vec<Vec<Value>> {
	let mut values: Vec<Vec<Value>> = snapshot_rows(changes).into_iter().map(|row| row.values).collect();
	values.sort();
	values
}

fn from_rows(from: &[RecordBatch], dropped: &[&str]) -> (Vec<String>, Vec<Row>) {
	let projected: Vec<RecordBatch> = from.iter().map(|batch| without(batch, dropped)).collect();
	let names = names(&projected[0]);
	let mut rows: Vec<Row> = projected.iter().flat_map(rows_of).collect();
	rows.sort();
	(names, rows)
}

fn from_user_values(from: &[RecordBatch]) -> Vec<Vec<Value>> {
	let (_, rows) = from_rows(from, &DROPPED);
	let mut values: Vec<Vec<Value>> = rows.into_iter().map(|row| row.values).collect();
	values.sort();
	values
}

fn assert_matches_from(
	changes: &[Change],
	source: ObjectId,
	version: CommitVersion,
	from: &[RecordBatch],
	dropped: &[&str],
) -> Vec<Row> {
	let (expected_names, expected_rows) = from_rows(from, dropped);
	for change in changes {
		assert_eq!(
			change.origin,
			ChangeOrigin::Object(source),
			"every change must come from the scanned source"
		);
		assert_eq!(change.version, ChangeVersion::from(version), "every snapshot change must carry V");
		let post = post(change);
		assert!(post.num_rows() > 0, "a 0-row chunk must never reach consume");
		let columns = names(post);
		assert!(
			!columns.iter().any(|name| name == "#commit_version" || name == "#partition"),
			"B2b: snapshot rows must not carry #commit_version or #partition, got {columns:?}"
		);
		assert_eq!(
			columns, expected_names,
			"a snapshot chunk must carry the same columns as `from`, minus {dropped:?}"
		);
		let newest = *require_updated_at(post).unwrap().iter().max().unwrap();
		assert_eq!(change.changed_at, newest, "changed_at must be the newest #updated_at of its chunk");
	}
	let rows = snapshot_rows(changes);
	let numbers: BTreeSet<RowNumber> = rows.iter().map(|row| row.number).collect();
	assert_eq!(numbers.len(), rows.len(), "a row must never be delivered twice");
	assert_eq!(rows, expected_rows, "the backfill at V must equal `from` at V, stamps included");
	rows
}

fn table_with_history(db: &Db) {
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4, v: int4 }",
		"INSERT ns::t [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }, { id: 4, v: 40 }, { id: 5, v: 50 }, { id: 6, v: 60 }]",
	]);
	db.commit(&["UPDATE ns::t { v: 21 } FILTER { id == 2 }", "DELETE ns::t FILTER { id == 3 }"]);
	db.commit(&[
		"INSERT ns::t [{ id: 7, v: 70 }]",
		"UPDATE ns::t { v: 51 } FILTER { id == 5 }",
		"DELETE ns::t FILTER { id == 6 }",
	]);
}

fn six_rows(db: &Db) -> CommitVersion {
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4, v: int4 }",
		"INSERT ns::t [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }, { id: 4, v: 40 }, { id: 5, v: 50 }, { id: 6, v: 60 }]",
	])
}

fn error(code: &str) -> Error {
	Error(Box::new(Diagnostic {
		code: code.to_string(),
		..Default::default()
	}))
}

#[test]
fn a_table_backfill_at_v_equals_from_at_v_after_inserts_updates_and_deletes() {
	// A scan that reads latest, keeps #commit_version, or re-stamps rows would diverge from `from` at the same V.
	let db = Db::new();
	table_with_history(&db);
	let mut q = db.query();
	let t = db.id(&mut Transaction::Query(&mut q), Kind::Table, "t");
	let (v, changes) = db.snapshot(Transaction::Query(&mut q), &[t], 2);
	let from = db.from(&mut Transaction::Query(&mut q), "ns::t");
	let rows = assert_matches_from(&changes, t, v, &from, &DROPPED);
	assert_eq!(user_values(&changes), pairs(&[(1, 10), (2, 21), (4, 40), (5, 51), (7, 70)]));
	for row in &rows {
		let touched = row.values[0] == Value::Int4(2) || row.values[0] == Value::Int4(5);
		assert_eq!(
			row.updated > row.created,
			touched,
			"#updated_at must stay the row's own last write time, row {:?}",
			row.values
		);
	}
	assert_eq!(chunk_sizes(&changes), vec![2, 2, 1]);
	assert!(
		changes.iter().any(|change| {
			let stamps = require_updated_at(post(change)).unwrap();
			stamps.iter().min() != stamps.iter().max()
		}),
		"the fixture must give some chunk mixed #updated_at, else the changed_at check proves nothing"
	);
}

#[test]
fn a_table_with_a_time_source_carries_time_from_the_stored_row() {
	// #time must come from the stored row (event column or first insert), never from #updated_at or the scan clock.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::e { id: int4, at: datetime, n: int4 } WITH { time: event(at) }",
		"CREATE TABLE ns::p { id: int4, n: int4 } WITH { time: processing }",
		"INSERT ns::e [{ id: 1, at: cast('2024-01-01T00:00:00Z', datetime), n: 1 }, { id: 2, at: cast('2024-02-01T00:00:00Z', datetime), n: 2 }, { id: 3, at: cast('2024-03-01T00:00:00Z', datetime), n: 3 }]",
		"INSERT ns::p [{ id: 1, n: 1 }, { id: 2, n: 2 }, { id: 3, n: 3 }]",
	]);
	db.commit(&["UPDATE ns::e { n: 20 } FILTER { id == 2 }", "UPDATE ns::p { n: 20 } FILTER { id == 2 }"]);
	let mut q = db.query();
	let e = db.id(&mut Transaction::Query(&mut q), Kind::Table, "e");
	let p = db.id(&mut Transaction::Query(&mut q), Kind::Table, "p");
	let (v, changes) = db.snapshot(Transaction::Query(&mut q), &[e, p], 2);
	let of = |source: ObjectId| -> Vec<Change> {
		changes.iter().filter(|change| change.origin == ChangeOrigin::Object(source)).cloned().collect()
	};
	let from_e = db.from(&mut Transaction::Query(&mut q), "ns::e");
	let from_p = db.from(&mut Transaction::Query(&mut q), "ns::p");
	let event_rows = assert_matches_from(&of(e), e, v, &from_e, &DROPPED);
	let processing_rows = assert_matches_from(&of(p), p, v, &from_p, &DROPPED);
	assert_eq!(event_rows.len(), 3);
	for row in &event_rows {
		let Value::DateTime(at) = row.values[1] else {
			panic!("column at must be a datetime, got {:?}", row.values[1]);
		};
		assert_eq!(row.time, Some(at), "event time #time must equal the at column");
	}
	assert_eq!(processing_rows.len(), 3);
	for row in &processing_rows {
		assert_eq!(row.time, Some(row.created), "processing time #time must stay the insert time");
	}
	assert!(
		processing_rows.iter().any(|row| row.time != Some(row.updated)),
		"the fixture must update a row, else #time and #updated_at cannot be told apart"
	);
}

#[test]
fn a_transactional_view_backfill_equals_from_the_view() {
	// The view scan node must read the view's own stored rows at V, not its sources.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4, v: int4 }",
		"CREATE TRANSACTIONAL VIEW ns::v { id: int4, v: int4 } AS { FROM ns::t FILTER { v > 15 } }",
		"INSERT ns::t [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }, { id: 4, v: 40 }]",
	]);
	db.commit(&["UPDATE ns::t { v: 31 } FILTER { id == 3 }", "DELETE ns::t FILTER { id == 4 }"]);
	db.commit(&["INSERT ns::t [{ id: 5, v: 50 }, { id: 6, v: 5 }]"]);
	let mut q = db.query();
	let view = db.id(&mut Transaction::Query(&mut q), Kind::View, "v");
	let (v, changes) = db.snapshot(Transaction::Query(&mut q), &[view], 2);
	let from = db.from(&mut Transaction::Query(&mut q), "ns::v");
	assert_matches_from(&changes, view, v, &from, &DROPPED);
	assert_eq!(user_values(&changes), pairs(&[(2, 20), (3, 31), (5, 50)]));
}

#[test]
fn a_deferred_view_backfill_equals_from_the_view() {
	// A deferred view is read from its stored rows like any view; without guard handling its scan would not open.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4, v: int4 }",
		"CREATE DEFERRED VIEW ns::dv { id: int4, v: int4 } AS { FROM ns::t }",
		"INSERT ns::t [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }, { id: 4, v: 40 }]",
	]);
	db.mirror("t", "dv");
	db.commit(&["UPDATE ns::t { v: 21 } FILTER { id == 2 }", "DELETE ns::t FILTER { id == 3 }"]);
	db.mirror("t", "dv");
	db.commit(&["INSERT ns::t [{ id: 5, v: 50 }]"]);
	db.mirror("t", "dv");
	for scan_in_admin in [false, true] {
		let (dv, v, changes, from) = if scan_in_admin {
			let mut txn = db.admin();
			let dv = db.id(&mut Transaction::Admin(&mut txn), Kind::View, "dv");
			let (v, changes) = db.snapshot(Transaction::Admin(&mut txn), &[dv], 2);
			let from = db.from(&mut Transaction::Admin(&mut txn), "ns::dv");
			txn.rollback().unwrap();
			(dv, v, changes, from)
		} else {
			let mut q = db.query();
			let dv = db.id(&mut Transaction::Query(&mut q), Kind::View, "dv");
			let (v, changes) = db.snapshot(Transaction::Query(&mut q), &[dv], 2);
			let from = db.from(&mut Transaction::Query(&mut q), "ns::dv");
			(dv, v, changes, from)
		};
		assert_matches_from(&changes, dv, v, &from, &DROPPED);
		assert_eq!(user_values(&changes), pairs(&[(1, 10), (2, 21), (4, 40), (5, 50)]));
	}
}

#[test]
fn a_ringbuffer_backfill_equals_from_after_eviction_updates_and_deletes() {
	// Ring buffer rows live in a slot range with a moving head; a scan from the wrong bound drops or repeats rows.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE RINGBUFFER ns::rb { a: int4, b: int4 } WITH { capacity: 5 }",
		"INSERT ns::rb [{ a: 1, b: 10 }, { a: 2, b: 20 }, { a: 3, b: 30 }]",
	]);
	db.commit(&["UPDATE ns::rb { b: 21 } FILTER { a == 2 }", "DELETE ns::rb FILTER { a == 3 }"]);
	db.commit(&["INSERT ns::rb [{ a: 4, b: 40 }, { a: 5, b: 50 }, { a: 6, b: 60 }, { a: 7, b: 70 }]"]);
	let mut q = db.query();
	let rb = db.id(&mut Transaction::Query(&mut q), Kind::RingBuffer, "rb");
	let (v, changes) = db.snapshot(Transaction::Query(&mut q), &[rb], 2);
	let from = db.from(&mut Transaction::Query(&mut q), "ns::rb");
	assert_matches_from(&changes, rb, v, &from, &DROPPED);
	assert_eq!(user_values(&changes), pairs(&[(2, 21), (4, 40), (5, 50), (6, 60), (7, 70)]));
	assert_eq!(chunk_sizes(&changes), vec![2, 2, 1]);
}

#[test]
fn a_series_backfill_equals_from_after_updates_and_deletes() {
	// The series scan fills its own stamps; they must match what `from` shows at V.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE SERIES ns::s { ts: int8, val: int4 } WITH { key: ts }",
		"INSERT ns::s [{ ts: 1, val: 1 }, { ts: 2, val: 2 }, { ts: 3, val: 3 }, { ts: 4, val: 4 }]",
	]);
	db.commit(&["UPDATE ns::s { val: 20 } FILTER { ts == 2 }", "DELETE ns::s FILTER { ts == 3 }"]);
	db.commit(&["INSERT ns::s [{ ts: 5, val: 5 }]"]);
	let mut q = db.query();
	let s = db.id(&mut Transaction::Query(&mut q), Kind::Series, "s");
	let (v, changes) = db.snapshot(Transaction::Query(&mut q), &[s], 2);
	let from = db.from(&mut Transaction::Query(&mut q), "ns::s");
	assert_matches_from(&changes, s, v, &from, &DROPPED);
	let expected: Vec<Vec<Value>> = [(1, 1), (2, 20), (4, 4), (5, 5)]
		.iter()
		.map(|&(ts, val)| vec![Value::Int8(ts), Value::Int4(val)])
		.collect();
	assert_eq!(user_values(&changes), expected);
}

#[test]
fn a_tagged_series_backfill_drops_the_tag_column_that_live_changes_never_carry() {
	// snapshot rows must have the live change shape; live series inserts carry key and data columns, no tag.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE ENUM ns::status { Active, Inactive }",
		"CREATE SERIES ns::m { ts: datetime, val: int4 } WITH { key: ts, tag: ns::status }",
		"INSERT ns::m [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: 0 }, { ts: cast('2024-01-02T00:00:00Z', datetime), val: 2, tag: 1 }, { ts: cast('2024-01-03T00:00:00Z', datetime), val: 3, tag: 1 }]",
	]);
	db.commit(&["DELETE ns::m FILTER { val == 3 }"]);
	let mut q = db.query();
	let m = db.id(&mut Transaction::Query(&mut q), Kind::Series, "m");
	let (v, changes) = db.snapshot(Transaction::Query(&mut q), &[m], 1);
	let from = db.from(&mut Transaction::Query(&mut q), "ns::m");
	assert!(names(&from[0]).contains(&"tag".to_string()), "the fixture relies on `from` showing the tag column");
	assert_matches_from(&changes, m, v, &from, &["#commit_version", "#partition", "tag"]);
	assert_eq!(chunk_sizes(&changes), vec![1, 1]);
}

#[test]
fn a_partitioned_table_backfill_equals_from_without_the_partition_column() {
	// Partitioned rows live under per-partition keys; the scan must walk every partition once and drop #partition.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::p { id: int4, region: utf8, n: int4 } WITH { partition: { by: { region } } }",
		"INSERT ns::p [{ id: 1, region: 'eu', n: 1 }, { id: 2, region: 'us', n: 2 }, { id: 3, region: 'eu', n: 3 }, { id: 4, region: 'ap', n: 4 }, { id: 5, region: 'us', n: 5 }, { id: 6, region: 'ap', n: 6 }]",
	]);
	db.commit(&["UPDATE ns::p { n: 30 } FILTER { id == 3 }", "DELETE ns::p FILTER { id == 5 }"]);
	db.commit(&["INSERT ns::p [{ id: 7, region: 'sa', n: 7 }]"]);
	let mut q = db.query();
	let p = db.id(&mut Transaction::Query(&mut q), Kind::Table, "p");
	for batch_size in [1, 2, 4, 100] {
		let (v, changes) = db.snapshot(Transaction::Query(&mut q), &[p], batch_size);
		let from = db.from(&mut Transaction::Query(&mut q), "ns::p");
		assert!(
			names(&from[0]).contains(&"#partition".to_string()),
			"the fixture relies on `from` showing #partition"
		);
		assert_matches_from(&changes, p, v, &from, &DROPPED);
		assert!(
			chunk_sizes(&changes).iter().all(|&rows| rows as u64 <= batch_size),
			"every chunk must stay within batch_size {batch_size}, got {:?}",
			chunk_sizes(&changes)
		);
		let ids: Vec<Value> = user_values(&changes).into_iter().map(|row| row[0].clone()).collect();
		assert_eq!(ids, [1, 2, 3, 4, 6, 7].map(Value::Int4).to_vec());
	}
}

#[test]
fn a_partitioned_ringbuffer_backfill_equals_from_without_the_partition_column() {
	// Each partition of a ring buffer has its own slot range; a scan of one partition only would drop rows.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE RINGBUFFER ns::rb { region: utf8, n: int4 } WITH { capacity: 10, partition: { by: { region } } }",
		"INSERT ns::rb [{ region: 'eu', n: 1 }, { region: 'us', n: 2 }, { region: 'eu', n: 3 }, { region: 'ap', n: 4 }]",
	]);
	db.commit(&["DELETE ns::rb FILTER { n == 3 }", "INSERT ns::rb [{ region: 'us', n: 5 }]"]);
	let mut q = db.query();
	let rb = db.id(&mut Transaction::Query(&mut q), Kind::RingBuffer, "rb");
	for batch_size in [1, 3, 100] {
		let (v, changes) = db.snapshot(Transaction::Query(&mut q), &[rb], batch_size);
		let from = db.from(&mut Transaction::Query(&mut q), "ns::rb");
		assert_matches_from(&changes, rb, v, &from, &DROPPED);
		assert!(chunk_sizes(&changes).iter().all(|&rows| rows as u64 <= batch_size));
		let ns: Vec<Value> = user_values(&changes).into_iter().map(|row| row[1].clone()).collect();
		let mut ns = ns;
		ns.sort();
		assert_eq!(ns, [1, 2, 4, 5].map(Value::Int4).to_vec());
	}
}

#[test]
fn several_sources_are_scanned_one_after_another_in_object_id_order_at_one_version() {
	// every source of one backfill is read at the same V; interleaved sources would break per-source chunking.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4, v: int4 }",
		"CREATE TRANSACTIONAL VIEW ns::v { id: int4, v: int4 } AS { FROM ns::t }",
		"CREATE RINGBUFFER ns::rb { a: int4 } WITH { capacity: 10 }",
		"CREATE SERIES ns::s { ts: int8, val: int4 } WITH { key: ts }",
		"INSERT ns::t [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }]",
		"INSERT ns::rb [{ a: 1 }, { a: 2 }, { a: 3 }]",
		"INSERT ns::s [{ ts: 1, val: 1 }, { ts: 2, val: 2 }, { ts: 3, val: 3 }]",
	]);
	let mut q = db.query();
	let t = db.id(&mut Transaction::Query(&mut q), Kind::Table, "t");
	let view = db.id(&mut Transaction::Query(&mut q), Kind::View, "v");
	let rb = db.id(&mut Transaction::Query(&mut q), Kind::RingBuffer, "rb");
	let s = db.id(&mut Transaction::Query(&mut q), Kind::Series, "s");
	let (v, changes) = db.snapshot(Transaction::Query(&mut q), &[s, rb, view, t], 2);
	let origins: Vec<ObjectId> = changes
		.iter()
		.map(|change| match change.origin {
			ChangeOrigin::Object(source) => source,
			ref other => panic!("a snapshot change must come from a source object, got {other:?}"),
		})
		.collect();
	let mut expected = origins.clone();
	expected.sort();
	assert_eq!(origins, expected, "sources must be scanned in ObjectId order, one after another");
	let mut distinct = origins.clone();
	distinct.dedup();
	assert_eq!(distinct, vec![t, view, rb, s], "every non-empty source must be scanned exactly once");
	for (source, name) in [(t, "ns::t"), (view, "ns::v"), (rb, "ns::rb"), (s, "ns::s")] {
		let own: Vec<Change> = changes
			.iter()
			.filter(|change| change.origin == ChangeOrigin::Object(source))
			.cloned()
			.collect();
		let from = db.from(&mut Transaction::Query(&mut q), name);
		assert_matches_from(&own, source, v, &from, &DROPPED);
	}
}

#[test]
fn a_commit_after_the_query_txn_began_is_invisible_to_its_backfill() {
	// The scan must read at the txn's own snapshot; reading latest would show the later insert, update and delete.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4, v: int4 }",
		"INSERT ns::t [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }]",
	]);
	let mut q = db.query();
	let t = db.id(&mut Transaction::Query(&mut q), Kind::Table, "t");
	let later = db.commit(&[
		"INSERT ns::t [{ id: 4, v: 40 }]",
		"UPDATE ns::t { v: 11 } FILTER { id == 1 }",
		"DELETE ns::t FILTER { id == 2 }",
	]);
	let (v, changes) = db.snapshot(Transaction::Query(&mut q), &[t], 2);
	assert!(later > v, "the concurrent commit must land after V, got V {v:?} and commit {later:?}");
	assert_eq!(user_values(&changes), pairs(&[(1, 10), (2, 20), (3, 30)]));
	let mut fresh = db.query();
	assert_eq!(
		from_user_values(&db.from(&mut Transaction::Query(&mut fresh), "ns::t")),
		pairs(&[(1, 11), (3, 30), (4, 40)]),
		"the concurrent commit must really have landed"
	);
}

#[test]
fn an_admin_txn_backfill_sees_its_own_writes_and_not_a_commit_made_after_it_began() {
	// A scan inside a write txn must show its own pending rows and never a concurrent commit.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4, v: int4 }",
		"INSERT ns::t [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }]",
	]);
	let mut txn = db.admin();
	db.exec(&mut txn, "INSERT ns::t [{ id: 10, v: 100 }]");
	db.exec(&mut txn, "UPDATE ns::t { v: 31 } FILTER { id == 3 }");
	let t = db.id(&mut Transaction::Admin(&mut txn), Kind::Table, "t");
	let later = db.commit(&["INSERT ns::t [{ id: 4, v: 40 }]", "DELETE ns::t FILTER { id == 1 }"]);
	let (v, changes) = db.snapshot(Transaction::Admin(&mut txn), &[t], 2);
	assert_eq!(v, txn.version(), "V is the admin txn's read version");
	assert!(later > v);
	assert_eq!(user_values(&changes), pairs(&[(1, 10), (2, 20), (3, 31), (10, 100)]));
	let from = db.from(&mut Transaction::Admin(&mut txn), "ns::t");
	assert_matches_from(&changes, t, v, &from, &DROPPED);
	txn.rollback().unwrap();
}

#[test]
fn a_command_txn_backfill_sees_its_own_uncommitted_inserts_updates_and_deletes() {
	// A backfill before the txn commits must include the txn's own writes in the snapshot.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4, v: int4 }",
		"INSERT ns::t [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }]",
	]);
	let mut cmd = db.command();
	db.exec_command(&mut cmd, "INSERT ns::t [{ id: 4, v: 40 }]");
	db.exec_command(&mut cmd, "UPDATE ns::t { v: 11 } FILTER { id == 1 }");
	db.exec_command(&mut cmd, "DELETE ns::t FILTER { id == 2 }");
	let t = db.id(&mut Transaction::Command(&mut cmd), Kind::Table, "t");
	let (v, changes) = db.snapshot(Transaction::Command(&mut cmd), &[t], 2);
	assert_eq!(v, cmd.version(), "V is the command txn's read version");
	assert_eq!(user_values(&changes), pairs(&[(1, 11), (3, 30), (4, 40)]));
	let from = db.from(&mut Transaction::Command(&mut cmd), "ns::t");
	assert_matches_from(&changes, t, v, &from, &DROPPED);
	let committed = cmd.commit().unwrap();
	assert!(committed > v, "the read version V must precede the commit version");
}

#[test]
fn an_admin_txn_backfill_sees_a_table_and_view_it_created_and_filled_itself() {
	// A view over rows inserted in the same uncommitted txn must see that txn's table and rows.
	let db = Db::new();
	let mut txn = db.admin();
	db.exec(&mut txn, "CREATE NAMESPACE ns");
	db.exec(&mut txn, "CREATE TABLE ns::t { id: int4, v: int4 }");
	db.exec(&mut txn, "INSERT ns::t [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }]");
	db.exec(&mut txn, "CREATE TRANSACTIONAL VIEW ns::v { id: int4, v: int4 } AS { FROM ns::t }");
	db.exec(&mut txn, "INSERT ns::t [{ id: 4, v: 40 }]");
	db.exec(&mut txn, "DELETE ns::t FILTER { id == 2 }");
	let t = db.id(&mut Transaction::Admin(&mut txn), Kind::Table, "t");
	let view = db.id(&mut Transaction::Admin(&mut txn), Kind::View, "v");
	let (v, changes) = db.snapshot(Transaction::Admin(&mut txn), &[t, view], 1);
	let of = |source: ObjectId| -> Vec<Change> {
		changes.iter().filter(|change| change.origin == ChangeOrigin::Object(source)).cloned().collect()
	};
	assert_eq!(user_values(&of(t)), pairs(&[(1, 10), (3, 30), (4, 40)]));
	let from_t = db.from(&mut Transaction::Admin(&mut txn), "ns::t");
	assert_matches_from(&of(t), t, v, &from_t, &DROPPED);
	let from_v = db.from(&mut Transaction::Admin(&mut txn), "ns::v");
	assert_matches_from(&of(view), view, v, &from_v, &DROPPED);
	assert_eq!(
		user_values(&of(view)),
		pairs(&[(1, 10), (3, 30), (4, 40)]),
		"the view holds the rows at its create plus the later writes"
	);
	txn.rollback().unwrap();
}

#[test]
fn a_transactional_view_written_earlier_in_the_same_txn_is_seen_by_the_backfill() {
	// A view over a transactional view is backfilled in the txn that feeds it; its pending rows must be visible.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4, v: int4 }",
		"CREATE TRANSACTIONAL VIEW ns::v { id: int4, v: int4 } AS { FROM ns::t }",
		"INSERT ns::t [{ id: 1, v: 10 }, { id: 2, v: 20 }]",
	]);
	let mut cmd = db.command();
	db.exec_command(&mut cmd, "INSERT ns::t [{ id: 3, v: 30 }]");
	db.exec_command(&mut cmd, "UPDATE ns::t { v: 11 } FILTER { id == 1 }");
	db.exec_command(&mut cmd, "DELETE ns::t FILTER { id == 2 }");
	let view = db.id(&mut Transaction::Command(&mut cmd), Kind::View, "v");
	let (v, changes) = db.snapshot(Transaction::Command(&mut cmd), &[view], 2);
	assert_eq!(user_values(&changes), pairs(&[(1, 11), (3, 30)]));
	let from = db.from(&mut Transaction::Command(&mut cmd), "ns::v");
	assert_matches_from(&changes, view, v, &from, &DROPPED);
	cmd.rollback().unwrap();
}

#[test]
fn a_deferred_view_whose_upstream_this_txn_wrote_is_an_error_not_stale_rows() {
	// The deferred view cannot hold this txn's upstream writes yet; scanning it anyway would seed a stale snapshot.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4, v: int4 }",
		"CREATE DEFERRED VIEW ns::dv { id: int4, v: int4 } AS { FROM ns::t }",
		"INSERT ns::t [{ id: 1, v: 10 }]",
	]);
	db.mirror("t", "dv");
	let mut txn = db.admin();
	db.exec(&mut txn, "INSERT ns::t [{ id: 2, v: 20 }]");
	let t = db.id(&mut Transaction::Admin(&mut txn), Kind::Table, "t");
	let dv = db.id(&mut Transaction::Admin(&mut txn), Kind::View, "dv");
	let (result, changes) = db.backfill(Transaction::Admin(&mut txn), &[dv], 2);
	let err = result.unwrap_err();
	assert_eq!(err.0.code, "TXN_015", "a stale deferred view read must fail loud, got {err:?}");
	assert!(changes.is_empty(), "no stale view row may reach consume");
	let (_, changes) = db.snapshot(Transaction::Admin(&mut txn), &[t], 2);
	assert_eq!(user_values(&changes), pairs(&[(1, 10), (2, 20)]), "the upstream table itself stays scannable");
	txn.rollback().unwrap();
}

#[test]
fn a_query_txn_at_an_older_leased_version_does_not_see_later_commits() {
	// A scan at a leased V must keep every commit after the lease out of the snapshot.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4, v: int4 }",
		"INSERT ns::t [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }]",
	]);
	let (leased, lease) = db.multi.acquire_current_snapshot_lease().unwrap();
	let later = db.commit(&[
		"INSERT ns::t [{ id: 4, v: 40 }]",
		"UPDATE ns::t { v: 11 } FILTER { id == 1 }",
		"DELETE ns::t FILTER { id == 2 }",
	]);
	assert!(later > leased);
	let mut old = QueryTransaction::new(
		db.multi.begin_query_at_version(&lease).unwrap(),
		db.single.clone(),
		IdentityId::system(),
	);
	let t = db.id(&mut Transaction::Query(&mut old), Kind::Table, "t");
	let (v, changes) = db.snapshot(Transaction::Query(&mut old), &[t], 2);
	assert_eq!(v, leased, "V must be the leased version");
	assert_eq!(user_values(&changes), pairs(&[(1, 10), (2, 20), (3, 30)]));
	let from = db.from(&mut Transaction::Query(&mut old), "ns::t");
	assert_matches_from(&changes, t, v, &from, &DROPPED);
	drop(lease);
}

#[test]
fn chunks_stay_within_batch_size_and_union_to_the_source_exactly() {
	// A wrong resume key repeats or skips a row at a chunk edge; an ignored batch_size gives one huge chunk.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4, v: int4 }",
		"CREATE TRANSACTIONAL VIEW ns::v { id: int4, v: int4 } AS { FROM ns::t }",
		"CREATE RINGBUFFER ns::rb { a: int4 } WITH { capacity: 10 }",
		"CREATE SERIES ns::s { ts: int8, val: int4 } WITH { key: ts }",
		"INSERT ns::t [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }, { id: 4, v: 40 }, { id: 5, v: 50 }, { id: 6, v: 60 }]",
		"INSERT ns::rb [{ a: 1 }, { a: 2 }, { a: 3 }, { a: 4 }, { a: 5 }, { a: 6 }]",
		"INSERT ns::s [{ ts: 1, val: 1 }, { ts: 2, val: 2 }, { ts: 3, val: 3 }, { ts: 4, val: 4 }, { ts: 5, val: 5 }, { ts: 6, val: 6 }]",
	]);
	let mut q = db.query();
	let sources = [
		(db.id(&mut Transaction::Query(&mut q), Kind::Table, "t"), "ns::t"),
		(db.id(&mut Transaction::Query(&mut q), Kind::View, "v"), "ns::v"),
		(db.id(&mut Transaction::Query(&mut q), Kind::RingBuffer, "rb"), "ns::rb"),
		(db.id(&mut Transaction::Query(&mut q), Kind::Series, "s"), "ns::s"),
	];
	let cases: [(u64, Vec<usize>); 6] = [
		(1, vec![1, 1, 1, 1, 1, 1]),
		(2, vec![2, 2, 2]),
		(3, vec![3, 3]),
		(5, vec![5, 1]),
		(6, vec![6]),
		(7, vec![6]),
	];
	for (source, name) in sources {
		let from = db.from(&mut Transaction::Query(&mut q), name);
		for (batch_size, expected) in &cases {
			let (v, changes) = db.snapshot(Transaction::Query(&mut q), &[source], *batch_size);
			assert_eq!(&chunk_sizes(&changes), expected, "{name} at batch_size {batch_size}");
			assert_matches_from(&changes, source, v, &from, &DROPPED);
		}
	}
}

#[test]
fn chunks_over_a_table_with_deleted_rows_have_no_gap_or_duplicate() {
	// Deleted rows leave holes in the key range; a resume that counts holes as rows would lose or repeat rows.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4, v: int4 }",
		"INSERT ns::t [{ id: 1, v: 1 }, { id: 2, v: 2 }, { id: 3, v: 3 }, { id: 4, v: 4 }, { id: 5, v: 5 }, { id: 6, v: 6 }, { id: 7, v: 7 }, { id: 8, v: 8 }, { id: 9, v: 9 }, { id: 10, v: 10 }]",
	]);
	db.commit(&["DELETE ns::t FILTER { id == 2 or id == 3 or id == 4 or id == 7 }"]);
	let mut q = db.query();
	let t = db.id(&mut Transaction::Query(&mut q), Kind::Table, "t");
	let from = db.from(&mut Transaction::Query(&mut q), "ns::t");
	for batch_size in [1, 2, 3, 4, 6, 7] {
		let (v, changes) = db.snapshot(Transaction::Query(&mut q), &[t], batch_size);
		let sizes = chunk_sizes(&changes);
		assert!(
			sizes.iter().all(|&rows| rows as u64 <= batch_size),
			"batch_size {batch_size} gave chunks {sizes:?}"
		);
		assert_matches_from(&changes, t, v, &from, &DROPPED);
		assert_eq!(user_values(&changes), pairs(&[(1, 1), (5, 5), (6, 6), (8, 8), (9, 9), (10, 10)]));
	}
}

#[test]
fn dictionary_columns_are_decoded_exactly_as_from_shows_them() {
	// Live flows see decoded values; a snapshot handing over raw dictionary ids would write ids into views.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE DICTIONARY ns::syms FOR utf8 AS uint2",
		"CREATE TABLE ns::t { id: int4, sym: utf8 with { dictionary: ns::syms } }",
		"CREATE RINGBUFFER ns::rb { id: int4, sym: utf8 with { dictionary: ns::syms } } WITH { capacity: 10 }",
		"CREATE SERIES ns::s { id: int8, sym: utf8 with { dictionary: ns::syms } } WITH { key: id }",
		"INSERT ns::t [{ id: 1, sym: 'sol' }, { id: 2, sym: 'eth' }, { id: 3, sym: 'sol' }]",
		"INSERT ns::rb [{ id: 1, sym: 'sol' }, { id: 2, sym: 'eth' }, { id: 3, sym: 'sol' }]",
		"INSERT ns::s [{ id: 1, sym: 'sol' }, { id: 2, sym: 'eth' }, { id: 3, sym: 'sol' }]",
	]);
	db.commit(&[
		"UPDATE ns::t { sym: 'btc' } FILTER { id == 2 }",
		"UPDATE ns::rb { sym: 'btc' } FILTER { id == 2 }",
		"UPDATE ns::s { sym: 'btc' } FILTER { id == 2 }",
	]);
	let mut q = db.query();
	let t = db.id(&mut Transaction::Query(&mut q), Kind::Table, "t");
	let rb = db.id(&mut Transaction::Query(&mut q), Kind::RingBuffer, "rb");
	let s = db.id(&mut Transaction::Query(&mut q), Kind::Series, "s");
	let (v, changes) = db.snapshot(Transaction::Query(&mut q), &[t, rb, s], 2);
	let symbols = [text("sol"), text("btc"), text("sol")];
	for (source, name, id) in [
		(t, "ns::t", Value::Int4 as fn(i32) -> Value),
		(rb, "ns::rb", Value::Int4 as fn(i32) -> Value),
		(s, "ns::s", (|id| Value::Int8(id as i64)) as fn(i32) -> Value),
	] {
		let own: Vec<Change> = changes
			.iter()
			.filter(|change| change.origin == ChangeOrigin::Object(source))
			.cloned()
			.collect();
		let from = db.from(&mut Transaction::Query(&mut q), name);
		assert_matches_from(&own, source, v, &from, &DROPPED);
		let expected: Vec<Vec<Value>> = symbols
			.iter()
			.enumerate()
			.map(|(index, sym)| vec![id(index as i32 + 1), sym.clone()])
			.collect();
		assert_eq!(user_values(&own), expected, "{name} must hand over decoded symbols");
	}
}

#[test]
fn a_source_kind_without_a_scan_node_is_an_error_not_a_panic() {
	// Consumers must leave queues, dictionaries and virtual tables out; if one slips in, the run must fail loud.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE QUEUE ns::q { id: int4 } WITH { fifo: { partitions: 1 } }",
		"CREATE DICTIONARY ns::syms FOR utf8 AS uint2",
	]);
	let mut q = db.query();
	let queue = db.id(&mut Transaction::Query(&mut q), Kind::Queue, "q");
	let dictionary = db.id(&mut Transaction::Query(&mut q), Kind::Dictionary, "syms");
	for source in [queue, dictionary, ObjectId::vtable(TABLES)] {
		let (result, changes) = db.backfill(Transaction::Query(&mut q), &[source], 2);
		let err = result.unwrap_err();
		assert_eq!(err.0.code, "INTERNAL_ERROR", "{source:?}: {err:?}");
		assert!(err.0.message.contains("has no row scan node"), "{source:?}: {}", err.0.message);
		assert!(changes.is_empty());
	}
}

#[test]
fn an_unscannable_source_stops_the_run_after_the_sources_before_it() {
	// run stops at the first error; a later source must never be scanned after an earlier one failed.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4, v: int4 }",
		"CREATE QUEUE ns::q { id: int4 } WITH { fifo: { partitions: 1 } }",
		"CREATE DICTIONARY ns::syms FOR utf8 AS uint2",
		"CREATE SERIES ns::s { ts: int8, val: int4 } WITH { key: ts }",
		"INSERT ns::t [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }]",
		"INSERT ns::s [{ ts: 1, val: 1 }]",
	]);
	let mut q = db.query();
	let t = db.id(&mut Transaction::Query(&mut q), Kind::Table, "t");
	let queue = db.id(&mut Transaction::Query(&mut q), Kind::Queue, "q");
	let dictionary = db.id(&mut Transaction::Query(&mut q), Kind::Dictionary, "syms");
	let s = db.id(&mut Transaction::Query(&mut q), Kind::Series, "s");
	let (result, changes) = db.backfill(Transaction::Query(&mut q), &[queue, t], 2);
	assert_eq!(result.unwrap_err().0.code, "INTERNAL_ERROR");
	assert!(changes.iter().all(|change| change.origin == ChangeOrigin::Object(t)));
	assert_eq!(
		user_values(&changes),
		pairs(&[(1, 10), (2, 20), (3, 30)]),
		"the table before the queue is scanned in full"
	);
	let (result, changes) = db.backfill(Transaction::Query(&mut q), &[s, dictionary], 2);
	assert_eq!(result.unwrap_err().0.code, "INTERNAL_ERROR");
	assert!(changes.is_empty(), "the series after the dictionary must not be scanned, got {changes:?}");
}

#[test]
fn a_dropped_or_missing_source_is_an_error() {
	// A source id from a stale set must fail, never read as an empty source and silently seed an empty view.
	let db = Db::new();
	db.commit(&["CREATE NAMESPACE ns", "CREATE TABLE ns::gone { id: int4 }", "INSERT ns::gone [{ id: 1 }]"]);
	let mut before = db.query();
	let gone = db.id(&mut Transaction::Query(&mut before), Kind::Table, "gone");
	db.commit(&["DROP TABLE ns::gone"]);
	let mut q = db.query();
	let (result, changes) = db.backfill(Transaction::Query(&mut q), &[gone], 2);
	assert!(result.is_err(), "a dropped table must be an error, got {result:?}");
	assert!(changes.is_empty());
	let (result, changes) = db.backfill(Transaction::Query(&mut q), &[ObjectId::table(TableId(987_654))], 2);
	assert!(result.is_err(), "a table id that never existed must be an error, got {result:?}");
	assert!(changes.is_empty());
}

#[test]
fn an_error_from_consume_propagates_unchanged_and_stops_the_run() {
	// A failing consumer must abort the backfill with its own error, never swallowed or rewrapped.
	let db = Db::new();
	six_rows(&db);
	let mut q = db.query();
	let t = db.id(&mut Transaction::Query(&mut q), Kind::Table, "t");
	let mut calls = 0;
	let result = run(db.executor.services(), Transaction::Query(&mut q), &set(&[t]), size(2), |_, _| {
		calls += 1;
		Err(error("TEST_CONSUME"))
	});
	assert_eq!(result.unwrap_err().0.code, "TEST_CONSUME");
	assert_eq!(calls, 1, "no chunk may be handed over after consume failed");
}

#[test]
fn empty_sources_give_no_changes_and_still_return_v() {
	// An empty source must not produce an empty Change or an error; the consumer still needs V for its cursor.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4 }",
		"CREATE TABLE ns::cleared { id: int4 }",
		"CREATE TRANSACTIONAL VIEW ns::v { id: int4 } AS { FROM ns::t }",
		"CREATE DEFERRED VIEW ns::dv { id: int4 } AS { FROM ns::t }",
		"CREATE RINGBUFFER ns::rb { a: int4 } WITH { capacity: 10 }",
		"CREATE RINGBUFFER ns::drained { a: int4 } WITH { capacity: 10 }",
		"CREATE SERIES ns::s { ts: int8, val: int4 } WITH { key: ts }",
		"INSERT ns::cleared [{ id: 1 }, { id: 2 }]",
		"INSERT ns::drained [{ a: 1 }, { a: 2 }]",
	]);
	db.commit(&["DELETE ns::cleared FILTER { id > 0 }", "DELETE ns::drained FILTER { a > 0 }"]);
	let mut q = db.query();
	let sources = [
		db.id(&mut Transaction::Query(&mut q), Kind::Table, "t"),
		db.id(&mut Transaction::Query(&mut q), Kind::Table, "cleared"),
		db.id(&mut Transaction::Query(&mut q), Kind::View, "v"),
		db.id(&mut Transaction::Query(&mut q), Kind::View, "dv"),
		db.id(&mut Transaction::Query(&mut q), Kind::RingBuffer, "rb"),
		db.id(&mut Transaction::Query(&mut q), Kind::RingBuffer, "drained"),
		db.id(&mut Transaction::Query(&mut q), Kind::Series, "s"),
	];
	for source in sources {
		let (v, changes) = db.snapshot(Transaction::Query(&mut q), &[source], 2);
		assert_eq!(v, q.version(), "{source:?}");
		assert!(changes.is_empty(), "{source:?} is empty but gave {changes:?}");
	}
	let (v, changes) = db.snapshot(Transaction::Query(&mut q), &sources, 1);
	assert_eq!(v, q.version());
	assert!(changes.is_empty());
}

#[test]
fn a_raw_scan_of_an_empty_source_ends_after_at_most_one_empty_chunk() {
	// A scan that never returns None on an empty source would hang every backfill over it.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4 }",
		"CREATE TRANSACTIONAL VIEW ns::v { id: int4 } AS { FROM ns::t }",
		"CREATE RINGBUFFER ns::rb { a: int4 } WITH { capacity: 10 }",
		"CREATE SERIES ns::s { ts: int8, val: int4 } WITH { key: ts }",
	]);
	let mut q = db.query();
	let sources = [
		db.id(&mut Transaction::Query(&mut q), Kind::Table, "t"),
		db.id(&mut Transaction::Query(&mut q), Kind::View, "v"),
		db.id(&mut Transaction::Query(&mut q), Kind::RingBuffer, "rb"),
		db.id(&mut Transaction::Query(&mut q), Kind::Series, "s"),
	];
	let mut scan = db.raw_scan(Transaction::Query(&mut q));
	for source in sources {
		scan.open(source, size(4)).unwrap();
		let mut empties = 0;
		loop {
			match scan.next().unwrap() {
				None => break,
				Some(chunk) => {
					assert_eq!(chunk.num_rows(), 0, "{source:?} is empty but gave rows");
					empties += 1;
					assert!(empties <= 1, "{source:?} kept giving empty chunks instead of ending");
				}
			}
		}
	}
}

#[test]
fn a_raw_scan_pulled_before_any_open_is_an_internal_error() {
	// A pull with no source opened is a caller bug; it must surface as an error, not a panic or an empty result.
	let db = Db::new();
	let mut q = db.query();
	let mut scan = db.raw_scan(Transaction::Query(&mut q));
	let err = scan.next().unwrap_err();
	assert_eq!(err.0.code, "INTERNAL_ERROR", "{err:?}");
}

#[test]
fn a_refused_open_keeps_pulling_the_source_already_open() {
	// A failed open that dropped the open source would lose its remaining rows in the fault sweep.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4, v: int4 }",
		"CREATE QUEUE ns::q { id: int4 } WITH { fifo: { partitions: 1 } }",
		"INSERT ns::t [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }]",
	]);
	let mut q = db.query();
	let t = db.id(&mut Transaction::Query(&mut q), Kind::Table, "t");
	let queue = db.id(&mut Transaction::Query(&mut q), Kind::Queue, "q");
	let expected: BTreeSet<RowNumber> = {
		let mut own = db.query();
		let from = db.from(&mut Transaction::Query(&mut own), "ns::t");
		from.iter().flat_map(|batch| require_row_numbers(batch).unwrap().to_vec()).collect()
	};
	let mut scan = db.raw_scan(Transaction::Query(&mut q));
	scan.open(t, size(1)).unwrap();
	let mut seen = Vec::new();
	let first = scan.next().unwrap().expect("a table with rows must give a first chunk");
	seen.extend(require_row_numbers(&first).unwrap().to_vec());
	assert!(scan.open(queue, size(1)).is_err());
	while let Some(chunk) = scan.next().unwrap() {
		if chunk.num_rows() > 0 {
			seen.extend(require_row_numbers(&chunk).unwrap().to_vec());
		}
	}
	assert_eq!(seen.len(), 3, "every row of the open table must still arrive exactly once, got {seen:?}");
	assert_eq!(seen.into_iter().collect::<BTreeSet<_>>(), expected);
}

#[test]
fn consume_can_write_through_the_scan_transaction_into_the_same_commit() {
	// Writes made from consume must land in the scanned txn and commit with it.
	let db = Db::new();
	db.commit(&[
		"CREATE NAMESPACE ns",
		"CREATE TABLE ns::t { id: int4, v: int4 }",
		"CREATE TABLE ns::out { id: int4 }",
		"INSERT ns::t [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }]",
	]);
	let mut txn = db.admin();
	let t = db.id(&mut Transaction::Admin(&mut txn), Kind::Table, "t");
	let executor = db.executor.clone();
	let mut chunks = 0;
	run(db.executor.services(), Transaction::Admin(&mut txn), &set(&[t]), size(1), |scan, change| {
		chunks += 1;
		for row in rows_of(post(&change)) {
			let Value::Int4(id) = row.values[0] else {
				panic!("id must be int4, got {:?}", row.values[0]);
			};
			let rql = format!("INSERT ns::out [{{ id: {id} }}]");
			if let Some(e) = executor.rql(scan.transaction(), &rql, Params::default()).error {
				return Err(e);
			}
		}
		Ok(())
	})
	.unwrap();
	assert_eq!(chunks, 3);
	txn.commit().unwrap();
	let mut q = db.query();
	let expected: Vec<Vec<Value>> = [1, 2, 3].iter().map(|&id| vec![Value::Int4(id)]).collect();
	assert_eq!(from_user_values(&db.from(&mut Transaction::Query(&mut q), "ns::out")), expected);
	assert_eq!(
		from_user_values(&db.from(&mut Transaction::Query(&mut q), "ns::t")),
		pairs(&[(1, 10), (2, 20), (3, 30)])
	);
}

#[cfg(feature = "testing")]
#[derive(Clone, Copy, PartialEq)]
enum Point {
	Open,
	Next(u64),
}

#[cfg(feature = "testing")]
struct CommitDuring {
	db: Db,
	point: Point,
	statements: &'static [&'static str],
	landed: Mutex<Option<CommitVersion>>,
}

#[cfg(feature = "testing")]
impl CommitDuring {
	fn install(db: &Db, point: Point, statements: &'static [&'static str]) -> Arc<Self> {
		// Registered in the engine IoC so run() wraps the adapter exactly as the production hook path does.
		let hook = Arc::new(Self {
			db: db.clone(),
			point,
			statements,
			landed: Mutex::new(None),
		});
		db.executor.ioc.register_service(InstalledScanHooks(hook.clone()));
		hook
	}

	fn land(&self) {
		let mut landed = self.landed.lock();
		if landed.is_none() {
			*landed = Some(self.db.commit(self.statements));
		}
	}

	fn landed(&self) -> CommitVersion {
		self.landed.lock().expect("the hook must fire, else run() ignores the installed scan hooks")
	}
}

#[cfg(feature = "testing")]
impl ScanHooks for CommitDuring {
	fn during_open(&self, _source: ObjectId) {
		if self.point == Point::Open {
			self.land();
		}
	}

	fn during_next(&self, _source: ObjectId, pull: u64) {
		if self.point == Point::Next(pull) {
			self.land();
		}
	}
}

#[cfg(feature = "testing")]
const CONCURRENT: &[&str] = &[
	"INSERT ns::t [{ id: 7, v: 70 }, { id: 8, v: 80 }]",
	"UPDATE ns::t { v: 11 } FILTER { id == 1 }",
	"UPDATE ns::t { v: 61 } FILTER { id == 6 }",
	"DELETE ns::t FILTER { id == 3 or id == 4 }",
];

#[cfg(feature = "testing")]
#[test]
fn a_write_committed_between_chunks_is_invisible_to_a_query_scan() {
	// A per-chunk re-read at latest would leak the concurrent insert, update and delete into later chunks.
	let db = Db::new();
	six_rows(&db);
	let hook = CommitDuring::install(&db, Point::Next(1), CONCURRENT);
	let mut q = db.query();
	let t = db.id(&mut Transaction::Query(&mut q), Kind::Table, "t");
	let (v, changes) = db.snapshot(Transaction::Query(&mut q), &[t], 2);
	assert!(hook.landed() > v, "the hook commit must land after V");
	assert_eq!(chunk_sizes(&changes), vec![2, 2, 2]);
	assert_eq!(user_values(&changes), pairs(&[(1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)]));
	let from = db.from(&mut Transaction::Query(&mut q), "ns::t");
	assert_matches_from(&changes, t, v, &from, &DROPPED);
	let mut fresh = db.query();
	assert_eq!(
		from_user_values(&db.from(&mut Transaction::Query(&mut fresh), "ns::t")),
		pairs(&[(1, 11), (2, 20), (5, 50), (6, 61), (7, 70), (8, 80)]),
		"the concurrent commit must really have landed"
	);
}

#[cfg(feature = "testing")]
#[test]
fn a_write_committed_between_chunks_is_invisible_to_an_admin_scan_that_still_sees_its_own_rows() {
	// A write txn scans at its read version plus its own writes, never at latest.
	let db = Db::new();
	six_rows(&db);
	let hook = CommitDuring::install(&db, Point::Next(2), CONCURRENT);
	let mut txn = db.admin();
	db.exec(&mut txn, "INSERT ns::t [{ id: 10, v: 100 }]");
	let t = db.id(&mut Transaction::Admin(&mut txn), Kind::Table, "t");
	let (v, changes) = db.snapshot(Transaction::Admin(&mut txn), &[t], 1);
	assert!(hook.landed() > v, "the hook commit must land after V");
	assert_eq!(chunk_sizes(&changes), vec![1; 7]);
	assert_eq!(user_values(&changes), pairs(&[(1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60), (10, 100)]));
	txn.rollback().unwrap();
}

#[cfg(feature = "testing")]
#[test]
fn a_write_committed_while_the_source_opens_is_invisible() {
	// during_open runs after V is fixed; building the scan node must not move the read version forward.
	let db = Db::new();
	six_rows(&db);
	let hook = CommitDuring::install(&db, Point::Open, CONCURRENT);
	let mut q = db.query();
	let t = db.id(&mut Transaction::Query(&mut q), Kind::Table, "t");
	let (v, changes) = db.snapshot(Transaction::Query(&mut q), &[t], 4);
	assert!(hook.landed() > v, "the hook commit must land after V");
	assert_eq!(user_values(&changes), pairs(&[(1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)]));
}
