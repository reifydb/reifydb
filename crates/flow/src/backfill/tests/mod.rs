// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, VecDeque},
	num::NonZeroU64,
	panic::{AssertUnwindSafe, catch_unwind},
	sync::Arc,
};

use arrow_array::RecordBatch;
use reifydb_core::{
	common::{ChangeVersion, CommitVersion, SourceVersion, TimeSource},
	interface::{
		catalog::{
			id::{RingBufferId, SeriesId, TableId, ViewId},
			object::ObjectId,
		},
		change::{Change, ChangeOrigin, Diff},
	},
	value::{
		batch::{batch, empty_batch},
		column::factory::{datetime, datetime_with_bitvec, int4, int8, uint8, uint16, utf8},
	},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{
	Result,
	error::{Diagnostic, Error},
	value::{
		Value,
		column_view::ColumnView,
		datetime::DateTime,
		row_number::RowNumber,
		system_columns::{
			SystemColumn, commit_versions, require_created_at, require_row_numbers, require_time,
			require_updated_at, system_column, user_columns,
		},
		value_type::ValueType,
	},
};

use crate::backfill::{
	Scan, backfill,
	memory::{MemoryScan, MemorySources},
	testing::{Continue, NoFaults, Outcome, ScanHooks, TestingScan},
};

const TRADES: ObjectId = ObjectId::Table(TableId(1));
const QUOTES: ObjectId = ObjectId::Table(TableId(2));
const POSITIONS: ObjectId = ObjectId::View(ViewId(3));
const TICKS: ObjectId = ObjectId::RingBuffer(RingBufferId(4));
const PRICES: ObjectId = ObjectId::Series(SeriesId(5));

fn version(number: u64) -> CommitVersion {
	CommitVersion(number)
}

fn at(seconds: i64) -> DateTime {
	DateTime::from_nanos(seconds * 1_000_000_000)
}

fn size(rows: u64) -> NonZeroU64 {
	NonZeroU64::new(rows).unwrap()
}

fn text(value: &str) -> Value {
	Value::Utf8(value.to_string())
}

fn set(sources: &[ObjectId]) -> BTreeSet<ObjectId> {
	sources.iter().copied().collect()
}

fn refused() -> Error {
	Error(Box::new(Diagnostic {
		code: "TEST_REFUSED".to_string(),
		..Default::default()
	}))
}

fn inner_failure() -> Error {
	Error(Box::new(Diagnostic {
		code: "TEST_INNER_FAILURE".to_string(),
		..Default::default()
	}))
}

fn define(sources: &MemorySources, source: ObjectId, time: TimeSource) {
	sources.define(source, &[("id", ValueType::Int4), ("symbol", ValueType::Utf8)], time);
}

fn define_prices(sources: &MemorySources) {
	sources.define(
		PRICES,
		&[("ts", ValueType::DateTime), ("px", ValueType::Int4)],
		TimeSource::Event {
			ts: "ts".to_string(),
		},
	);
}

fn values(id: i32, symbol: &str) -> Vec<Value> {
	vec![Value::Int4(id), text(symbol)]
}

fn fill(
	sources: &MemorySources,
	source: ObjectId,
	label: &str,
	count: i32,
	at_version: CommitVersion,
) -> Vec<RowNumber> {
	(1..=count)
		.map(|id| sources.insert(source, at_version, at(id as i64), values(id, &format!("{label}{id}"))))
		.collect()
}

#[derive(Debug, Clone, PartialEq)]
struct Row {
	number: RowNumber,
	values: Vec<Value>,
	created: DateTime,
	updated: DateTime,
	time: Option<DateTime>,
}

fn row(number: RowNumber, values: Vec<Value>, created: i64, updated: i64) -> Row {
	Row {
		number,
		values,
		created: at(created),
		updated: at(updated),
		time: None,
	}
}

fn timed(number: RowNumber, values: Vec<Value>, created: i64, updated: i64, time: i64) -> Row {
	Row {
		time: Some(at(time)),
		..row(number, values, created, updated)
	}
}

fn filled(id: i32, label: &str) -> Row {
	row(RowNumber(id as u64), values(id, &format!("{label}{id}")), id as i64, id as i64)
}

fn rows_of(batch: &RecordBatch) -> Vec<Row> {
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

fn numbers_of(batch: &RecordBatch) -> Vec<RowNumber> {
	require_row_numbers(batch).unwrap().to_vec()
}

fn names(batch: &RecordBatch) -> Vec<String> {
	batch.schema_ref().fields().iter().map(|field| field.name().clone()).collect()
}

fn post(change: &Change) -> &RecordBatch {
	assert_eq!(change.diffs.len(), 1, "a snapshot chunk must be exactly one diff");
	match &change.diffs[0] {
		Diff::Insert {
			post,
			origin: None,
		} => post,
		other => panic!("a snapshot chunk must be a plain insert with no diff origin, got {other:?}"),
	}
}

fn origin_of(change: &Change) -> ObjectId {
	match &change.origin {
		ChangeOrigin::Object(source) => *source,
		other => panic!("a snapshot change must originate from its source object, got {other:?}"),
	}
}

fn at_v(snapshot: CommitVersion) -> ChangeVersion {
	ChangeVersion {
		commit: snapshot,
		source: SourceVersion(snapshot.0),
	}
}

fn collect<S: Scan>(scan: &mut S, sources: &[ObjectId], batch_size: u64) -> Result<Vec<Change>> {
	let mut changes = Vec::new();
	backfill(scan, &set(sources), size(batch_size), |_, change| {
		changes.push(change);
		Ok(())
	})?;
	Ok(changes)
}

fn pull_all<S: Scan>(scan: &mut S, source: ObjectId, batch_size: u64) -> Vec<RecordBatch> {
	scan.open(source, size(batch_size)).unwrap();
	let mut chunks = Vec::new();
	while let Some(chunk) = scan.next().unwrap() {
		chunks.push(chunk);
	}
	chunks
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

#[derive(Debug, Clone, PartialEq)]
enum Event {
	Call(u64),
	DuringOpen(ObjectId),
	OnOpen(ObjectId),
	DuringNext(ObjectId, u64),
	OnNext(ObjectId, u64),
	Consumed(ObjectId, Vec<RowNumber>, u64),
}

struct Recorder {
	events: Mutex<Vec<Event>>,
}

impl Recorder {
	fn new() -> Self {
		Self {
			events: Mutex::new(Vec::new()),
		}
	}

	fn events(&self) -> Vec<Event> {
		self.events.lock().clone()
	}
}

impl ScanHooks for Recorder {
	fn on_open(&self, source: ObjectId) -> Outcome {
		self.events.lock().push(Event::OnOpen(source));
		Outcome::Land
	}

	fn on_next(&self, source: ObjectId, pull: u64) -> Outcome {
		self.events.lock().push(Event::OnNext(source, pull));
		Outcome::Land
	}

	fn during_open(&self, source: ObjectId) {
		self.events.lock().push(Event::DuringOpen(source));
	}

	fn during_next(&self, source: ObjectId, pull: u64) {
		self.events.lock().push(Event::DuringNext(source, pull));
	}

	fn on_call(&self, call: u64) -> Continue {
		self.events.lock().push(Event::Call(call));
		Continue::Yes
	}
}

struct CrashAt(u64);

impl ScanHooks for CrashAt {
	fn on_call(&self, call: u64) -> Continue {
		if call == self.0 {
			Continue::Crash
		} else {
			Continue::Yes
		}
	}
}

struct Refuse {
	open: Option<ObjectId>,
	next: Option<(ObjectId, u64)>,
	times: Mutex<u64>,
	pulls: Mutex<Vec<(ObjectId, u64)>>,
}

impl Refuse {
	fn never() -> Self {
		Self {
			open: None,
			next: None,
			times: Mutex::new(0),
			pulls: Mutex::new(Vec::new()),
		}
	}

	fn open(source: ObjectId) -> Self {
		Self {
			open: Some(source),
			..Self::never()
		}
	}

	fn next(source: ObjectId, pull: u64, times: u64) -> Self {
		Self {
			next: Some((source, pull)),
			times: Mutex::new(times),
			..Self::never()
		}
	}

	fn pulls(&self) -> Vec<(ObjectId, u64)> {
		self.pulls.lock().clone()
	}
}

impl ScanHooks for Refuse {
	fn on_open(&self, source: ObjectId) -> Outcome {
		if self.open == Some(source) {
			Outcome::Err(refused())
		} else {
			Outcome::Land
		}
	}

	fn on_next(&self, source: ObjectId, pull: u64) -> Outcome {
		self.pulls.lock().push((source, pull));
		let mut times = self.times.lock();
		if self.next == Some((source, pull)) && *times > 0 {
			*times -= 1;
			Outcome::Err(refused())
		} else {
			Outcome::Land
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Point {
	Open(ObjectId),
	Next(ObjectId, u64),
}

struct During<F>(F);

impl<F: Fn(Point) + Send + Sync> ScanHooks for During<F> {
	fn during_open(&self, source: ObjectId) {
		(self.0)(Point::Open(source))
	}

	fn during_next(&self, source: ObjectId, pull: u64) {
		(self.0)(Point::Next(source, pull))
	}
}

enum Scripted {
	Chunk(RecordBatch),
	Fail,
}

#[derive(Debug, Clone, PartialEq)]
enum Step {
	Open(ObjectId, u64),
	Next,
	Consumed(ObjectId, usize),
}

struct ScriptedScan {
	script: BTreeMap<ObjectId, VecDeque<Scripted>>,
	pending: VecDeque<Scripted>,
	log: Vec<Step>,
}

impl ScriptedScan {
	fn new(script: Vec<(ObjectId, Vec<Scripted>)>) -> Self {
		Self {
			script: script.into_iter().map(|(source, steps)| (source, steps.into())).collect(),
			pending: VecDeque::new(),
			log: Vec::new(),
		}
	}
}

const SCRIPTED_V: CommitVersion = CommitVersion(7);

impl Scan for ScriptedScan {
	fn version(&self) -> CommitVersion {
		SCRIPTED_V
	}

	fn open(&mut self, source: ObjectId, batch_size: NonZeroU64) -> Result<()> {
		self.log.push(Step::Open(source, batch_size.get()));
		self.pending = self.script.remove(&source).expect("every opened source is scripted exactly once");
		Ok(())
	}

	fn next(&mut self) -> Result<Option<RecordBatch>> {
		self.log.push(Step::Next);
		match self.pending.pop_front() {
			Some(Scripted::Chunk(chunk)) => Ok(Some(chunk)),
			Some(Scripted::Fail) => Err(inner_failure()),
			None => Ok(None),
		}
	}
}

fn stamped(ids: &[i32], updated: &[i64]) -> RecordBatch {
	assert_eq!(ids.len(), updated.len());
	batch(vec![
		int4("id", ids.iter().copied()),
		uint8("#rownum", 1..=ids.len() as u64),
		datetime("#created_at", updated.iter().map(|_| at(1))),
		datetime("#updated_at", updated.iter().map(|&seconds| at(seconds))),
	])
	.unwrap()
}

fn consume_into_log(scan: &mut ScriptedScan, change: Change) -> Result<()> {
	scan.log.push(Step::Consumed(origin_of(&change), post(&change).num_rows()));
	Ok(())
}

#[test]
fn memory_row_numbers_start_at_one_per_source_and_latest_tracks_the_highest_write() {
	// row numbers are per source and dense; latest() is zero before any write and never lags the highest version
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	define(&sources, QUOTES, TimeSource::None);
	assert_eq!(sources.latest(), version(0));

	let first = sources.insert(TRADES, version(1), at(1), values(1, "a"));
	let quote = sources.insert(QUOTES, version(1), at(1), values(1, "q"));
	let second = sources.insert(TRADES, version(2), at(2), values(2, "b"));
	sources.delete(TRADES, first, version(3));
	let third = sources.insert(TRADES, version(3), at(3), values(3, "c"));

	assert_eq!([first, second, third], [RowNumber(1), RowNumber(2), RowNumber(3)]);
	assert_eq!(quote, RowNumber(1));
	assert_eq!(sources.latest(), version(3));
	assert_eq!(sources.scan_at(version(2)).version(), version(2));
}

#[test]
fn memory_table_chunks_carry_each_rows_last_commit_version_at_or_before_v() {
	// #commit_version must be the row's own last write at or before V, never a later write and never V itself
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	let early = sources.insert(TRADES, version(1), at(1), values(1, "a"));
	sources.insert(TRADES, version(2), at(2), values(2, "b"));
	sources.update(TRADES, early, version(3), at(3), values(1, "a3"));

	let at_two = pull_all(&mut sources.scan_at(version(2)), TRADES, 10);
	let at_three = pull_all(&mut sources.scan_at(version(3)), TRADES, 10);
	let at_five = pull_all(&mut sources.scan_at(version(5)), TRADES, 10);

	assert_eq!(names(&at_two[0]), ["id", "symbol", "#rownum", "#created_at", "#updated_at", "#commit_version"]);
	assert_eq!(commit_versions(&at_two[0]).unwrap(), [1, 2]);
	assert_eq!(commit_versions(&at_three[0]).unwrap(), [3, 2]);
	assert_eq!(commit_versions(&at_five[0]).unwrap(), [3, 2]);
	assert_eq!(rows_of(&at_two[0])[0].values, values(1, "a"));
	assert_eq!(rows_of(&at_three[0])[0].values, values(1, "a3"));
}

#[test]
fn memory_chunks_of_views_ring_buffers_and_series_have_no_commit_version() {
	// only table scans stamp #commit_version; other source kinds must never carry it
	let sources = MemorySources::default();
	define(&sources, POSITIONS, TimeSource::None);
	define(&sources, TICKS, TimeSource::Processing);
	define_prices(&sources);
	sources.insert(POSITIONS, version(1), at(1), values(1, "p"));
	sources.insert(TICKS, version(1), at(1), values(1, "k"));
	sources.insert(PRICES, version(1), at(1), vec![Value::DateTime(at(100)), Value::Int4(9)]);
	let mut scan = sources.scan_at(version(1));

	assert_eq!(
		names(&pull_all(&mut scan, POSITIONS, 10)[0]),
		["id", "symbol", "#rownum", "#created_at", "#updated_at"]
	);
	assert_eq!(
		names(&pull_all(&mut scan, TICKS, 10)[0]),
		["id", "symbol", "#rownum", "#created_at", "#updated_at", "#time"]
	);
	assert_eq!(
		names(&pull_all(&mut scan, PRICES, 10)[0]),
		["ts", "px", "#rownum", "#created_at", "#updated_at", "#time"]
	);
}

#[test]
fn memory_time_column_follows_the_time_source_across_updates() {
	// processing time must stay at insert time on update while event time must follow the ts column
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	define(&sources, TICKS, TimeSource::Processing);
	define_prices(&sources);
	let trade = sources.insert(TRADES, version(1), at(10), values(1, "a"));
	let tick = sources.insert(TICKS, version(1), at(10), values(1, "k"));
	let price = sources.insert(PRICES, version(1), at(10), vec![Value::DateTime(at(100)), Value::Int4(1)]);
	sources.update(TRADES, trade, version(2), at(20), values(1, "a2"));
	sources.update(TICKS, tick, version(2), at(20), values(1, "k2"));
	sources.update(PRICES, price, version(2), at(20), vec![Value::DateTime(at(200)), Value::Int4(2)]);
	let mut scan = sources.scan_at(version(2));

	assert_eq!(rows_of(&pull_all(&mut scan, TRADES, 10)[0]), [row(trade, values(1, "a2"), 10, 20)]);
	assert_eq!(rows_of(&pull_all(&mut scan, TICKS, 10)[0]), [timed(tick, values(1, "k2"), 10, 20, 10)]);
	assert_eq!(
		rows_of(&pull_all(&mut scan, PRICES, 10)[0]),
		[timed(price, vec![Value::DateTime(at(200)), Value::Int4(2)], 10, 20, 200)]
	);
}

#[test]
fn memory_second_write_to_a_row_at_one_version_replaces_the_first() {
	// two writes in one commit must collapse to the last one, with created_at kept from the insert
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	let rewritten = sources.insert(TRADES, version(1), at(10), values(1, "a"));
	sources.update(TRADES, rewritten, version(1), at(15), values(1, "a15"));
	let twice = sources.insert(TRADES, version(1), at(10), values(3, "c"));
	let gone = sources.insert(TRADES, version(2), at(20), values(2, "b"));
	sources.delete(TRADES, gone, version(2));
	sources.update(TRADES, twice, version(3), at(30), values(3, "c30"));
	sources.update(TRADES, twice, version(3), at(35), values(3, "c35"));

	let at_two = pull_all(&mut sources.scan_at(version(2)), TRADES, 10);
	let at_three = pull_all(&mut sources.scan_at(version(3)), TRADES, 10);

	assert_eq!(rows_of(&at_two[0]), [row(rewritten, values(1, "a15"), 10, 15), row(twice, values(3, "c"), 10, 10)]);
	assert_eq!(
		rows_of(&at_three[0]),
		[row(rewritten, values(1, "a15"), 10, 15), row(twice, values(3, "c35"), 10, 35)]
	);
}

#[test]
fn memory_source_with_no_rows_gives_one_empty_chunk_with_all_columns_then_none() {
	// an empty source must still describe its columns once, then end
	let sources = MemorySources::default();
	define(&sources, QUOTES, TimeSource::Processing);
	let mut scan = sources.scan_at(version(1));

	scan.open(QUOTES, size(4)).unwrap();
	let first = scan.next().unwrap().expect("an empty source must yield one empty chunk first");
	let second = scan.next().unwrap();

	assert_eq!(first.num_rows(), 0);
	assert_eq!(
		names(&first),
		["id", "symbol", "#rownum", "#created_at", "#updated_at", "#time", "#commit_version"]
	);
	assert!(second.is_none());
}

#[test]
fn memory_source_whose_rows_are_all_dead_or_future_at_v_scans_like_an_empty_source() {
	// rows deleted at or before V and rows first written after V must both be invisible at V
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	let deleted = sources.insert(TRADES, version(1), at(1), values(1, "a"));
	sources.delete(TRADES, deleted, version(2));
	let mut scan = sources.scan_at(version(2));
	sources.insert(TRADES, version(3), at(3), values(2, "b"));

	scan.open(TRADES, size(4)).unwrap();

	assert_eq!(scan.next().unwrap().map(|chunk| chunk.num_rows()), Some(0));
	assert!(scan.next().unwrap().is_none());
}

#[test]
fn memory_short_chunk_ends_the_source_and_a_full_last_chunk_is_followed_by_none() {
	// the pull after the last row must be None, never an extra empty or repeated chunk
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	define(&sources, QUOTES, TimeSource::None);
	fill(&sources, TRADES, "t", 4, version(1));
	fill(&sources, QUOTES, "q", 3, version(1));
	let mut scan = sources.scan_at(version(1));

	let exact: Vec<usize> = pull_all(&mut scan, TRADES, 2).iter().map(RecordBatch::num_rows).collect();
	let short: Vec<usize> = pull_all(&mut scan, QUOTES, 2).iter().map(RecordBatch::num_rows).collect();

	assert_eq!(exact, [2, 2]);
	assert_eq!(short, [2, 1]);
}

#[test]
fn memory_open_restarts_a_source_from_its_beginning_and_replaces_the_open_one() {
	// a second open must rewind, and opening another source must not leak rows of the previous one
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	define(&sources, QUOTES, TimeSource::None);
	fill(&sources, TRADES, "t", 3, version(1));
	fill(&sources, QUOTES, "q", 2, version(1));
	let mut scan = sources.scan_at(version(1));

	scan.open(TRADES, size(1)).unwrap();
	scan.next().unwrap();
	scan.next().unwrap();
	scan.open(TRADES, size(2)).unwrap();
	let rewound = scan.next().unwrap().unwrap();
	scan.open(QUOTES, size(5)).unwrap();
	let replaced = scan.next().unwrap().unwrap();

	assert_eq!(rows_of(&rewound), [filled(1, "t"), filled(2, "t")]);
	assert_eq!(rows_of(&replaced), [filled(1, "q"), filled(2, "q")]);
	assert!(scan.next().unwrap().is_none());
}

#[test]
#[should_panic(expected = "is not defined in the memory sources")]
fn memory_open_of_an_undefined_source_panics() {
	// scanning a source the fake never defined is a test bug and must fail loud
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	let mut scan = sources.scan_at(version(1));
	let _ = scan.open(QUOTES, size(1));
}

#[test]
#[should_panic(expected = "pulled before any source was opened")]
fn memory_pull_before_any_open_panics() {
	// a pull with no open source is a caller bug and must never return a silent None
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	fill(&sources, TRADES, "t", 1, version(1));
	let mut scan = sources.scan_at(version(1));
	let _ = scan.next();
}

#[test]
#[should_panic(expected = "is already defined")]
fn memory_defining_a_source_twice_panics() {
	// a redefinition would silently reshape stored rows, so it must panic
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	define(&sources, TRADES, TimeSource::Processing);
}

#[test]
#[should_panic(expected = "does not fit")]
fn memory_insert_with_the_wrong_value_count_panics() {
	// a row that does not match the declared columns must never be stored
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	sources.insert(TRADES, version(1), at(1), vec![Value::Int4(1)]);
}

#[test]
#[should_panic(expected = "is older than the latest version")]
fn memory_write_below_latest_panics() {
	// versions must never go backwards, otherwise a scan could see history rewritten
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	sources.insert(TRADES, version(3), at(1), values(1, "a"));
	sources.insert(TRADES, version(2), at(2), values(2, "b"));
}

#[test]
#[should_panic(expected = "that a scan already reads")]
fn memory_write_at_a_scanned_version_panics() {
	// a version a scan reads is sealed: a write at exactly V would change the snapshot under the scan
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	sources.insert(TRADES, version(1), at(1), values(1, "a"));
	let _scan = sources.scan_at(version(3));
	sources.insert(TRADES, version(3), at(3), values(2, "b"));
}

#[test]
#[should_panic(expected = "that a scan already reads")]
fn memory_write_between_latest_and_a_scanned_version_panics() {
	// a write above latest but at or below a scanned V must still be refused, otherwise V is not immutable
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	sources.insert(TRADES, version(1), at(1), values(1, "a"));
	let _scan = sources.scan_at(version(3));
	sources.insert(TRADES, version(2), at(2), values(2, "b"));
}

#[test]
#[should_panic(expected = "is not live")]
fn memory_update_of_a_deleted_row_panics() {
	// updating a row that is not live at latest would resurrect it, so it must panic
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	let gone = sources.insert(TRADES, version(1), at(1), values(1, "a"));
	sources.delete(TRADES, gone, version(2));
	sources.update(TRADES, gone, version(3), at(3), values(1, "b"));
}

#[test]
#[should_panic(expected = "is not live")]
fn memory_delete_of_a_row_that_never_existed_panics() {
	// deleting a missing row must fail loud instead of recording a phantom delete
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	sources.insert(TRADES, version(1), at(1), values(1, "a"));
	sources.delete(TRADES, RowNumber(9), version(2));
}

#[test]
#[should_panic(expected = "not a datetime")]
fn memory_event_time_column_that_is_not_a_datetime_panics() {
	// an event-time source must never stamp #time from a non-datetime value
	let sources = MemorySources::default();
	define_prices(&sources);
	sources.insert(PRICES, version(1), at(1), vec![Value::Int4(100), Value::Int4(1)]);
}

#[test]
fn backfill_at_v_emits_exactly_the_rows_live_at_v() {
	// inserts, updates and deletes before, at and after V: the output must be the source exactly as it stood at V
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	let kept = sources.insert(TRADES, version(1), at(10), values(1, "a"));
	let updated_before = sources.insert(TRADES, version(1), at(10), values(2, "b"));
	let deleted_before = sources.insert(TRADES, version(1), at(10), values(3, "c"));
	let deleted_at_v = sources.insert(TRADES, version(1), at(10), values(4, "d"));
	sources.update(TRADES, updated_before, version(2), at(20), values(2, "b2"));
	sources.delete(TRADES, deleted_before, version(2));
	let updated_at_v = sources.insert(TRADES, version(2), at(20), values(5, "e"));
	sources.update(TRADES, updated_at_v, version(3), at(30), values(5, "e3"));
	sources.delete(TRADES, deleted_at_v, version(3));
	let inserted_at_v = sources.insert(TRADES, version(3), at(30), values(6, "f"));
	let mut scan = sources.scan_at(version(3));
	sources.update(TRADES, kept, version(4), at(40), values(1, "a4"));
	sources.delete(TRADES, updated_before, version(4));
	sources.insert(TRADES, version(4), at(40), values(7, "g"));

	let changes = collect(&mut scan, &[TRADES], 100).unwrap();

	assert_eq!(changes.len(), 1);
	assert_eq!(origin_of(&changes[0]), TRADES);
	assert_eq!(changes[0].version, at_v(version(3)));
	assert_eq!(changes[0].changed_at, at(30));
	assert_eq!(names(post(&changes[0])), ["id", "symbol", "#rownum", "#created_at", "#updated_at"]);
	assert_eq!(
		rows_of(post(&changes[0])),
		[
			row(kept, values(1, "a"), 10, 10),
			row(updated_before, values(2, "b2"), 10, 20),
			row(updated_at_v, values(5, "e3"), 20, 30),
			row(inserted_at_v, values(6, "f"), 30, 30),
		]
	);
}

#[test]
fn every_change_carries_v_even_when_table_rows_were_last_written_earlier() {
	// the per-row #commit_version must neither leak into the change version nor into the batch
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	let first = sources.insert(TRADES, version(1), at(1), values(1, "a"));
	sources.insert(TRADES, version(2), at(2), values(2, "b"));
	sources.insert(TRADES, version(3), at(3), values(3, "c"));
	sources.update(TRADES, first, version(4), at(4), values(1, "a4"));
	let mut scan = sources.scan_at(version(9));

	let changes = collect(&mut scan, &[TRADES], 1).unwrap();

	assert_eq!(changes.len(), 3);
	for change in &changes {
		assert_eq!(change.version, at_v(version(9)));
		assert_eq!(change.version.commit, version(9));
		assert_eq!(change.version.source, SourceVersion(9));
		assert!(system_column(post(change), SystemColumn::CommitVersion).is_none());
	}
}

#[test]
fn emitted_post_is_the_scanned_chunk_minus_commit_version_bit_for_bit() {
	// user columns, stamps, row order and row count must pass through untouched; only #commit_version goes
	let sources = MemorySources::default();
	define(&sources, QUOTES, TimeSource::Processing);
	fill(&sources, QUOTES, "q", 5, version(1));
	sources.update(QUOTES, RowNumber(2), version(2), at(20), values(2, "q2b"));
	sources.delete(QUOTES, RowNumber(4), version(2));

	let changes = collect(&mut sources.scan_at(version(2)), &[QUOTES], 2).unwrap();
	let chunks = pull_all(&mut sources.scan_at(version(2)), QUOTES, 2);

	assert_eq!(changes.len(), chunks.len());
	assert_eq!(changes.len(), 2);
	for (change, chunk) in changes.iter().zip(&chunks) {
		assert_eq!(post(change), &without(chunk, &["#commit_version"]));
	}
}

#[test]
fn emitted_column_set_equals_the_live_column_set_for_every_source_kind() {
	// snapshot batches must carry the same columns as live changes: stamps kept, #commit_version dropped
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	define(&sources, QUOTES, TimeSource::Processing);
	define(&sources, POSITIONS, TimeSource::None);
	define(&sources, TICKS, TimeSource::Processing);
	define_prices(&sources);
	for source in [TRADES, QUOTES, POSITIONS, TICKS] {
		sources.insert(source, version(1), at(1), values(1, "x"));
	}
	sources.insert(PRICES, version(1), at(1), vec![Value::DateTime(at(100)), Value::Int4(1)]);

	let changes =
		collect(&mut sources.scan_at(version(1)), &[PRICES, TICKS, POSITIONS, QUOTES, TRADES], 10).unwrap();
	let emitted: Vec<(ObjectId, Vec<String>)> =
		changes.iter().map(|change| (origin_of(change), names(post(change)))).collect();

	let plain = ["id", "symbol", "#rownum", "#created_at", "#updated_at"].map(String::from).to_vec();
	let with_time = ["id", "symbol", "#rownum", "#created_at", "#updated_at", "#time"].map(String::from).to_vec();
	let series = ["ts", "px", "#rownum", "#created_at", "#updated_at", "#time"].map(String::from).to_vec();
	assert_eq!(
		emitted,
		[
			(TRADES, plain.clone()),
			(QUOTES, with_time.clone()),
			(POSITIONS, plain),
			(TICKS, with_time),
			(PRICES, series),
		]
	);
}

#[test]
fn changed_at_is_the_stored_updated_at_of_the_row_with_batch_size_one() {
	// changed_at must be the row's own updated_at, never V's commit time or the insert time
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	let first = sources.insert(TRADES, version(1), at(10), values(1, "a"));
	let second = sources.insert(TRADES, version(1), at(10), values(2, "b"));
	sources.insert(TRADES, version(2), at(15), values(3, "c"));
	sources.update(TRADES, first, version(3), at(40), values(1, "a4"));
	sources.update(TRADES, second, version(3), at(25), values(2, "b2"));

	let changes = collect(&mut sources.scan_at(version(3)), &[TRADES], 1).unwrap();

	let stamped: Vec<(DateTime, DateTime)> =
		changes.iter().map(|change| (change.changed_at, rows_of(post(change))[0].updated)).collect();
	assert_eq!(stamped, [(at(40), at(40)), (at(25), at(25)), (at(15), at(15))]);
}

#[test]
fn changed_at_of_a_multi_row_chunk_is_its_latest_updated_at_and_rows_keep_their_own() {
	// the chunk stamp must be the maximum updated_at, not the first or last row's, and per-row stamps must survive
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	let first = sources.insert(TRADES, version(1), at(10), values(1, "a"));
	let middle = sources.insert(TRADES, version(1), at(10), values(2, "b"));
	let last = sources.insert(TRADES, version(1), at(10), values(3, "c"));
	sources.update(TRADES, middle, version(2), at(50), values(2, "b2"));
	sources.update(TRADES, last, version(3), at(30), values(3, "c2"));

	let changes = collect(&mut sources.scan_at(version(3)), &[TRADES], 3).unwrap();

	assert_eq!(changes.len(), 1);
	assert_eq!(changes[0].changed_at, at(50));
	assert_eq!(
		rows_of(post(&changes[0])),
		[
			row(first, values(1, "a"), 10, 10),
			row(middle, values(2, "b2"), 10, 50),
			row(last, values(3, "c2"), 10, 30),
		]
	);
}

#[test]
fn row_number_created_at_and_time_ride_along_unchanged() {
	// snapshot rows must keep their stored stamps, never restamp them with V or the scan time
	let sources = MemorySources::default();
	define(&sources, TICKS, TimeSource::Processing);
	define_prices(&sources);
	let tick = sources.insert(TICKS, version(1), at(10), values(1, "a"));
	let price = sources.insert(PRICES, version(1), at(10), vec![Value::DateTime(at(100)), Value::Int4(1)]);
	sources.update(TICKS, tick, version(2), at(20), values(1, "a2"));
	let late_tick = sources.insert(TICKS, version(2), at(20), values(2, "b"));
	sources.update(PRICES, price, version(2), at(20), vec![Value::DateTime(at(200)), Value::Int4(11)]);
	let late_price = sources.insert(PRICES, version(2), at(20), vec![Value::DateTime(at(150)), Value::Int4(2)]);

	let changes = collect(&mut sources.scan_at(version(2)), &[PRICES, TICKS], 10).unwrap();

	assert_eq!(changes.iter().map(origin_of).collect::<Vec<_>>(), [TICKS, PRICES]);
	assert_eq!(
		rows_of(post(&changes[0])),
		[timed(tick, values(1, "a2"), 10, 20, 10), timed(late_tick, values(2, "b"), 20, 20, 20)]
	);
	assert_eq!(
		rows_of(post(&changes[1])),
		[
			timed(price, vec![Value::DateTime(at(200)), Value::Int4(11)], 10, 20, 200),
			timed(late_price, vec![Value::DateTime(at(150)), Value::Int4(2)], 20, 20, 150),
		]
	);
}

#[test]
fn a_source_larger_than_batch_size_splits_into_full_chunks_and_one_tail_with_no_gap_or_duplicate() {
	// chunks must be full except the last, never exceed batch_size, and union to exactly the source
	for (count, batch_size, sizes) in [
		(7, 3, vec![3, 3, 1]),
		(6, 3, vec![3, 3]),
		(7, 7, vec![7]),
		(6, 7, vec![6]),
		(8, 7, vec![7, 1]),
		(3, 1, vec![1, 1, 1]),
	] {
		let sources = MemorySources::default();
		define(&sources, TRADES, TimeSource::None);
		let numbers = fill(&sources, TRADES, "t", count, version(1));
		let mut scan = TestingScan::over(sources.scan_at(version(2)), Arc::new(NoFaults));

		let changes = collect(&mut scan, &[TRADES], batch_size).unwrap();

		let chunked: Vec<Vec<RowNumber>> = changes.iter().map(|change| numbers_of(post(change))).collect();
		let label = format!("{count} rows in chunks of {batch_size}");
		assert_eq!(chunked.iter().map(Vec::len).collect::<Vec<_>>(), sizes, "{label}");
		assert_eq!(chunked.concat(), numbers, "{label}");
		assert!(changes.iter().all(|change| change.version == at_v(version(2))), "{label}");
		assert!(changes.iter().all(|change| origin_of(change) == TRADES), "{label}");
		assert_eq!(scan.calls(), 2 + sizes.len() as u64, "{label}");
	}
}

#[test]
fn deleted_rows_do_not_count_toward_a_chunk() {
	// a chunk counted over dead rows would come back short and end the source early, dropping live rows
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	let numbers = fill(&sources, TRADES, "t", 10, version(1));
	for number in numbers.iter().filter(|number| number.0 % 2 == 0) {
		sources.delete(TRADES, *number, version(2));
	}

	let changes = collect(&mut sources.scan_at(version(2)), &[TRADES], 2).unwrap();

	let chunked: Vec<Vec<u64>> =
		changes.iter().map(|change| numbers_of(post(change)).iter().map(|number| number.0).collect()).collect();
	assert_eq!(chunked, [vec![1, 3], vec![5, 7], vec![9]]);
}

#[test]
fn several_sources_are_read_in_object_id_order_at_one_v_even_when_later_ones_change_mid_scan() {
	// writes at V+1 to sources not yet opened must never leak in, so every source is read at the same V
	let sources = MemorySources::default();
	let labelled = [(TRADES, "t"), (QUOTES, "q"), (POSITIONS, "p"), (TICKS, "k")];
	for (source, label) in labelled {
		define(&sources, source, TimeSource::None);
		fill(&sources, source, label, 2, version(1));
	}
	let writer = sources.clone();
	let hooks = Arc::new(During(move |point: Point| match point {
		Point::Open(source) if source != TRADES => {
			writer.update(source, RowNumber(1), version(2), at(90), values(1, "changed"));
			writer.delete(source, RowNumber(2), version(2));
			writer.insert(source, version(2), at(90), values(9, "opened"));
		}
		Point::Next(TRADES, 1) => {
			for source in [QUOTES, POSITIONS, TICKS] {
				writer.insert(source, version(2), at(80), values(8, "mid"));
			}
		}
		_ => {}
	}));
	let mut scan = TestingScan::over(sources.scan_at(version(1)), hooks);

	let changes = collect(&mut scan, &[TICKS, POSITIONS, QUOTES, TRADES], 1).unwrap();

	let seen: Vec<(ObjectId, Row)> = changes
		.iter()
		.flat_map(|change| rows_of(post(change)).into_iter().map(move |row| (origin_of(change), row)))
		.collect();
	let expected: Vec<(ObjectId, Row)> = labelled
		.iter()
		.flat_map(|&(source, label)| [(source, filled(1, label)), (source, filled(2, label))])
		.collect();
	assert_eq!(seen, expected);
	assert!(changes.iter().all(|change| change.version == at_v(version(1))));
	for source in [QUOTES, POSITIONS, TICKS] {
		let after = pull_all(&mut sources.scan_at(version(2)), source, 10);
		assert_eq!(numbers_of(&after[0]), [RowNumber(1), RowNumber(3), RowNumber(4)]);
	}
}

#[test]
fn a_write_at_v_plus_one_inside_any_during_hook_is_not_in_the_backfill() {
	// writes landing between chunks must not appear, vanish or change rows of the snapshot at V
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	fill(&sources, TRADES, "t", 4, version(1));
	let writer = sources.clone();
	let hooks = Arc::new(During(move |point: Point| match point {
		Point::Open(TRADES) => {
			writer.update(TRADES, RowNumber(4), version(2), at(90), values(4, "late"));
			writer.insert(TRADES, version(2), at(90), values(5, "new"));
		}
		Point::Next(TRADES, 0) => writer.delete(TRADES, RowNumber(3), version(2)),
		Point::Next(TRADES, 1) => {
			writer.update(TRADES, RowNumber(1), version(2), at(91), values(1, "late"));
			writer.update(TRADES, RowNumber(2), version(2), at(91), values(2, "late"));
		}
		Point::Next(TRADES, 2) => {
			writer.delete(TRADES, RowNumber(4), version(2));
			writer.insert(TRADES, version(2), at(92), values(6, "new"));
		}
		_ => {}
	}));
	let mut scan = TestingScan::over(sources.scan_at(version(1)), hooks);

	let changes = collect(&mut scan, &[TRADES], 1).unwrap();

	let seen: Vec<Row> = changes.iter().flat_map(|change| rows_of(post(change))).collect();
	assert_eq!(seen, [filled(1, "t"), filled(2, "t"), filled(3, "t"), filled(4, "t")]);
	assert!(changes.iter().all(|change| change.version == at_v(version(1))));
	let after = pull_all(&mut sources.scan_at(version(2)), TRADES, 10);
	assert_eq!(numbers_of(&after[0]), [RowNumber(1), RowNumber(2), RowNumber(5), RowNumber(6)]);
}

#[test]
fn zero_sources_is_ok_and_makes_no_scan_call() {
	// an empty source set must neither call the consumer nor touch the scan
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	fill(&sources, TRADES, "t", 2, version(1));
	let recorder = Arc::new(Recorder::new());
	let mut scan = TestingScan::over(sources.scan_at(version(1)), recorder.clone());

	let changes = collect(&mut scan, &[], 1).unwrap();

	assert!(changes.is_empty());
	assert_eq!(scan.calls(), 0);
	assert!(recorder.events().is_empty());
}

#[test]
fn an_empty_source_emits_nothing_and_costs_one_open_and_two_pulls() {
	// the empty first chunk must be skipped, never handed over as a zero-row insert
	let sources = MemorySources::default();
	define(&sources, QUOTES, TimeSource::None);
	let mut scan = TestingScan::over(sources.scan_at(version(1)), Arc::new(NoFaults));

	let changes = collect(&mut scan, &[QUOTES], 1).unwrap();

	assert!(changes.is_empty());
	assert_eq!(scan.calls(), 3);
}

#[test]
fn an_empty_source_between_two_full_ones_does_not_stop_the_backfill() {
	// skipping an empty chunk must continue with the next source, never end the whole backfill
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	define(&sources, QUOTES, TimeSource::None);
	define(&sources, POSITIONS, TimeSource::None);
	fill(&sources, TRADES, "t", 1, version(1));
	fill(&sources, POSITIONS, "p", 1, version(1));

	let changes = collect(&mut sources.scan_at(version(1)), &[TRADES, QUOTES, POSITIONS], 4).unwrap();

	let seen: Vec<(ObjectId, Vec<Row>)> =
		changes.iter().map(|change| (origin_of(change), rows_of(post(change)))).collect();
	assert_eq!(seen, [(TRADES, vec![filled(1, "t")]), (POSITIONS, vec![filled(1, "p")])]);
}

#[test]
fn a_refused_open_propagates_and_stops_before_any_later_source() {
	// an open error must end the backfill at once: no pull on the refused source and no later source opened
	let sources = MemorySources::default();
	for (source, label) in [(TRADES, "t"), (QUOTES, "q"), (POSITIONS, "p")] {
		define(&sources, source, TimeSource::None);
		fill(&sources, source, label, 2, version(1));
	}
	let hooks = Arc::new(Refuse::open(QUOTES));
	let mut scan = TestingScan::over(sources.scan_at(version(1)), hooks.clone());
	let mut consumed = Vec::new();

	let result = backfill(&mut scan, &set(&[TRADES, QUOTES, POSITIONS]), size(1), |_, change| {
		consumed.push(origin_of(&change));
		Ok(())
	});

	assert_eq!(result, Err(refused()));
	assert_eq!(consumed, [TRADES, TRADES]);
	assert_eq!(scan.calls(), 5);
	assert_eq!(hooks.pulls(), [(TRADES, 0), (TRADES, 1), (TRADES, 2)]);
}

#[test]
fn a_refused_pull_propagates_and_stops_the_loop() {
	// a pull error must end the backfill at once: no retry, no further pull, no later source
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	define(&sources, QUOTES, TimeSource::None);
	fill(&sources, TRADES, "t", 3, version(1));
	fill(&sources, QUOTES, "q", 1, version(1));
	let hooks = Arc::new(Refuse::next(TRADES, 1, u64::MAX));
	let mut scan = TestingScan::over(sources.scan_at(version(1)), hooks.clone());
	let mut consumed = Vec::new();

	let result = backfill(&mut scan, &set(&[TRADES, QUOTES]), size(1), |_, change| {
		consumed.push(numbers_of(post(&change)));
		Ok(())
	});

	assert_eq!(result, Err(refused()));
	assert_eq!(consumed, [vec![RowNumber(1)]]);
	assert_eq!(scan.calls(), 3);
	assert_eq!(hooks.pulls(), [(TRADES, 0), (TRADES, 1)]);
}

#[test]
fn a_consumer_error_propagates_and_stops_the_loop() {
	// a handler error must end the backfill before the next pull, so no chunk is read past the failure
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	fill(&sources, TRADES, "t", 3, version(1));
	let recorder = Arc::new(Recorder::new());
	let mut scan = TestingScan::over(sources.scan_at(version(1)), recorder.clone());
	let mut handled = 0;

	let result = backfill(&mut scan, &set(&[TRADES]), size(1), |_, _| {
		handled += 1;
		Err(refused())
	});

	assert_eq!(result, Err(refused()));
	assert_eq!(handled, 1);
	assert_eq!(scan.calls(), 2);
	assert_eq!(
		recorder.events(),
		[
			Event::Call(1),
			Event::DuringOpen(TRADES),
			Event::OnOpen(TRADES),
			Event::Call(2),
			Event::DuringNext(TRADES, 0),
			Event::OnNext(TRADES, 0),
		]
	);
}

#[test]
fn an_inner_scan_error_propagates_through_the_decorator_and_backfill_unchanged() {
	// the inner scan's own error must reach the caller as is and stop the loop
	let inner = ScriptedScan::new(vec![(
		TRADES,
		vec![Scripted::Chunk(stamped(&[1], &[5])), Scripted::Fail, Scripted::Chunk(stamped(&[2], &[6]))],
	)]);
	let mut scan = TestingScan::over(inner, Arc::new(NoFaults));
	let mut consumed = 0;

	let result = backfill(&mut scan, &set(&[TRADES]), size(1), |_, _| {
		consumed += 1;
		Ok(())
	});

	assert_eq!(result, Err(inner_failure()));
	assert_eq!(consumed, 1);
	assert_eq!(scan.scan_mut().log, [Step::Open(TRADES, 1), Step::Next, Step::Next]);
}

#[test]
fn backfill_opens_each_source_then_pulls_until_none_handing_each_chunk_over_before_the_next_pull() {
	// each chunk must reach the consumer, with the scan itself, before the next pull; sources follow ObjectId order
	let mut scan = ScriptedScan::new(vec![
		(POSITIONS, vec![Scripted::Chunk(stamped(&[1, 2, 3], &[1, 2, 3]))]),
		(TRADES, vec![Scripted::Chunk(stamped(&[1, 2], &[4, 5])), Scripted::Chunk(stamped(&[3], &[6]))]),
	]);

	backfill(&mut scan, &set(&[POSITIONS, TRADES]), size(3), consume_into_log).unwrap();

	assert_eq!(
		scan.log,
		[
			Step::Open(TRADES, 3),
			Step::Next,
			Step::Consumed(TRADES, 2),
			Step::Next,
			Step::Consumed(TRADES, 1),
			Step::Next,
			Step::Open(POSITIONS, 3),
			Step::Next,
			Step::Consumed(POSITIONS, 3),
			Step::Next,
		]
	);
}

#[test]
fn partition_and_commit_version_are_dropped_and_everything_else_is_kept_in_order() {
	// only #rownum, #created_at, #updated_at and #time may survive; user columns keep their order and values
	let chunk = batch(vec![
		utf8("zeta", ["z1", "z2"]),
		int4("alpha", [10, 20]),
		uint8("#rownum", [4, 7]),
		uint16("#partition", [3, 3]),
		datetime("#created_at", [at(1), at(2)]),
		datetime("#updated_at", [at(40), at(30)]),
		datetime("#time", [at(5), at(6)]),
		uint8("#commit_version", [11, 12]),
	])
	.unwrap();
	let expected = without(&chunk, &["#partition", "#commit_version"]);
	let mut scan = ScriptedScan::new(vec![(TRADES, vec![Scripted::Chunk(chunk)])]);

	let changes = collect(&mut scan, &[TRADES], 10).unwrap();

	assert_eq!(changes.len(), 1);
	assert_eq!(origin_of(&changes[0]), TRADES);
	assert_eq!(changes[0].version, at_v(SCRIPTED_V));
	assert_eq!(changes[0].changed_at, at(40));
	assert_eq!(names(post(&changes[0])), ["zeta", "alpha", "#rownum", "#created_at", "#updated_at", "#time"]);
	assert_eq!(post(&changes[0]), &expected);
}

#[test]
fn a_zero_row_chunk_is_skipped_even_without_stamps_and_the_source_keeps_going() {
	// an empty chunk must never be validated, handed over, or treated as the end of the source
	let mut scan = ScriptedScan::new(vec![(
		TRADES,
		vec![
			Scripted::Chunk(empty_batch()),
			Scripted::Chunk(stamped(&[1, 2], &[3, 4])),
			Scripted::Chunk(stamped(&[], &[])),
		],
	)]);

	backfill(&mut scan, &set(&[TRADES]), size(2), consume_into_log).unwrap();

	assert_eq!(
		scan.log,
		[Step::Open(TRADES, 2), Step::Next, Step::Next, Step::Consumed(TRADES, 2), Step::Next, Step::Next]
	);
}

#[test]
fn a_non_empty_chunk_without_updated_at_is_an_error() {
	// without #updated_at there is no changed_at, so the backfill must fail instead of guessing
	let chunk = batch(vec![int4("id", [1]), uint8("#rownum", [1]), datetime("#created_at", [at(1)])]).unwrap();
	let mut scan =
		ScriptedScan::new(vec![(TRADES, vec![Scripted::Chunk(chunk), Scripted::Chunk(stamped(&[2], &[2]))])]);

	let result = backfill(&mut scan, &set(&[TRADES]), size(1), consume_into_log);

	assert!(result.is_err());
	assert_eq!(scan.log, [Step::Open(TRADES, 1), Step::Next]);
}

#[test]
fn a_chunk_with_a_none_updated_at_is_an_error() {
	// a none stamp must fail loud, never be skipped or replaced by a default time
	let chunk = batch(vec![
		int4("id", [1, 2]),
		uint8("#rownum", [1, 2]),
		datetime("#created_at", [at(1), at(1)]),
		datetime_with_bitvec("#updated_at", [at(5), at(6)], vec![true, false]),
	])
	.unwrap();
	let mut scan = ScriptedScan::new(vec![(TRADES, vec![Scripted::Chunk(chunk)])]);

	let result = backfill(&mut scan, &set(&[TRADES]), size(2), consume_into_log);

	assert!(result.is_err());
	assert_eq!(scan.log, [Step::Open(TRADES, 2), Step::Next]);
}

#[test]
fn a_chunk_whose_updated_at_is_not_a_timestamp_is_an_error() {
	// a mistyped stamp column must fail loud instead of being reinterpreted as a time
	let chunk = batch(vec![
		int4("id", [1]),
		uint8("#rownum", [1]),
		datetime("#created_at", [at(1)]),
		int8("#updated_at", [5]),
	])
	.unwrap();
	let mut scan = ScriptedScan::new(vec![(TRADES, vec![Scripted::Chunk(chunk)])]);

	let result = backfill(&mut scan, &set(&[TRADES]), size(1), consume_into_log);

	assert!(result.is_err());
	assert_eq!(scan.log, [Step::Open(TRADES, 1), Step::Next]);
}

#[test]
fn calls_count_every_open_and_pull_including_refused_ones_and_nothing_else() {
	// version() must not count, and a refused call still counts, otherwise on_call(n) sweeps skip points
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	fill(&sources, TRADES, "t", 2, version(1));
	let mut scan = TestingScan::over(sources.scan_at(version(1)), Arc::new(Refuse::next(TRADES, 1, 1)));

	assert_eq!(scan.version(), version(1));
	assert_eq!(scan.calls(), 0);
	scan.open(TRADES, size(1)).unwrap();
	assert_eq!(scan.calls(), 1);
	scan.next().unwrap();
	assert_eq!(scan.version(), version(1));
	assert_eq!(scan.calls(), 2);
	assert_eq!(scan.next(), Err(refused()));
	assert_eq!(scan.calls(), 3);
}

#[test]
fn a_clean_backfill_calls_on_call_once_per_open_and_pull_in_order() {
	// the counter must be exact: 2 live rows at batch 1 cost 4 calls and an empty source 3
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	define(&sources, QUOTES, TimeSource::None);
	fill(&sources, TRADES, "t", 2, version(1));
	let recorder = Arc::new(Recorder::new());
	let mut scan = TestingScan::over(sources.scan_at(version(1)), recorder.clone());

	collect(&mut scan, &[TRADES, QUOTES], 1).unwrap();

	let calls: Vec<u64> = recorder
		.events()
		.into_iter()
		.filter_map(|event| match event {
			Event::Call(call) => Some(call),
			_ => None,
		})
		.collect();
	assert_eq!(calls, [1, 2, 3, 4, 5, 6, 7]);
	assert_eq!(scan.calls(), 7);
}

#[test]
fn a_crash_at_call_n_panics_at_exactly_n_for_every_n_and_never_before() {
	// a crash sweep needs every call point reachable: calls before n must pass and call n must panic
	let consumed_before_crash = [0, 0, 1, 2, 2, 2, 2];
	for (n, expected) in (1..=7u64).zip(consumed_before_crash) {
		let sources = MemorySources::default();
		define(&sources, TRADES, TimeSource::None);
		define(&sources, QUOTES, TimeSource::None);
		fill(&sources, TRADES, "t", 2, version(1));
		let mut scan = TestingScan::over(sources.scan_at(version(1)), Arc::new(CrashAt(n)));
		let mut consumed = 0;

		let payload = catch_unwind(AssertUnwindSafe(|| {
			backfill(&mut scan, &set(&[TRADES, QUOTES]), size(1), |_, _| {
				consumed += 1;
				Ok(())
			})
		}))
		.unwrap_err();

		let message = format!("simulated crash at call {n}");
		assert_eq!(payload.downcast_ref::<String>().map(String::as_str), Some(message.as_str()));
		assert_eq!(scan.calls(), n);
		assert_eq!(consumed, expected, "crash at call {n}");
	}

	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	define(&sources, QUOTES, TimeSource::None);
	fill(&sources, TRADES, "t", 2, version(1));
	let mut scan = TestingScan::over(sources.scan_at(version(1)), Arc::new(CrashAt(8)));
	assert_eq!(collect(&mut scan, &[TRADES, QUOTES], 1).unwrap().len(), 2);
	assert_eq!(scan.calls(), 7);
}

#[test]
fn a_crash_happens_before_the_inner_scan_is_read() {
	// a simulated crash must stop before the inner pull, otherwise the crashed call would consume a row
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	fill(&sources, TRADES, "t", 2, version(1));
	let mut scan = TestingScan::over(sources.scan_at(version(1)), Arc::new(CrashAt(2)));
	scan.open(TRADES, size(1)).unwrap();

	let crashed = catch_unwind(AssertUnwindSafe(|| scan.next()));

	assert!(crashed.is_err());
	assert_eq!(scan.calls(), 2);
	let first = scan.scan_mut().next().unwrap().unwrap();
	assert_eq!(rows_of(&first), [filled(1, "t")]);
}

#[test]
fn no_faults_gives_results_identical_to_the_bare_memory_scan() {
	// the decorator with no faults must be invisible: same chunks, same changes, same order
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	define(&sources, QUOTES, TimeSource::None);
	define(&sources, TICKS, TimeSource::Processing);
	fill(&sources, TRADES, "t", 5, version(1));
	fill(&sources, TICKS, "k", 3, version(1));
	sources.update(TRADES, RowNumber(2), version(2), at(20), values(2, "t2b"));
	sources.delete(TRADES, RowNumber(3), version(2));
	let everything = [TRADES, QUOTES, TICKS];

	let bare = collect(&mut sources.scan_at(version(2)), &everything, 2).unwrap();
	let mut decorated_scan = TestingScan::over(sources.scan_at(version(2)), Arc::new(NoFaults));
	let decorated = collect(&mut decorated_scan, &everything, 2).unwrap();

	assert_eq!(bare.len(), 4);
	assert_eq!(decorated.len(), bare.len());
	for (left, right) in decorated.iter().zip(&bare) {
		assert_eq!(left.origin, right.origin);
		assert_eq!(left.version, right.version);
		assert_eq!(left.changed_at, right.changed_at);
		assert_eq!(left.diffs, right.diffs);
	}
	assert_eq!(decorated_scan.calls(), 11);
	let mut bare_scan: MemoryScan = sources.scan_at(version(2));
	let mut wrapped = TestingScan::over(sources.scan_at(version(2)), Arc::new(NoFaults));
	assert_eq!(wrapped.version(), bare_scan.version());
	for source in everything {
		assert_eq!(pull_all(&mut wrapped, source, 2), pull_all(&mut bare_scan, source, 2));
	}
}

#[test]
fn hooks_fire_at_their_documented_points_and_counts() {
	// during_* must fire after V is fixed and between chunks, after the previous chunk was consumed, once per point
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	define(&sources, QUOTES, TimeSource::None);
	fill(&sources, TRADES, "t", 2, version(1));
	let recorder = Arc::new(Recorder::new());
	let log = recorder.clone();
	let mut scan = TestingScan::over(sources.scan_at(version(1)), recorder.clone());

	backfill(&mut scan, &set(&[QUOTES, TRADES]), size(1), |scan, change| {
		log.events.lock().push(Event::Consumed(origin_of(&change), numbers_of(post(&change)), scan.calls()));
		Ok(())
	})
	.unwrap();

	assert_eq!(
		recorder.events(),
		[
			Event::Call(1),
			Event::DuringOpen(TRADES),
			Event::OnOpen(TRADES),
			Event::Call(2),
			Event::DuringNext(TRADES, 0),
			Event::OnNext(TRADES, 0),
			Event::Consumed(TRADES, vec![RowNumber(1)], 2),
			Event::Call(3),
			Event::DuringNext(TRADES, 1),
			Event::OnNext(TRADES, 1),
			Event::Consumed(TRADES, vec![RowNumber(2)], 3),
			Event::Call(4),
			Event::DuringNext(TRADES, 2),
			Event::OnNext(TRADES, 2),
			Event::Call(5),
			Event::DuringOpen(QUOTES),
			Event::OnOpen(QUOTES),
			Event::Call(6),
			Event::DuringNext(QUOTES, 0),
			Event::OnNext(QUOTES, 0),
			Event::Call(7),
			Event::DuringNext(QUOTES, 1),
			Event::OnNext(QUOTES, 1),
		]
	);
}

#[test]
fn a_refused_pull_neither_advances_the_pull_index_nor_reads_the_inner_scan() {
	// a refused pull must lose no row: the retry sees the same pull index and gets the row the refusal held back
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	fill(&sources, TRADES, "t", 3, version(1));
	let hooks = Arc::new(Refuse::next(TRADES, 1, 1));
	let mut scan = TestingScan::over(sources.scan_at(version(1)), hooks.clone());

	scan.open(TRADES, size(1)).unwrap();
	let first = scan.next().unwrap().unwrap();
	let refused_pull = scan.next();
	let second = scan.next().unwrap().unwrap();
	let third = scan.next().unwrap().unwrap();
	let end = scan.next().unwrap();

	assert_eq!(refused_pull, Err(refused()));
	assert_eq!(numbers_of(&first), [RowNumber(1)]);
	assert_eq!(numbers_of(&second), [RowNumber(2)]);
	assert_eq!(numbers_of(&third), [RowNumber(3)]);
	assert!(end.is_none());
	assert_eq!(hooks.pulls(), [(TRADES, 0), (TRADES, 1), (TRADES, 1), (TRADES, 2), (TRADES, 3)]);
	assert_eq!(scan.calls(), 6);
}

#[test]
fn a_refused_open_leaves_the_current_source_and_its_pull_index_alone() {
	// a refused open must not open the inner scan, so pulls continue on the source that was already open
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	define(&sources, QUOTES, TimeSource::None);
	fill(&sources, TRADES, "t", 2, version(1));
	fill(&sources, QUOTES, "q", 2, version(1));
	let hooks = Arc::new(Refuse::open(QUOTES));
	let mut scan = TestingScan::over(sources.scan_at(version(1)), hooks.clone());

	scan.open(TRADES, size(1)).unwrap();
	let first = scan.next().unwrap().unwrap();
	let refused_open = scan.open(QUOTES, size(1));
	let second = scan.next().unwrap().unwrap();

	assert_eq!(refused_open, Err(refused()));
	assert_eq!(rows_of(&first), [filled(1, "t")]);
	assert_eq!(rows_of(&second), [filled(2, "t")]);
	assert_eq!(hooks.pulls(), [(TRADES, 0), (TRADES, 1)]);
}

#[test]
#[should_panic(expected = "testing scan pulled before any source was opened")]
fn pulling_after_only_a_refused_open_panics() {
	// a refused open opens nothing, so a following pull is a caller bug that must panic, never read the inner scan
	let sources = MemorySources::default();
	define(&sources, TRADES, TimeSource::None);
	fill(&sources, TRADES, "t", 1, version(1));
	let mut scan = TestingScan::over(sources.scan_at(version(1)), Arc::new(Refuse::open(TRADES)));

	assert_eq!(scan.open(TRADES, size(1)), Err(refused()));
	let _ = scan.next();
}

#[test]
fn an_inner_pull_error_does_not_advance_the_pull_index() {
	// the pull index must grow only on success, so a failed inner pull is retried at the same index
	let inner = ScriptedScan::new(vec![(TRADES, vec![Scripted::Fail, Scripted::Chunk(stamped(&[1], &[1]))])]);
	let hooks = Arc::new(Refuse::never());
	let mut scan = TestingScan::over(inner, hooks.clone());

	scan.open(TRADES, size(1)).unwrap();
	let failed = scan.next();
	let chunk = scan.next().unwrap();
	let end = scan.next().unwrap();

	assert_eq!(failed, Err(inner_failure()));
	assert_eq!(chunk.map(|chunk| chunk.num_rows()), Some(1));
	assert!(end.is_none());
	assert_eq!(hooks.pulls(), [(TRADES, 0), (TRADES, 0), (TRADES, 1)]);
	assert_eq!(scan.calls(), 4);
}
