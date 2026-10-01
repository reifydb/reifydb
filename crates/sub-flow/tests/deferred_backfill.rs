// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeSet,
	ops::Bound,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
		mpsc::{Receiver, channel},
	},
	thread::{sleep, spawn, yield_now},
};

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb::{
	ConfigKey, SqliteConfig, WithSubsystem, embedded,
	routine::abi::{
		Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
	},
	testing::db::{TempDbPath, TestDb, await_value, poll_until},
};
use reifydb_cdc::{consume::checkpoint::CdcCheckpoint, lift::changed_objects};
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::{ChangeVersion, CommitVersion, SourceVersion},
	interface::{
		catalog::{
			flow::{FlowId, OperatorId},
			id::{TableId, ViewId},
			object::ObjectId,
		},
		cdc::CdcConsumerId,
		flow::OperatorCapability,
	},
	key::operator::state::{GroupId, managed_key_in},
	operator_with::ApplyWith,
	value::column::factory::rename,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_flow::backfill::testing::{InstalledScanHooks, Outcome, ScanHooks};
use reifydb_runtime::{
	RuntimeConfig,
	context::clock::{Clock, MockClock},
	fatal::FatalConfig,
	sync::mutex::Mutex,
};
use reifydb_sdk::{
	error::Result as SdkResult,
	flow::operator::{
		ManagedOperator, OperatorMetadata,
		column::operator::OperatorColumn,
		context::{ClassState, GuestContext, Managed},
		view::{ChangeView, ColumnsView, DiffView, RowView},
	},
};
use reifydb_store_cdc::storage::{CdcStorage, Cutoff};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	config::ExtensionParams,
	error::{Diagnostic, Error},
	params::Params,
	value::{Value, constraint::TypeConstraint, duration::Duration, identity::IdentityId, value_type::ValueType},
};

const TIMEOUT: Duration = Duration::from_seconds_const(10);

const DDL_CURSOR_WAIT: Duration = Duration::from_seconds_const(20);

const HOLD: Duration = Duration::from_seconds_const(1);

const PAST_CHECKPOINT_AGE: Duration = Duration::from_seconds_const(6);

const REFUSED: &str = "TEST_DEFERRED_BACKFILL_REFUSED";

const COLUMNS: &str = "id: int4, g: int4, sym: utf8, v: int4";

const SIX_COLUMNS: &str = "id: int4, g: int4, v: int4";

const SERIES_COLUMNS: &str = "ts: int8, id: int4, g: int4";

const AGGREGATE_COLUMNS: &str = "g: int4, n: int8, total: Option(int4), hi: Option(int4)";

const AGGREGATE: &str =
	"FROM bf::src | aggregate { n: math::count(id), total: math::sum(v), hi: math::max(v) } by { g }";

const FILTER: &str = "FROM bf::src | filter { v > 50 }";

const SORTED: &str = "FROM bf::src | filter { v > 20 } | sort { v }";

const START_NANOS: u64 = 1_767_225_600_000_000_000;

#[derive(Clone, Copy)]
enum Cols {
	WithRownum,
	User,
}

fn runtime() -> RuntimeConfig {
	RuntimeConfig::default().fatal(FatalConfig::disarmed())
}

fn memory() -> TestDb {
	memory_with(4)
}

fn memory_with(batch: u16) -> TestDb {
	let db = TestDb::from(
		embedded::memory()
			.with_runtime_config(runtime())
			.with_config(ConfigKey::QueryRowBatchSize, Value::Uint2(batch))
			.with_flow(|f| f)
			.build()
			.expect("build memory db with flow"),
	);
	tables(&db);
	db
}

fn open(path: &TempDbPath, flow: bool, hooks: Option<Arc<dyn ScanHooks>>) -> TestDb {
	let mut builder = embedded::sqlite(SqliteConfig::new(path))
		.with_runtime_config(runtime())
		.with_config(ConfigKey::QueryRowBatchSize, Value::Uint2(4));
	if let Some(hooks) = hooks {
		builder = builder.with_dependency(InstalledScanHooks(hooks));
	}
	if flow {
		builder = builder.with_flow(|f| f);
	}
	TestDb::from(builder.build().expect("open the sqlite db"))
}

fn tables(db: &TestDb) {
	db.admin("CREATE NAMESPACE bf");
	db.admin("CREATE DICTIONARY bf::syms FOR utf8 AS uint4");
	db.admin(&format!("CREATE TABLE bf::src {{ {COLUMNS} }}"));
	db.admin(&format!("CREATE TABLE bf::src2 {{ {COLUMNS} }}"));
	db.admin("CREATE TABLE bf::dsrc { id: int4, g: int4, sym: utf8 with { dictionary: bf::syms }, v: int4 }");
}

fn history(table: &str) -> Vec<String> {
	vec![
		format!(
			"INSERT {table} [{{ id: 1, g: 1, sym: 'a', v: 5 }}, {{ id: 2, g: 1, sym: 'b', v: 50 }}, {{ id: 3, g: 2, sym: 'c', v: 95 }}, {{ id: 4, g: 2, sym: 'd', v: 30 }}]"
		),
		format!("UPDATE {table} {{ v: 60 }} FILTER {{ id == 1 }}"),
		format!("DELETE {table} FILTER {{ id == 3 }}"),
		format!("INSERT {table} [{{ id: 5, g: 3, sym: 'e', v: 70 }}, {{ id: 6, g: 1, sym: 'f', v: 10 }}]"),
		format!("UPDATE {table} {{ g: 2, v: 80 }} FILTER {{ id == 2 }}"),
		format!("DELETE {table} FILTER {{ id == 6 }}"),
		format!(
			"INSERT {table} [{{ id: 7, g: 3, sym: 'g', v: 55 }}]; UPDATE {table} {{ v: 25 }} FILTER {{ id == 7 }}"
		),
	]
}

fn later(table: &str) -> Vec<String> {
	vec![
		format!("INSERT {table} [{{ id: 8, g: 1, sym: 'h', v: 90 }}]"),
		format!("UPDATE {table} {{ v: 5 }} FILTER {{ id == 1 }}"),
		format!("DELETE {table} FILTER {{ id == 4 }}"),
		format!("UPDATE {table} {{ g: 3, v: 99 }} FILTER {{ id == 2 }}"),
		format!("DELETE {table} FILTER {{ id == 5 }}; INSERT {table} [{{ id: 9, g: 2, sym: 'i', v: 75 }}]"),
	]
}

fn between(table: &str) -> Vec<String> {
	vec![
		format!("DELETE {table} FILTER {{ id == 5 }}"),
		format!("UPDATE {table} {{ v: 90 }} FILTER {{ id == 4 }}"),
		format!("UPDATE {table} {{ v: 10 }} FILTER {{ id == 1 }}"),
		format!("INSERT {table} [{{ id: 8, g: 1, sym: 'h', v: 99 }}]"),
	]
}

fn run(db: &TestDb, statements: &[String]) {
	for rql in statements {
		db.command(rql);
	}
}

fn render(cells: Vec<(String, Value)>, cols: Cols) -> String {
	if matches!(cols, Cols::WithRownum) {
		assert!(cells.iter().any(|(name, _)| name == "#rownum"), "a row came back without #rownum: {cells:?}");
	}
	let mut cells: Vec<String> = cells
		.into_iter()
		.filter(|(name, _)| !name.starts_with('#') || (matches!(cols, Cols::WithRownum) && name == "#rownum"))
		.map(|(name, value)| format!("{name}={value}"))
		.collect();
	cells.sort();
	cells.join(",")
}

fn rows(db: &TestDb, rql: &str, cols: Cols) -> Vec<String> {
	let mut rows: Vec<String> =
		db.query(rql).iter().flat_map(|frame| frame.to_rows()).map(|row| render(row, cols)).collect();
	rows.sort();
	rows
}

fn sorted(want: &[&str]) -> Vec<String> {
	let mut want: Vec<String> = want.iter().map(|row| row.to_string()).collect();
	want.sort();
	want
}

fn settle(db: &TestDb) {
	assert!(db.await_all_flows(TIMEOUT), "the deferred flows did not catch up");
}

fn stop_without_flow(db: &mut TestDb) {
	// A stop with no flow never drains the cdc producer, so an unproduced write is lost to the next life.
	let head = db.engine().current_version().expect("the current version after the last write");
	poll_until(|| (db.engine().cdc_producer_watermark() >= head).then_some(()), TIMEOUT)
		.expect("the cdc of the last write was never written before the stop");
	db.stop();
}

fn await_rows(db: &TestDb, rql: &str, cols: Cols, want: &[String], step: &str) {
	let got = await_value(want.to_vec(), TIMEOUT, || rows(db, rql, cols));
	assert_eq!(got, want, "{rql} is wrong {step}");
}

fn agree(db: &TestDb, early: &str, late: &str, cols: Cols, step: &str) {
	settle(db);
	let want = rows(db, &format!("FROM {early}"), cols);
	let got = await_value(want.clone(), TIMEOUT, || rows(db, &format!("FROM {late}"), cols));
	assert_eq!(got, want, "{late} diverged from {early} {step}");
}

fn twins(db: &TestDb, create: impl Fn(&str) -> String, cols: Cols, past: &[String], want: &[&str], future: &[String]) {
	db.admin(&create("bf::early"));
	settle(db);
	run(db, past);
	settle(db);
	await_rows(db, "FROM bf::early", Cols::User, &sorted(want), "after the history, fed live");
	db.admin(&create("bf::late"));
	agree(db, "bf::early", "bf::late", cols, "right after the late create");
	for rql in future {
		db.command(rql);
		agree(db, "bf::early", "bf::late", cols, &format!("after: {rql}"));
	}
}

fn late_equals_early(
	db: &TestDb,
	columns: &str,
	body: &str,
	cols: Cols,
	past: &[String],
	want: &[&str],
	future: &[String],
) {
	twins(
		db,
		|name| format!("CREATE DEFERRED VIEW {name} {{ {columns} }} AS {{ {body} }}"),
		cols,
		past,
		want,
		future,
	);
}

fn only(table: &str) -> (Vec<String>, Vec<String>) {
	(history(table), later(table))
}

fn both() -> (Vec<String>, Vec<String>) {
	let past = [history("bf::src"), history("bf::src2")].concat();
	let future = [later("bf::src"), later("bf::src2")].concat();
	(past, future)
}

fn count(engine: &StandardEngine, rql: &str) -> Result<usize, String> {
	let result = engine.query_as(IdentityId::root(), rql, Params::None);
	match result.error {
		Some(error) => Err(format!("{error:?}")),
		None => Ok(result.frames.iter().map(|frame| frame.row_count()).sum()),
	}
}

fn only_flow(db: &TestDb) -> FlowId {
	let ids: Vec<u64> = db
		.query("FROM system::flows")
		.iter()
		.flat_map(|frame| frame.rows())
		.map(|row| row.get::<u64>("id").expect("the flow id column reads").expect("a flow id"))
		.collect();
	assert_eq!(ids.len(), 1, "the database must hold exactly the one view's flow: {ids:?}");
	FlowId(ids[0])
}

fn checkpoint(db: &TestDb, flow: FlowId) -> Option<CommitVersion> {
	db.engine().operator_state().checkpoint_get(flow).expect("the checkpoint lookup")
}

fn catalog_id(db: &TestDb, system: &str, name: &str) -> u64 {
	let ids: Vec<u64> = db
		.query(&format!("FROM system::{system} FILTER {{ name == '{name}' }}"))
		.iter()
		.flat_map(|frame| frame.rows())
		.map(|row| row.get::<u64>("id").expect("the id column reads").expect("an id"))
		.collect();
	assert_eq!(ids.len(), 1, "'{name}' must name exactly one entry in system::{system}: {ids:?}");
	ids[0]
}

fn flow_named(db: &TestDb, name: &str) -> FlowId {
	FlowId(catalog_id(db, "flows", name))
}

fn table_object(db: &TestDb, name: &str) -> ObjectId {
	ObjectId::Table(TableId(catalog_id(db, "tables", name)))
}

fn view_object(db: &TestDb, name: &str) -> ObjectId {
	ObjectId::View(ViewId(catalog_id(db, "views", name)))
}

fn stamps(db: &TestDb, object: ObjectId) -> Vec<ChangeVersion> {
	// Without a retry, a commit racing the read fails the truncation check on a read that was complete.
	let batch = poll_until(
		|| {
			let batch = db
				.engine()
				.cdc_store()
				.read_range(Bound::Unbounded, Bound::Unbounded, 10_000)
				.expect("a full cdc read");
			(!batch.has_more).then_some(batch)
		},
		TIMEOUT,
	)
	.expect("a truncated cdc read would hide commits");
	batch.items.iter().filter(|cdc| changed_objects(cdc).contains(&object)).map(|cdc| cdc.version).collect()
}

fn ddl_cursor(db: &TestDb) -> Option<CommitVersion> {
	let mut query = db.engine().begin_query(IdentityId::root()).expect("a query transaction");
	CdcCheckpoint::fetch_opt(&mut Transaction::Query(&mut query), &CdcConsumerId::flow_consumer())
		.expect("the ddl cursor lookup")
}

fn operators(db: &TestDb, flow: FlowId) -> Vec<OperatorId> {
	let operators: Vec<OperatorId> = db
		.query(&format!("FROM system::flow::operators FILTER {{ flow_id == {} }}", flow.0))
		.iter()
		.flat_map(|frame| frame.rows())
		.map(|row| {
			OperatorId(row.get::<u64>("id").expect("the operator id column reads").expect("an operator id"))
		})
		.collect();
	assert!(!operators.is_empty(), "the flow {flow:?} must have operators");
	operators
}

fn state_bytes(db: &TestDb, operators: &[OperatorId]) -> u64 {
	operators
		.iter()
		.map(|operator| {
			db.engine().operator_state().bytes(*operator).expect("the operator state size").as_bytes()
		})
		.sum()
}

fn refused() -> Error {
	Error(Box::new(Diagnostic {
		code: REFUSED.to_string(),
		message: "refused deferred backfill scan".to_string(),
		..Default::default()
	}))
}

fn sealed(db: &TestDb) -> CommitVersion {
	let cdc = db.engine().cdc_store();
	let head = db.engine().current_version().expect("the current version");
	let produced = await_value(true, TIMEOUT, || {
		cdc.max_version().expect("the cdc max version").is_some_and(|max| max >= head)
	});
	assert!(produced, "the cdc must hold every commit up to {head:?} before it is sealed");
	assert!(cdc.flush_pending(), "the cdc history must be sealed before it can be truncated");
	head
}

fn evict_before(db: &TestDb, cutoff: CommitVersion) -> CommitVersion {
	let cdc = db.engine().cdc_store();
	cdc.drop_before(Cutoff::Version(cutoff), usize::MAX).expect("drop the cdc history");
	cdc.truncated_before().expect("the cdc truncation floor")
}

struct Recorder {
	opens: Mutex<Vec<ObjectId>>,
}

impl Recorder {
	fn new() -> Arc<Self> {
		Arc::new(Self {
			opens: Mutex::new(Vec::new()),
		})
	}
}

impl ScanHooks for Recorder {
	fn on_open(&self, source: ObjectId) -> Outcome {
		self.opens.lock().push(source);
		Outcome::Land
	}
}

struct Watcher {
	engine: Mutex<Option<StandardEngine>>,
	view: String,
	seen: Mutex<Vec<Result<usize, String>>>,
}

impl ScanHooks for Watcher {
	fn during_next(&self, _source: ObjectId, _pull: u64) {
		let engine = self.engine.lock().clone();
		let seen = match engine {
			Some(engine) => count(&engine, &format!("FROM {}", self.view)),
			None => Err("the watcher was pulled after its engine handle was released".to_string()),
		};
		self.seen.lock().push(seen);
	}
}

struct Writer {
	engine: Mutex<Option<StandardEngine>>,
	rql: String,
	fired: AtomicBool,
	written: Mutex<Vec<Result<CommitVersion, String>>>,
}

impl Writer {
	fn new(rql: &str) -> Arc<Self> {
		Arc::new(Self {
			engine: Mutex::new(None),
			rql: rql.to_string(),
			fired: AtomicBool::new(false),
			written: Mutex::new(Vec::new()),
		})
	}

	fn bind(&self, db: &TestDb) {
		*self.engine.lock() = Some(db.engine().clone());
	}

	fn release(&self) {
		self.engine.lock().take();
	}

	fn engine(&self) -> Option<StandardEngine> {
		let deadline = Clock::Real.instant() + TIMEOUT;
		loop {
			if let Some(engine) = self.engine.lock().clone() {
				return Some(engine);
			}
			if Clock::Real.instant() >= deadline {
				return None;
			}
			sleep(Duration::from_milliseconds_const(10).to_std());
		}
	}
}

struct Probe {
	engine: Mutex<Option<StandardEngine>>,
	seen: Mutex<Vec<Result<CommitVersion, String>>>,
}

impl ScanHooks for Probe {
	fn during_open(&self, _source: ObjectId) {
		let engine = self.engine.lock().clone();
		let seen = match engine {
			Some(engine) => engine.current_version().map_err(|error| format!("{error:?}")),
			None => Err("the probe was opened after its engine handle was released".to_string()),
		};
		self.seen.lock().push(seen);
	}
}

struct Floors {
	engine: Mutex<Option<StandardEngine>>,
	seen: Mutex<Vec<Result<Option<CommitVersion>, String>>>,
}

impl Floors {
	fn record(&self) {
		let engine = self.engine.lock().clone();
		let seen = match engine {
			Some(engine) => {
				engine.operator_state().checkpoint_floor().map_err(|error| format!("{error:?}"))
			}
			None => Err("the floor probe ran after its engine handle was released".to_string()),
		};
		self.seen.lock().push(seen);
	}
}

impl ScanHooks for Floors {
	fn during_open(&self, _source: ObjectId) {
		self.record();
	}

	fn during_next(&self, _source: ObjectId, _pull: u64) {
		self.record();
	}
}

struct Refuse {
	armed: AtomicBool,
	refused: Mutex<Vec<ObjectId>>,
}

impl ScanHooks for Refuse {
	fn on_open(&self, source: ObjectId) -> Outcome {
		if !self.armed.load(Ordering::SeqCst) {
			return Outcome::Land;
		}
		self.refused.lock().push(source);
		Outcome::Err(refused())
	}
}

struct Gate {
	info: RoutineInfo,
	armed: AtomicBool,
	held: AtomicBool,
	opened: Mutex<Vec<bool>>,
	open: Mutex<Receiver<()>>,
}

impl<'a> Routine<FunctionContext<'a>> for Gate {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, input_types: &[ValueType]) -> ValueType {
		input_types[0].clone()
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		if self.armed.swap(false, Ordering::SeqCst) {
			self.held.store(true, Ordering::SeqCst);
			let opened = self.open.lock().recv_timeout(TIMEOUT.to_std()).is_ok();
			self.opened.lock().push(opened);
		}
		Ok(rename(args[0].clone(), ctx.fragment.text()))
	}
}

impl Function for Gate {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}

impl ScanHooks for Writer {
	fn during_open(&self, _source: ObjectId) {
		if self.fired.swap(true, Ordering::SeqCst) {
			return;
		}
		let written = match self.engine() {
			None => Err("the writer's engine handle was never bound".to_string()),
			Some(engine) => {
				let result = engine.command_as(IdentityId::root(), &self.rql, Params::None);
				match result.error {
					Some(error) => Err(format!("{error:?}")),
					None => engine.current_version().map_err(|error| format!("{error:?}")),
				}
			}
		};
		self.written.lock().push(written);
	}
}

struct HoldThenRefuse {
	target: Mutex<Option<ObjectId>>,
	held: AtomicBool,
	opens: Mutex<u32>,
	released: Mutex<Vec<bool>>,
	release: Mutex<Receiver<()>>,
}

impl ScanHooks for HoldThenRefuse {
	fn on_open(&self, source: ObjectId) -> Outcome {
		if *self.target.lock() != Some(source) {
			return Outcome::Land;
		}
		*self.opens.lock() += 1;
		if self.held.swap(true, Ordering::SeqCst) {
			return Outcome::Land;
		}
		let released = self.release.lock().recv_timeout(TIMEOUT.to_std()).is_ok();
		self.released.lock().push(released);
		Outcome::Err(refused())
	}
}

#[test]
fn a_late_filter_view_equals_an_early_one() {
	// A snapshot row that skipped the filter, or kept a deleted row, shows up against the view fed live.
	let db = memory();
	let (past, future) = only("bf::src");
	late_equals_early(
		&db,
		COLUMNS,
		FILTER,
		Cols::WithRownum,
		&past,
		&["g=1,id=1,sym=a,v=60", "g=2,id=2,sym=b,v=80", "g=3,id=5,sym=e,v=70"],
		&future,
	);
}

#[test]
fn a_late_map_view_equals_an_early_one() {
	// The projection must keep every live row under the source's own row number.
	let db = memory();
	let (past, future) = only("bf::src");
	late_equals_early(
		&db,
		"id: int4, v: int4",
		"FROM bf::src | map { id, v }",
		Cols::WithRownum,
		&past,
		&["id=1,v=60", "id=2,v=80", "id=4,v=30", "id=5,v=70", "id=7,v=25"],
		&future,
	);
}

#[test]
fn a_late_extend_view_equals_an_early_one() {
	// The computed column must come from the row as it is at V, not from its first insert.
	let db = memory();
	let (past, future) = only("bf::src");
	late_equals_early(
		&db,
		"id: int4, g: int4, sym: utf8, v: int4, w: int4",
		"FROM bf::src | extend { w: v + 1 }",
		Cols::WithRownum,
		&past,
		&[
			"g=1,id=1,sym=a,v=60,w=61",
			"g=2,id=2,sym=b,v=80,w=81",
			"g=2,id=4,sym=d,v=30,w=31",
			"g=3,id=5,sym=e,v=70,w=71",
			"g=3,id=7,sym=g,v=25,w=26",
		],
		&future,
	);
}

#[test]
fn a_late_append_view_equals_an_early_one() {
	// Both sources must be scanned, and each branch must stamp the lane row numbers the live path stamps.
	let db = memory();
	let (past, future) = both();
	late_equals_early(
		&db,
		COLUMNS,
		"FROM bf::src | append { FROM bf::src2 }",
		Cols::WithRownum,
		&past,
		&[
			"g=1,id=1,sym=a,v=60",
			"g=1,id=1,sym=a,v=60",
			"g=2,id=2,sym=b,v=80",
			"g=2,id=2,sym=b,v=80",
			"g=2,id=4,sym=d,v=30",
			"g=2,id=4,sym=d,v=30",
			"g=3,id=5,sym=e,v=70",
			"g=3,id=5,sym=e,v=70",
			"g=3,id=7,sym=g,v=25",
			"g=3,id=7,sym=g,v=25",
		],
		&future,
	);
}

#[test]
fn a_late_aggregate_view_equals_an_early_one() {
	// A snapshot row fed twice, or a deleted row fed at all, moves the count, the sum and the max.
	let db = memory();
	let (past, future) = only("bf::src");
	late_equals_early(
		&db,
		AGGREGATE_COLUMNS,
		AGGREGATE,
		Cols::User,
		&past,
		&["g=1,hi=60,n=1,total=60", "g=2,hi=80,n=2,total=110", "g=3,hi=70,n=2,total=95"],
		&future,
	);
}

#[test]
fn a_late_aggregate_view_built_one_row_per_chunk_equals_an_early_one() {
	// Group state must carry across every chunk of one backfill, or each chunk resets the group it touches.
	let db = memory_with(1);
	let (past, future) = only("bf::src");
	late_equals_early(
		&db,
		AGGREGATE_COLUMNS,
		AGGREGATE,
		Cols::User,
		&past,
		&["g=1,hi=60,n=1,total=60", "g=2,hi=80,n=2,total=110", "g=3,hi=70,n=2,total=95"],
		&future,
	);
}

#[test]
fn a_late_take_view_equals_an_early_one() {
	// Take ranks by stored stamps, so the snapshot must keep the same rows the early view kept from live changes.
	let db = memory();
	let (past, future) = only("bf::src");
	late_equals_early(
		&db,
		COLUMNS,
		"FROM bf::src | take 3",
		Cols::User,
		&past,
		&["g=2,id=4,sym=d,v=30", "g=3,id=5,sym=e,v=70", "g=3,id=7,sym=g,v=25"],
		&future,
	);
}

#[test]
fn a_late_sorted_view_equals_an_early_one() {
	// A sort key taken from a stale value keys the row twice or in the wrong place.
	let db = memory();
	let (past, future) = only("bf::src");
	late_equals_early(
		&db,
		COLUMNS,
		SORTED,
		Cols::WithRownum,
		&past,
		&[
			"g=1,id=1,sym=a,v=60",
			"g=2,id=2,sym=b,v=80",
			"g=2,id=4,sym=d,v=30",
			"g=3,id=5,sym=e,v=70",
			"g=3,id=7,sym=g,v=25",
		],
		&future,
	);
}

fn ring_history(table: &str) -> Vec<String> {
	vec![
		format!(
			"INSERT {table} [{{ id: 1, g: 1, sym: 'a', v: 5 }}, {{ id: 2, g: 1, sym: 'b', v: 50 }}, {{ id: 3, g: 2, sym: 'c', v: 95 }}, {{ id: 4, g: 2, sym: 'd', v: 30 }}]"
		),
		format!("INSERT {table} [{{ id: 5, g: 3, sym: 'e', v: 70 }}]"),
		format!("UPDATE {table} {{ v: 31 }} FILTER {{ id == 4 }}"),
		format!("DELETE {table} FILTER {{ id == 1 }}"),
		format!(
			"INSERT {table} [{{ id: 6, g: 1, sym: 'f', v: 10 }}]; INSERT {table} [{{ id: 7, g: 3, sym: 'g', v: 55 }}]"
		),
		format!("UPDATE {table} {{ v: 56 }} FILTER {{ id == 7 }}"),
	]
}

fn ring_later(table: &str) -> Vec<String> {
	vec![
		format!("INSERT {table} [{{ id: 8, g: 1, sym: 'h', v: 90 }}]"),
		format!("UPDATE {table} {{ v: 57 }} FILTER {{ id == 7 }}"),
		format!("DELETE {table} FILTER {{ id == 8 }}"),
		format!("INSERT {table} [{{ id: 9, g: 2, sym: 'i', v: 75 }}, {{ id: 10, g: 2, sym: 'j', v: 1 }}]"),
		format!("DELETE {table} FILTER {{ id == 2 }}"),
		format!("UPDATE {table} {{ v: 2 }} FILTER {{ id == 10 }}"),
	]
}

fn newest_three() -> [&'static str; 3] {
	["g=3,id=5,sym=e,v=70", "g=1,id=6,sym=f,v=10", "g=3,id=7,sym=g,v=56"]
}

fn ring(capacity: u32, source: &str) -> impl Fn(&str) -> String {
	let source = source.to_string();
	move |name| {
		format!(
			"CREATE DEFERRED RINGBUFFER VIEW {name} {{ {COLUMNS} }} WITH {{ capacity: {capacity} }} AS {{ FROM {source} }}"
		)
	}
}

#[test]
fn a_late_ringbuffer_view_keeps_the_same_newest_rows_as_an_early_one() {
	// Snapshot rows fed newest first make the ring evict the newest ones and keep the oldest.
	let db = memory();
	twins(&db, ring(3, "bf::src"), Cols::User, &ring_history("bf::src"), &newest_three(), &ring_later("bf::src"));
}

#[test]
fn a_late_ringbuffer_view_built_one_row_per_chunk_keeps_the_same_newest_rows_as_an_early_one() {
	// Chunks fed newest chunk first keep the oldest rows even when each chunk alone is in order.
	let db = memory_with(1);
	twins(&db, ring(3, "bf::src"), Cols::User, &ring_history("bf::src"), &newest_three(), &ring_later("bf::src"));
}

#[test]
fn a_late_ringbuffer_view_over_a_ringbuffer_source_keeps_the_same_newest_rows_as_an_early_one() {
	// The ringbuffer source has its own scan node, and it too must hand its rows over oldest first.
	let db = memory();
	db.admin(&format!("CREATE RINGBUFFER bf::rsrc {{ {COLUMNS} }} WITH {{ capacity: 5 }}"));
	twins(
		&db,
		ring(3, "bf::rsrc"),
		Cols::User,
		&ring_history("bf::rsrc"),
		&newest_three(),
		&ring_later("bf::rsrc"),
	);
}

fn partitioned_ring_history(table: &str) -> Vec<String> {
	vec![
		format!(
			"INSERT {table} [{{ id: 1, g: 1, sym: 'a', v: 5 }}, {{ id: 2, g: 1, sym: 'b', v: 50 }}, {{ id: 3, g: 1, sym: 'c', v: 95 }}, {{ id: 4, g: 2, sym: 'd', v: 30 }}]"
		),
		format!("INSERT {table} [{{ id: 5, g: 2, sym: 'e', v: 70 }}, {{ id: 6, g: 2, sym: 'f', v: 10 }}]"),
		format!("UPDATE {table} {{ v: 96 }} FILTER {{ id == 3 }}"),
		format!("DELETE {table} FILTER {{ id == 4 }}"),
		format!(
			"INSERT {table} [{{ id: 7, g: 1, sym: 'g', v: 55 }}]; INSERT {table} [{{ id: 8, g: 3, sym: 'h', v: 1 }}]"
		),
		format!("UPDATE {table} {{ v: 11 }} FILTER {{ id == 6 }}"),
	]
}

fn partitioned_ring_later(table: &str) -> Vec<String> {
	vec![
		format!("INSERT {table} [{{ id: 9, g: 1, sym: 'i', v: 75 }}]"),
		format!("UPDATE {table} {{ v: 71 }} FILTER {{ id == 5 }}"),
		format!("DELETE {table} FILTER {{ id == 9 }}"),
		format!(
			"INSERT {table} [{{ id: 10, g: 2, sym: 'j', v: 1 }}, {{ id: 11, g: 3, sym: 'k', v: 2 }}, {{ id: 12, g: 3, sym: 'l', v: 3 }}]"
		),
		format!("DELETE {table} FILTER {{ id == 1 }}"),
		format!("UPDATE {table} {{ v: 4 }} FILTER {{ id == 12 }}"),
	]
}

fn partitioned_ring(batch: u16) {
	let db = memory_with(batch);
	twins(
		&db,
		|name| {
			format!(
				"CREATE DEFERRED RINGBUFFER VIEW {name} {{ {COLUMNS} }} WITH {{ capacity: 2, partition: {{ by: {{ g }} }} }} AS {{ FROM bf::src }}"
			)
		},
		Cols::User,
		&partitioned_ring_history("bf::src"),
		&[
			"g=1,id=3,sym=c,v=96",
			"g=1,id=7,sym=g,v=55",
			"g=2,id=5,sym=e,v=70",
			"g=2,id=6,sym=f,v=11",
			"g=3,id=8,sym=h,v=1",
		],
		&partitioned_ring_later("bf::src"),
	);
}

#[test]
fn a_late_partitioned_ringbuffer_view_keeps_the_same_newest_rows_per_partition_as_an_early_one() {
	// Each partition evicts on its own, so a reversed feed keeps the oldest rows of every full partition.
	partitioned_ring(4);
}

#[test]
fn a_late_partitioned_ringbuffer_view_built_one_row_per_chunk_keeps_the_same_newest_rows_per_partition() {
	// One row per chunk still has to arrive oldest first across chunks, or a full partition keeps its oldest rows.
	partitioned_ring(1);
}

fn six(source: &str) -> Vec<String> {
	(1..=6).map(|id| format!("INSERT {source} [{{ id: {id}, g: {}, v: {} }}]", 2 - id % 2, id * 10)).collect()
}

fn six_later(source: &str) -> Vec<String> {
	vec![
		format!("INSERT {source} [{{ id: 7, g: 1, v: 70 }}]"),
		format!("UPDATE {source} {{ v: 61 }} FILTER {{ id == 6 }}"),
		format!("DELETE {source} FILTER {{ id == 5 }}"),
		format!("INSERT {source} [{{ id: 8, g: 2, v: 80 }}, {{ id: 9, g: 1, v: 90 }}]"),
		format!("UPDATE {source} {{ v: 41 }} FILTER {{ id == 4 }}"),
	]
}

fn newest_of_six() -> [&'static str; 3] {
	["g=2,id=4,v=40", "g=1,id=5,v=50", "g=2,id=6,v=60"]
}

fn ring_of_three(columns: &'static str, source: &str) -> impl Fn(&str) -> String {
	let source = source.to_string();
	move |name| {
		format!(
			"CREATE DEFERRED RINGBUFFER VIEW {name} {{ {columns} }} WITH {{ capacity: 3 }} AS {{ FROM {source} }}"
		)
	}
}

fn ring_over_a_partitioned_table(batch: u16) {
	let db = memory_with(batch);
	db.admin("CREATE NAMESPACE p");
	db.admin(&format!("CREATE TABLE p::t {{ {SIX_COLUMNS} }} WITH {{ partition: {{ by: {{ g }} }} }}"));
	twins(&db, ring_of_three(SIX_COLUMNS, "p::t"), Cols::User, &six("p::t"), &newest_of_six(), &six_later("p::t"));
}

#[test]
fn a_late_ringbuffer_view_over_a_partitioned_table_keeps_the_same_newest_rows_as_an_early_one() {
	// Partitions handed over one after another end on one partition's rows, not on the newest across all of them.
	ring_over_a_partitioned_table(4);
}

#[test]
fn a_late_ringbuffer_view_over_a_partitioned_table_built_one_row_per_chunk_keeps_the_same_newest_rows() {
	// The merge must refill each partition chunk by chunk, or a row at a chunk edge arrives out of order or twice.
	ring_over_a_partitioned_table(1);
}

fn ring_over_a_partitioned_ringbuffer(batch: u16) {
	let db = memory_with(batch);
	db.admin("CREATE NAMESPACE p");
	db.admin(&format!(
		"CREATE RINGBUFFER p::rb {{ {SIX_COLUMNS} }} WITH {{ capacity: 8, partition: {{ by: {{ g }} }} }}"
	));
	twins(
		&db,
		ring_of_three(SIX_COLUMNS, "p::rb"),
		Cols::User,
		&six("p::rb"),
		&newest_of_six(),
		&six_later("p::rb"),
	);
}

#[test]
fn a_late_ringbuffer_view_over_a_partitioned_ringbuffer_keeps_the_same_newest_rows_as_an_early_one() {
	// A partitioned ringbuffer source read one partition after another ends on one partition's rows.
	ring_over_a_partitioned_ringbuffer(4);
}

#[test]
fn a_late_ringbuffer_view_over_a_partitioned_ringbuffer_built_one_row_per_chunk_keeps_the_same_newest_rows() {
	// Each ringbuffer partition must be merged chunk by chunk, or a row at a chunk edge arrives out of order.
	ring_over_a_partitioned_ringbuffer(1);
}

fn ring_over_a_deferred_view(batch: u16) {
	let db = memory_with(batch);
	db.admin("CREATE NAMESPACE p");
	db.admin(&format!("CREATE TABLE p::base {{ {SIX_COLUMNS} }}"));
	db.admin(&format!("CREATE DEFERRED VIEW p::mid {{ {SIX_COLUMNS} }} AS {{ FROM p::base }}"));
	settle(&db);
	twins(
		&db,
		ring_of_three(SIX_COLUMNS, "p::mid"),
		Cols::User,
		&six("p::base"),
		&newest_of_six(),
		&six_later("p::base"),
	);
}

#[test]
fn a_late_ringbuffer_view_over_a_deferred_view_keeps_the_same_newest_rows_as_an_early_one() {
	// A view source scanned in its stored key order hands the newest row over first, so the ring keeps the oldest.
	ring_over_a_deferred_view(4);
}

#[test]
fn a_late_ringbuffer_view_over_a_deferred_view_built_one_row_per_chunk_keeps_the_same_newest_rows() {
	// An oldest-first view scan must resume after the last row of each chunk, or rows are skipped or repeated.
	ring_over_a_deferred_view(1);
}

fn series_six() -> Vec<String> {
	(1..=6).map(|id| format!("INSERT p::s [{{ ts: {}, id: {id}, g: {} }}]", id * 100, 2 - id % 2)).collect()
}

fn series_later() -> Vec<String> {
	vec![
		"INSERT p::s [{ ts: 700, id: 7, g: 1 }]".to_string(),
		"UPDATE p::s { g: 3 } FILTER { id == 6 }".to_string(),
		"DELETE p::s FILTER { id == 5 }".to_string(),
		"INSERT p::s [{ ts: 800, id: 8, g: 2 }, { ts: 900, id: 9, g: 1 }]".to_string(),
		"UPDATE p::s { g: 4 } FILTER { id == 4 }".to_string(),
	]
}

fn ring_over_a_series(batch: u16) {
	let db = memory_with(batch);
	db.admin("CREATE NAMESPACE p");
	db.admin(&format!("CREATE SERIES p::s {{ {SERIES_COLUMNS} }} WITH {{ key: ts }}"));
	twins(
		&db,
		ring_of_three(SERIES_COLUMNS, "p::s"),
		Cols::User,
		&series_six(),
		&["g=2,id=4,ts=400", "g=1,id=5,ts=500", "g=2,id=6,ts=600"],
		&series_later(),
	);
}

#[test]
fn a_late_ringbuffer_view_over_a_series_keeps_the_same_newest_rows_as_an_early_one() {
	// A series scanned newest key first makes the ring evict the newest rows and keep the oldest.
	ring_over_a_series(4);
}

#[test]
fn a_late_ringbuffer_view_over_a_series_built_one_row_per_chunk_keeps_the_same_newest_rows() {
	// An oldest-first series scan must resume after the last key of each chunk, or rows are skipped or repeated.
	ring_over_a_series(1);
}

fn join_statements() -> (Vec<String>, Vec<String>) {
	let (past, future) = both();
	let past = [
		past,
		vec![
			"UPDATE bf::src2 { v: 1 } FILTER { id == 2 }".to_string(),
			"DELETE bf::src2 FILTER { id == 1 }".to_string(),
		],
	]
	.concat();
	(past, future)
}

#[test]
fn a_late_join_view_equals_an_early_one() {
	// Both sides must be read at the same V; a side read later or not at all drops or duplicates matches.
	let db = memory();
	let (past, future) = join_statements();
	late_equals_early(
		&db,
		"id: int4, g: int4, v: int4, rv: int4",
		"FROM bf::src INNER JOIN { FROM bf::src2 } AS r USING (id, r.id) | map { id, g, v, rv: r_v }",
		Cols::User,
		&past,
		&["g=2,id=2,rv=1,v=80", "g=2,id=4,rv=30,v=30", "g=3,id=5,rv=70,v=70", "g=3,id=7,rv=25,v=25"],
		&future,
	);
}

#[test]
fn a_late_join_view_built_one_row_per_chunk_equals_an_early_one() {
	// Join state from earlier chunks must be probed by later ones, or matches across chunks are lost.
	let db = memory_with(1);
	let (past, future) = join_statements();
	late_equals_early(
		&db,
		"id: int4, g: int4, v: int4, rv: int4",
		"FROM bf::src INNER JOIN { FROM bf::src2 } AS r USING (id, r.id) | map { id, g, v, rv: r_v }",
		Cols::User,
		&past,
		&["g=2,id=2,rv=1,v=80", "g=2,id=4,rv=30,v=30", "g=3,id=5,rv=70,v=70", "g=3,id=7,rv=25,v=25"],
		&future,
	);
}

const SNAPSHOT_JOIN_COLUMNS: &str = "id: int4, g: int4, v: int4, rv: int4";

const SNAPSHOT_PAIRS: [&str; 4] =
	["g=1,id=1,rv=100,v=60", "g=2,id=2,rv=201,v=80", "g=3,id=5,rv=500,v=70", "g=3,id=7,rv=700,v=25"];

fn snapshot_join(kind: &str, left: &str) -> String {
	format!(
		"FROM {left} {kind} JOIN {{ FROM bf::src2 }} AS r USING (id, r.id) WITH {{ snapshot: true, latest: true }} | map {{ id, g, v, rv: r_v }}"
	)
}

fn snapshot_join_history() -> Vec<String> {
	[
		vec![
			"INSERT bf::src2 [{ id: 7, g: 0, sym: 'r', v: 700 }, { id: 5, g: 0, sym: 'r', v: 500 }, { id: 4, g: 0, sym: 'r', v: 400 }, { id: 3, g: 0, sym: 'r', v: 300 }, { id: 2, g: 0, sym: 'r', v: 200 }, { id: 1, g: 0, sym: 'r', v: 100 }]".to_string(),
			"UPDATE bf::src2 { v: 201 } FILTER { id == 2 }".to_string(),
			"DELETE bf::src2 FILTER { id == 4 }".to_string(),
		],
		history("bf::src"),
	]
	.concat()
}

fn snapshot_join_later() -> Vec<String> {
	[
		"INSERT bf::src2 [{ id: 4, g: 0, sym: 'r', v: 401 }, { id: 8, g: 0, sym: 'r', v: 800 }]",
		"UPDATE bf::src2 { v: 101 } FILTER { id == 1 }",
		"UPDATE bf::src { v: 61 } FILTER { id == 1 }",
		"INSERT bf::src [{ id: 8, g: 1, sym: 'h', v: 90 }]",
		"UPDATE bf::src { v: 31 } FILTER { id == 4 }",
		"DELETE bf::src FILTER { id == 5 }",
		"UPDATE bf::src2 { v: 202 } FILTER { id == 2 }; UPDATE bf::src { v: 82 } FILTER { id == 2 }",
		"INSERT bf::src [{ id: 9, g: 2, sym: 'i', v: 75 }]",
		"INSERT bf::src2 [{ id: 9, g: 0, sym: 'r', v: 900 }]",
		"DELETE bf::src2 FILTER { id == 7 }",
		"UPDATE bf::src { v: 26 } FILTER { id == 7 }",
	]
	.map(str::to_string)
	.to_vec()
}

fn left_before_right(db: &TestDb) {
	assert!(
		table_object(db, "src") < table_object(db, "src2"),
		"precondition: the left table must sort first, so a feed in ObjectId order runs the left side first"
	);
}

#[test]
fn a_late_snapshot_join_view_equals_an_early_one() {
	// A snapshot join never pairs a left row with a right row fed after it, so the right side must be fed first.
	let db = memory();
	left_before_right(&db);
	late_equals_early(
		&db,
		SNAPSHOT_JOIN_COLUMNS,
		&snapshot_join("INNER", "bf::src"),
		Cols::User,
		&snapshot_join_history(),
		&SNAPSHOT_PAIRS,
		&snapshot_join_later(),
	);
}

#[test]
fn a_late_snapshot_join_view_built_one_row_per_chunk_equals_an_early_one() {
	// Right rows sit in reverse id order, so alternating right and left chunks feeds some left row first.
	let db = memory_with(1);
	left_before_right(&db);
	late_equals_early(
		&db,
		SNAPSHOT_JOIN_COLUMNS,
		&snapshot_join("INNER", "bf::src"),
		Cols::User,
		&snapshot_join_history(),
		&SNAPSHOT_PAIRS,
		&snapshot_join_later(),
	);
}

#[test]
fn a_late_left_snapshot_join_view_pairs_every_row_an_early_one_pairs() {
	// A left side fed first leaves every row unpaired that the early view paired with an earlier right row.
	let db = memory();
	left_before_right(&db);
	let mut want = SNAPSHOT_PAIRS.to_vec();
	want.push("g=2,id=4,rv=none,v=30");
	late_equals_early(
		&db,
		"id: int4, g: int4, v: int4, rv: Option(int4)",
		&snapshot_join("LEFT", "bf::src"),
		Cols::User,
		&snapshot_join_history(),
		&want,
		&snapshot_join_later(),
	);
}

#[test]
fn a_late_ungrouped_aggregate_over_a_snapshot_join_equals_an_early_one() {
	// With no view between join and aggregate, pairs lost to the feed order leave the count and the total short.
	let db = memory();
	left_before_right(&db);
	late_equals_early(
		&db,
		"n: int8, total: Option(int4)",
		"FROM bf::src INNER JOIN { FROM bf::src2 } AS r USING (id, r.id) WITH { snapshot: true, latest: true } | aggregate { n: math::count(id), total: math::sum(r_v) } by {}",
		Cols::User,
		&snapshot_join_history(),
		&["n=4,total=1501"],
		&snapshot_join_later(),
	);
}

#[test]
fn a_late_snapshot_join_view_created_in_the_same_commit_as_its_data_equals_an_early_one() {
	// V holds both sides written with the create, and the snapshot must still feed the right side before the left.
	let db = memory();
	left_before_right(&db);
	let create = |name: &str| {
		format!(
			"CREATE DEFERRED VIEW {name} {{ {SNAPSHOT_JOIN_COLUMNS} }} AS {{ {} }}",
			snapshot_join("INNER", "bf::src")
		)
	};
	db.admin(&create("bf::early"));
	settle(&db);
	let before = db.engine().current_version().expect("the current version");
	db.admin(&format!(
		"INSERT bf::src2 [{{ id: 7, g: 0, sym: 'r', v: 700 }}, {{ id: 5, g: 0, sym: 'r', v: 500 }}, {{ id: 3, g: 0, sym: 'r', v: 300 }}, {{ id: 2, g: 0, sym: 'r', v: 201 }}, {{ id: 1, g: 0, sym: 'r', v: 100 }}]; INSERT bf::src [{{ id: 1, g: 1, sym: 'a', v: 60 }}, {{ id: 2, g: 2, sym: 'b', v: 80 }}, {{ id: 4, g: 2, sym: 'd', v: 30 }}, {{ id: 5, g: 3, sym: 'e', v: 70 }}, {{ id: 7, g: 3, sym: 'g', v: 25 }}]; {}",
		create("bf::late")
	));
	settle(&db);
	await_rows(
		&db,
		"FROM bf::early",
		Cols::User,
		&sorted(&SNAPSHOT_PAIRS),
		"after the writes committed with the late create, fed live",
	);
	agree(&db, "bf::early", "bf::late", Cols::User, "right after the create committed with its data");
	let commits = |table: &str| -> Vec<CommitVersion> {
		stamps(&db, table_object(&db, table))
			.into_iter()
			.map(|stamp| stamp.commit)
			.filter(|commit| *commit > before)
			.collect()
	};
	let left = commits("src");
	assert_eq!(left.len(), 1, "precondition: the left writes must land in one commit: {left:?}");
	assert_eq!(commits("src2"), left, "precondition: both sides must be written in the create's own commit");
	for rql in snapshot_join_later() {
		db.command(&rql);
		agree(&db, "bf::early", "bf::late", Cols::User, &format!("after: {rql}"));
	}
}

#[test]
fn a_late_snapshot_join_view_over_a_left_view_and_a_right_table_equals_an_early_one() {
	// A feed that puts view sources first runs the left view before the right table, and nothing pairs.
	let db = memory();
	db.admin(&format!("CREATE DEFERRED VIEW bf::lv {{ {COLUMNS} }} AS {{ FROM bf::src }}"));
	late_equals_early(
		&db,
		SNAPSHOT_JOIN_COLUMNS,
		&snapshot_join("INNER", "bf::lv"),
		Cols::User,
		&snapshot_join_history(),
		&SNAPSHOT_PAIRS,
		&snapshot_join_later(),
	);
}

#[test]
fn a_late_view_over_a_dictionary_column_equals_an_early_one() {
	// The scan decodes dictionary ids, so the late view must show the same text the live path shows.
	let db = memory();
	let (past, future) = only("bf::dsrc");
	late_equals_early(
		&db,
		COLUMNS,
		"FROM bf::dsrc",
		Cols::WithRownum,
		&past,
		&[
			"g=1,id=1,sym=a,v=60",
			"g=2,id=2,sym=b,v=80",
			"g=2,id=4,sym=d,v=30",
			"g=3,id=5,sym=e,v=70",
			"g=3,id=7,sym=g,v=25",
		],
		&future,
	);
}

#[test]
fn a_late_view_over_a_late_view_equals_an_early_chain() {
	// The second hop may backfill before the first has written anything, and must still catch up exactly.
	let db = memory();
	db.admin(&format!(
		"CREATE DEFERRED VIEW bf::early1 {{ {COLUMNS} }} AS {{ FROM bf::src | filter {{ v > 20 }} }}"
	));
	db.admin("CREATE DEFERRED VIEW bf::early2 { id: int4, v: int4 } AS { FROM bf::early1 | map { id, v } }");
	run(&db, &history("bf::src"));
	settle(&db);
	await_rows(
		&db,
		"FROM bf::early2",
		Cols::User,
		&sorted(&["id=1,v=60", "id=2,v=80", "id=4,v=30", "id=5,v=70", "id=7,v=25"]),
		"after the history, fed live",
	);
	db.admin(&format!(
		"CREATE DEFERRED VIEW bf::late1 {{ {COLUMNS} }} AS {{ FROM bf::src | filter {{ v > 20 }} }}"
	));
	db.admin("CREATE DEFERRED VIEW bf::late2 { id: int4, v: int4 } AS { FROM bf::late1 | map { id, v } }");
	agree(&db, "bf::early2", "bf::late2", Cols::WithRownum, "right after the late creates");
	for rql in later("bf::src") {
		db.command(&rql);
		agree(&db, "bf::early2", "bf::late2", Cols::WithRownum, &format!("after: {rql}"));
	}
}

#[test]
fn a_late_view_over_an_early_view_equals_an_early_chain() {
	// A view source is read from what the upstream has written, and must then follow its later writes.
	let db = memory();
	db.admin(&format!(
		"CREATE DEFERRED VIEW bf::early1 {{ {COLUMNS} }} AS {{ FROM bf::src | filter {{ v > 20 }} }}"
	));
	db.admin("CREATE DEFERRED VIEW bf::early2 { id: int4, v: int4 } AS { FROM bf::early1 | map { id, v } }");
	run(&db, &history("bf::src"));
	settle(&db);
	await_rows(
		&db,
		"FROM bf::early2",
		Cols::User,
		&sorted(&["id=1,v=60", "id=2,v=80", "id=4,v=30", "id=5,v=70", "id=7,v=25"]),
		"after the history, fed live",
	);
	db.admin("CREATE DEFERRED VIEW bf::late2 { id: int4, v: int4 } AS { FROM bf::early1 | map { id, v } }");
	agree(&db, "bf::early2", "bf::late2", Cols::WithRownum, "right after the late create");
	for rql in later("bf::src") {
		db.command(&rql);
		agree(&db, "bf::early2", "bf::late2", Cols::WithRownum, &format!("after: {rql}"));
	}
}

#[test]
fn a_late_view_reads_a_producer_past_v_when_the_producer_commits_a_write_at_or_below_v_after_v() {
	// Read at V the held row is missing, and the live hand-off drops it again as a change at or below V.
	let (open, opened) = channel();
	let gate = Arc::new(Gate {
		info: RoutineInfo::new("gate::pass"),
		armed: AtomicBool::new(false),
		held: AtomicBool::new(false),
		opened: Mutex::new(Vec::new()),
		open: Mutex::new(opened),
	});
	let function = gate.clone();
	let db = TestDb::from(
		embedded::memory()
			.with_runtime_config(runtime())
			.with_config(ConfigKey::QueryRowBatchSize, Value::Uint2(4))
			.with_routines(move |routines| routines.register_function(function))
			.with_flow(|f| f)
			.build()
			.expect("build memory db with flow and the gate function"),
	);
	tables(&db);
	let reader = "{ id: int4, v: int4 } AS { FROM bf::p | filter { v > 20 } | map { id, v } }";
	db.admin(&format!(
		"CREATE DEFERRED VIEW bf::p {{ {COLUMNS} }} AS {{ FROM bf::src | map {{ id, g, sym, v: gate::pass(v) }} }}"
	));
	db.admin(&format!("CREATE DEFERRED VIEW bf::early {reader}"));
	settle(&db);
	run(&db, &history("bf::src"));
	settle(&db);
	let before = sorted(&["id=1,v=60", "id=2,v=80", "id=4,v=30", "id=5,v=70", "id=7,v=25"]);
	await_rows(&db, "FROM bf::early", Cols::User, &before, "after the history, fed live");

	gate.armed.store(true, Ordering::SeqCst);
	db.command("INSERT bf::src [{ id: 20, g: 2, sym: 't', v: 44 }]");
	assert!(
		await_value(true, TIMEOUT, || gate.held.load(Ordering::SeqCst)),
		"precondition: the producer must be held inside the slice of the write"
	);
	db.admin(&format!("CREATE DEFERRED VIEW bf::late {reader}"));
	let v = db.engine().current_version().expect("the current version");
	sleep(HOLD.to_std());
	assert!(
		!rows(&db, "FROM bf::p", Cols::User).iter().any(|row| row.contains("id=20,")),
		"precondition: at V the producer must not hold the held row yet"
	);
	open.send(()).expect("release the producer");

	let after = sorted(&["id=1,v=60", "id=2,v=80", "id=4,v=30", "id=5,v=70", "id=7,v=25", "id=20,v=44"]);
	await_rows(&db, "FROM bf::late", Cols::User, &after, "after the producer committed the held row past V");
	agree(&db, "bf::early", "bf::late", Cols::WithRownum, "after the producer committed the held row past V");
	assert_eq!(
		*gate.opened.lock(),
		vec![true],
		"the producer must be released by the test, not by the hold timeout"
	);
	let late = stamps(&db, view_object(&db, "late"));
	assert_eq!(
		late.first().map(|stamp| stamp.source),
		Some(SourceVersion::from(v)),
		"precondition: the late backfill must snapshot at its create, before the producer was released: {late:?}"
	);
	let produced = stamps(&db, view_object(&db, "p"));
	assert!(
		produced.iter().any(|stamp| stamp.commit > v && stamp.source <= SourceVersion::from(v)),
		"precondition: the producer must commit a write at or below V after V ({v:?}): {produced:?}"
	);
	for rql in later("bf::src") {
		db.command(&rql);
		agree(&db, "bf::early", "bf::late", Cols::WithRownum, &format!("after: {rql}"));
	}
}

#[test]
fn a_late_snapshot_join_over_a_late_view_whose_backfill_retried_keeps_v_and_equals_an_early_chain() {
	// A retry that snapshots past V stamps the producer past the consumer's cut, so the consumer reads it empty.
	let (release, parked) = channel();
	let hook = Arc::new(HoldThenRefuse {
		target: Mutex::new(None),
		held: AtomicBool::new(false),
		opens: Mutex::new(0),
		released: Mutex::new(Vec::new()),
		release: Mutex::new(parked),
	});
	let db = TestDb::from(
		embedded::memory()
			.with_runtime_config(runtime())
			.with_config(ConfigKey::QueryRowBatchSize, Value::Uint2(4))
			.with_dependency(InstalledScanHooks(hook.clone()))
			.with_flow(|f| f)
			.build()
			.expect("build memory db with flow and the scan hooks"),
	);
	tables(&db);
	db.admin("CREATE TABLE bf::level { pool: utf8, px: int8 }");
	db.admin("CREATE TABLE bf::snap { pool: utf8 }");
	let curve = |name: &str| {
		format!(
			"CREATE DEFERRED VIEW bf::{name} {{ pool: utf8, usd: int8 }} AS {{ FROM bf::level MAP {{ pool, usd: px * 2 }} }}"
		)
	};
	let hop = |name: &str, over: &str| {
		format!(
			"CREATE DEFERRED VIEW bf::{name} {{ pool: utf8, usd: int8 }} AS {{ FROM bf::{over} MAP {{ pool, usd }} }}"
		)
	};
	let cost = |name: &str, over: &str| {
		format!(
			"CREATE DEFERRED VIEW bf::{name} {{ pool: utf8, usd: int8 }} AS {{ FROM bf::snap INNER JOIN {{ FROM bf::{over} }} AS c USING (pool, c.pool) WITH {{ snapshot: true, latest: true }} MAP {{ pool, usd: c_usd }} }}"
		)
	};
	let paired = sorted(&["pool=a,usd=20"]);

	db.admin(&curve("early_curve"));
	db.admin(&hop("early_curve2", "early_curve"));
	db.admin(&cost("early_cost", "early_curve2"));
	settle(&db);
	db.command("INSERT bf::level [{ pool: 'a', px: 10 }]");
	db.command("INSERT bf::snap [{ pool: 'a' }]");
	settle(&db);
	await_rows(&db, "FROM bf::early_cost", Cols::User, &paired, "after the rung and the header, fed live");

	db.admin(&curve("curve"));
	settle(&db);
	await_rows(&db, "FROM bf::curve", Cols::User, &paired, "right after the first hop backfilled");
	*hook.target.lock() = Some(view_object(&db, "curve"));
	db.admin(&hop("curve2", "curve"));
	assert!(
		await_value(true, TIMEOUT, || hook.held.load(Ordering::SeqCst)),
		"precondition: the second hop's backfill must be parked in its scan of the first hop"
	);
	db.admin(&cost("cost", "curve2"));
	// Never wait on the consumer's snapshot itself: its cut must hold until the producer has committed.
	sleep(HOLD.to_std());
	// A commit after V that a retry at "current" would fold into the producer's backfill.
	db.command("INSERT bf::src [{ id: 1, g: 1, sym: 'a', v: 5 }]");
	let unrelated = db.engine().current_version().expect("the current version after the unrelated commit");
	release.send(()).expect("release the second hop's backfill into its refusal");
	settle(&db);

	await_rows(&db, "FROM bf::curve2", Cols::User, &paired, "after the second hop's backfill retried");
	assert_eq!(
		*hook.released.lock(),
		vec![true],
		"the second hop must be released by the test, not by the hold timeout"
	);
	assert_eq!(
		*hook.opens.lock(),
		2,
		"precondition: the second hop must open the first hop exactly twice, the refused open and one retry"
	);
	let produced = stamps(&db, view_object(&db, "curve2"));
	assert!(
		produced.first().is_some_and(|stamp| stamp.source < SourceVersion::from(unrelated)),
		"the second hop's retry must keep its create version, below the unrelated commit ({unrelated:?}): {produced:?}"
	);
	agree(
		&db,
		"bf::early_cost",
		"bf::cost",
		Cols::User,
		"after its producer's backfill retried at its create version",
	);
}

fn follows_the_source(db: &TestDb, view: &str, source: &str, cols: Cols, step: &str) {
	settle(db);
	let want = rows(db, source, cols);
	let got = await_value(want.clone(), TIMEOUT, || rows(db, &format!("FROM {view}"), cols));
	assert_eq!(got, want, "{view} diverged from {source} {step}");
}

fn full_after_eviction(db: &TestDb, floor: CommitVersion, first_kept: CommitVersion) {
	let full = sorted(&[
		"g=1,id=1,sym=a,v=60",
		"g=2,id=2,sym=b,v=80",
		"g=2,id=4,sym=d,v=30",
		"g=3,id=5,sym=e,v=70",
		"g=3,id=7,sym=g,v=25",
	]);
	assert_eq!(rows(db, "FROM bf::src", Cols::User), full, "the eviction must not touch the table itself");
	assert!(floor <= first_kept, "the eviction went further than asked: floor {floor:?}, kept from {first_kept:?}");
	db.admin(&format!("CREATE DEFERRED VIEW bf::late {{ {COLUMNS} }} AS {{ FROM bf::src }}"));
	await_rows(db, "FROM bf::late", Cols::User, &full, "right after a create over evicted history");
	follows_the_source(
		db,
		"bf::late",
		"FROM bf::src",
		Cols::WithRownum,
		"right after a create over evicted history",
	);
	for rql in later("bf::src") {
		db.command(&rql);
		follows_the_source(db, "bf::late", "FROM bf::src", Cols::WithRownum, &format!("after: {rql}"));
	}
}

#[test]
fn a_view_created_after_every_cdc_entry_of_its_rows_was_evicted_is_full() {
	// Replaying CDC to build the view silently drops every row whose history is gone.
	let db = memory();
	run(&db, &history("bf::src"));
	let head = sealed(&db);
	let floor = evict_before(&db, CommitVersion(head.0 + 1));
	assert!(floor > head, "precondition: every history commit must be evicted, floor {floor:?}, head {head:?}");
	full_after_eviction(&db, floor, CommitVersion(head.0 + 1));
}

#[test]
fn a_view_created_after_part_of_its_cdc_history_was_evicted_is_full() {
	// A replay of only the retained tail keeps later updates but loses the rows they update.
	let db = memory();
	let statements = history("bf::src");
	run(&db, &statements[..3]);
	let evicted = sealed(&db);
	run(&db, &statements[3..4]);
	let first_kept = db.engine().current_version().expect("the current version");
	run(&db, &statements[4..]);
	sealed(&db);
	let floor = evict_before(&db, CommitVersion(evicted.0 + 1));
	assert!(floor > evicted, "precondition: the first commits must be evicted, floor {floor:?}");
	full_after_eviction(&db, floor, first_kept);
}

fn rolling(name: &str, table: &str) -> String {
	format!(
		"CREATE DEFERRED VIEW {name} {{ g: int4, total: float8 }} AS {{ FROM {table} | window rolling {{ total: math::sum(v) }} with {{ duration: 1h, lateness: 5m }} by {{ g }} }}"
	)
}

#[test]
fn a_late_event_time_rolling_view_leaves_rows_older_than_the_window_out() {
	// One chunk holds old and recent rows, so only each row's own event time can age the old ones out.
	let db = memory_with(64);
	db.admin("CREATE TABLE bf::ev { id: int4, g: int4, v: float8, ts: datetime } with { time: event(ts) }");
	db.admin(&rolling("bf::early", "bf::ev"));
	db.command(
		r#"INSERT bf::ev [{ id: 1, g: 1, v: 1.0, ts: "2026-01-01T00:00:00Z" }, { id: 2, g: 1, v: 2.0, ts: "2026-01-01T00:30:00Z" }]"#,
	);
	db.command(
		r#"INSERT bf::ev [{ id: 3, g: 2, v: 8.0, ts: "2026-01-01T04:30:00Z" }, { id: 4, g: 1, v: 4.0, ts: "2026-01-01T05:00:00Z" }]"#,
	);
	settle(&db);
	let want = sorted(&["g=1,total=4", "g=2,total=8"]);
	await_rows(&db, "FROM bf::early", Cols::User, &want, "after the old and the recent rows, fed live");
	db.admin(&rolling("bf::late", "bf::ev"));
	await_rows(&db, "FROM bf::late", Cols::User, &want, "right after the late create");
	agree(&db, "bf::early", "bf::late", Cols::User, "right after the late create");
	db.command(r#"INSERT bf::ev [{ id: 5, g: 2, v: 16.0, ts: "2026-01-01T05:20:00Z" }]"#);
	await_rows(&db, "FROM bf::late", Cols::User, &sorted(&["g=1,total=4", "g=2,total=24"]), "after a live row");
	agree(&db, "bf::early", "bf::late", Cols::User, "after a live row");
}

#[test]
fn a_late_processing_time_rolling_view_leaves_rows_older_than_the_window_out() {
	// The chunk's stamp is the newest row's update time, so a window reading it would keep the old rows.
	let clock = MockClock::new(START_NANOS);
	let db = TestDb::from(
		embedded::memory()
			.with_runtime_config(runtime().clock(Clock::Mock(clock.clone())))
			.with_config(ConfigKey::QueryRowBatchSize, Value::Uint2(64))
			.with_flow(|f| f)
			.build()
			.expect("build memory db with flow and a mock clock"),
	);
	db.admin("CREATE NAMESPACE bf");
	db.admin("CREATE TABLE bf::pt { id: int4, g: int4, v: float8 } with { time: processing }");
	db.admin(&rolling("bf::early", "bf::pt"));
	db.command("INSERT bf::pt [{ id: 1, g: 1, v: 1.0 }, { id: 2, g: 1, v: 2.0 }]");
	settle(&db);
	await_rows(&db, "FROM bf::early", Cols::User, &sorted(&["g=1,total=3"]), "after the old rows, fed live");
	clock.advance_hours(3);
	db.command("INSERT bf::pt [{ id: 3, g: 1, v: 4.0 }, { id: 4, g: 2, v: 8.0 }]");
	settle(&db);
	let want = sorted(&["g=1,total=4", "g=2,total=8"]);
	await_rows(&db, "FROM bf::early", Cols::User, &want, "after the recent rows, fed live");
	db.admin(&rolling("bf::late", "bf::pt"));
	await_rows(&db, "FROM bf::late", Cols::User, &want, "right after the late create");
	agree(&db, "bf::early", "bf::late", Cols::User, "right after the late create");
}

fn bulk(table: &str) -> Vec<String> {
	let mut statements = Vec::new();
	for commit in 0..4 {
		let values: Vec<String> = (1..=10)
			.map(|n| {
				let id = commit * 10 + n;
				format!("{{ id: {id}, g: {}, sym: 's{id}', v: {} }}", id % 3, id * 2)
			})
			.collect();
		statements.push(format!("INSERT {table} [{}]", values.join(", ")));
	}
	statements.push(format!("DELETE {table} FILTER {{ id % 4 == 0 }}"));
	statements.push(format!("UPDATE {table} {{ v: 1 }} FILTER {{ id % 5 == 0 }}"));
	statements
}

#[test]
fn a_backfill_larger_than_the_batch_size_is_exact_and_readers_never_see_part_of_it() {
	// A commit per chunk shows a reader a partial view; a chunk dropped at a boundary shows a missing row.
	let db = memory_with(4);
	db.admin(&format!("CREATE DEFERRED VIEW bf::early {{ {COLUMNS} }} AS {{ FROM bf::src }}"));
	run(&db, &bulk("bf::src"));
	settle(&db);
	let live = 30;
	assert_eq!(rows(&db, "FROM bf::src", Cols::User).len(), live, "precondition: 40 inserted, 10 deleted");
	follows_the_source(&db, "bf::early", "FROM bf::src", Cols::WithRownum, "after the bulk load, fed live");

	let watcher = Arc::new(Watcher {
		engine: Mutex::new(Some(db.engine().clone())),
		view: "bf::late".to_string(),
		seen: Mutex::new(Vec::new()),
	});
	db.engine().ioc().register_service(InstalledScanHooks(watcher.clone()));
	db.admin(&format!("CREATE DEFERRED VIEW bf::late {{ {COLUMNS} }} AS {{ FROM bf::src }}"));

	let stop = Arc::new(AtomicBool::new(false));
	let reader = {
		let engine = db.engine().clone();
		let stop = stop.clone();
		spawn(move || {
			let mut seen = BTreeSet::new();
			while !stop.load(Ordering::SeqCst) {
				seen.insert(count(&engine, "FROM bf::late"));
				sleep(Duration::from_milliseconds_const(1).to_std());
			}
			// Without this read, a reader paused before the commit never sees the full view.
			seen.insert(count(&engine, "FROM bf::late"));
			seen
		})
	};
	follows_the_source(&db, "bf::late", "FROM bf::src", Cols::WithRownum, "right after the late create");
	agree(&db, "bf::early", "bf::late", Cols::WithRownum, "right after the late create");
	stop.store(true, Ordering::SeqCst);
	let polled = reader.join().expect("the polling reader");
	watcher.engine.lock().take();

	let seen = watcher.seen.lock().clone();
	let chunks = live.div_ceil(4);
	assert!(
		seen.len() >= chunks,
		"30 live rows at batch size 4 need at least {chunks} pulls, so at least that many mid-backfill reads: {seen:?}"
	);
	assert!(
		seen.iter().all(|read| *read == Ok(0)),
		"a read between chunks must see the view empty until the one commit at the end: {seen:?}"
	);
	let allowed: BTreeSet<Result<usize, String>> = [Ok(0), Ok(live)].into_iter().collect();
	assert!(polled.is_subset(&allowed), "a concurrent reader saw a partial view: {polled:?}");
	assert!(polled.contains(&Ok(live)), "the concurrent reader never saw the full view: {polled:?}");
}

fn stale_rows() -> Vec<String> {
	sorted(&["g=1,id=1,sym=a,v=60", "g=2,id=2,sym=b,v=80", "g=3,id=5,sym=e,v=70"])
}

fn fresh_rows() -> Vec<String> {
	sorted(&["g=2,id=2,sym=b,v=80", "g=2,id=4,sym=d,v=90", "g=1,id=8,sym=h,v=99"])
}

fn fresh_aggregates() -> Vec<String> {
	sorted(&["g=1,hi=99,n=2,total=109", "g=2,hi=90,n=2,total=170", "g=3,hi=25,n=1,total=25"])
}

fn stale_aggregates() -> Vec<String> {
	sorted(&["g=1,hi=60,n=1,total=60", "g=2,hi=80,n=2,total=110", "g=3,hi=70,n=2,total=95"])
}

fn keeps_following(db: &TestDb, view: &str, source: &str, cols: Cols) {
	for rql in later("bf::src") {
		db.command(&rql);
		settle(db);
		let want = rows(db, source, cols);
		let got = await_value(want.clone(), TIMEOUT, || rows(db, &format!("FROM {view}"), cols));
		assert_eq!(got, want, "{view} diverged from {source} after the restart and: {rql}");
	}
}

#[derive(Clone, Copy, PartialEq)]
enum Image {
	CheckpointLost,
	StateAndCheckpointLost,
}

fn deferred_view(columns: &str, body: &str) -> String {
	format!("CREATE DEFERRED VIEW bf::v {{ {columns} }} AS {{ {body} }}")
}

fn crash_after_the_view_commit(
	tag: &str,
	create: &str,
	body: &str,
	cols: Cols,
	stale: Vec<String>,
	fresh: Vec<String>,
	image: Image,
) {
	let path = crash_image(tag, create, &stale, image, &between("bf::src"));
	let mut db = open(&path, true, None);
	await_rows(&db, "FROM bf::v", Cols::User, &fresh, "after a restart with no checkpoint");
	follows_the_source(&db, "bf::v", body, cols, "after a restart with no checkpoint");
	keeps_following(&db, "bf::v", body, cols);
	db.stop();
}

fn crash_image(tag: &str, create: &str, stale: &[String], image: Image, offline: &[String]) -> TempDbPath {
	let path = TempDbPath::new(tag);
	{
		let mut db = open(&path, true, None);
		tables(&db);
		run(&db, &history("bf::src"));
		db.admin(create);
		settle(&db);
		await_rows(&db, "FROM bf::v", Cols::User, stale, "after the first backfill");
		let flow = only_flow(&db);
		assert!(checkpoint(&db, flow).is_some(), "precondition: the first backfill must leave a checkpoint");
		if image == Image::StateAndCheckpointLost {
			let operators = operators(&db, flow);
			assert!(
				state_bytes(&db, &operators) > 0,
				"precondition: the first backfill must leave operator state to lose"
			);
			for operator in &operators {
				db.engine().operator_state().drop_operator(*operator).expect("drop the operator state");
			}
			assert_eq!(state_bytes(&db, &operators), 0, "precondition: the operator state must be gone");
		}
		db.engine().operator_state().checkpoint_remove(flow).expect("remove the checkpoint");
		assert_eq!(checkpoint(&db, flow), None, "precondition: the checkpoint must be gone");
		db.stop();
	}
	{
		let mut db = open(&path, false, None);
		let flow = only_flow(&db);
		assert_eq!(checkpoint(&db, flow), None, "precondition: the crash image must hold no checkpoint");
		if image == Image::StateAndCheckpointLost {
			assert_eq!(
				state_bytes(&db, &operators(&db, flow)),
				0,
				"precondition: the crash image must hold no operator state"
			);
		}
		assert_eq!(
			rows(&db, "FROM bf::v", Cols::User),
			stale,
			"precondition: the view rows of the first life stay"
		);
		run(&db, offline);
		stop_without_flow(&mut db);
	}
	path
}

fn ring_crash_after_the_view_commit(
	tag: &str,
	create: &str,
	stale: &[&str],
	offline: &[String],
	fresh: &[&str],
	steps: &[(&str, &[&str])],
) {
	let path = crash_image(tag, create, &sorted(stale), Image::CheckpointLost, offline);
	let mut db = open(&path, true, None);
	await_rows(&db, "FROM bf::v", Cols::User, &sorted(fresh), "after a restart with no checkpoint");
	for (rql, want) in steps {
		db.command(rql);
		settle(&db);
		await_rows(&db, "FROM bf::v", Cols::User, &sorted(want), &format!("after the restart and: {rql}"));
	}
	db.stop();
}

#[test]
fn a_crash_before_the_backfill_scan_fills_the_view_on_restart() {
	// Seeding the restart at the create's cursor instead of backfilling skips every row older than the view.
	let path = TempDbPath::new("deferred_backfill_before_scan");
	{
		let mut db = open(&path, false, None);
		tables(&db);
		run(&db, &history("bf::src"));
		db.admin(&format!("CREATE DEFERRED VIEW bf::v {{ {COLUMNS} }} AS {{ {FILTER} }}"));
		assert_eq!(rows(&db, "FROM bf::v", Cols::User), Vec::<String>::new(), "precondition: no flow ran yet");
		run(&db, &between("bf::src"));
		stop_without_flow(&mut db);
	}
	let mut db = open(&path, true, None);
	await_rows(&db, "FROM bf::v", Cols::User, &fresh_rows(), "after a restart that never scanned");
	follows_the_source(&db, "bf::v", FILTER, Cols::WithRownum, "after a restart that never scanned");
	keeps_following(&db, "bf::v", FILTER, Cols::WithRownum);
	db.stop();
}

#[test]
fn a_crash_after_the_view_commit_clears_the_stale_rows_on_restart() {
	// A restart that backfills on top of the old view rows keeps the row deleted after the first V.
	crash_after_the_view_commit(
		"deferred_backfill_after_view_commit",
		&deferred_view(COLUMNS, FILTER),
		FILTER,
		Cols::WithRownum,
		stale_rows(),
		fresh_rows(),
		Image::CheckpointLost,
	);
}

#[test]
fn a_crash_after_the_view_commit_resets_half_written_operator_state_on_restart() {
	// Group state left without a checkpoint must be reset, or the restart's backfill counts every row twice.
	crash_after_the_view_commit(
		"deferred_backfill_half_written_state",
		&deferred_view(AGGREGATE_COLUMNS, AGGREGATE),
		AGGREGATE,
		Cols::User,
		stale_aggregates(),
		fresh_aggregates(),
		Image::CheckpointLost,
	);
}

#[test]
fn a_crash_between_the_view_commit_and_the_operator_store_write_rebuilds_the_view_on_restart() {
	// View rows whose group state is lost must be cleared, or the rebuilt groups land next to the stale rows.
	crash_after_the_view_commit(
		"deferred_backfill_view_rows_without_state",
		&deferred_view(AGGREGATE_COLUMNS, AGGREGATE),
		AGGREGATE,
		Cols::User,
		stale_aggregates(),
		fresh_aggregates(),
		Image::StateAndCheckpointLost,
	);
}

fn partitioned_view(body: &str) -> String {
	format!("CREATE DEFERRED VIEW bf::v {{ {COLUMNS} }} WITH {{ partition: {{ by: {{ sym }} }} }} AS {{ {body} }}")
}

fn stale_over_twenty() -> Vec<String> {
	sorted(&[
		"g=1,id=1,sym=a,v=60",
		"g=2,id=2,sym=b,v=80",
		"g=2,id=4,sym=d,v=30",
		"g=3,id=5,sym=e,v=70",
		"g=3,id=7,sym=g,v=25",
	])
}

fn fresh_over_twenty() -> Vec<String> {
	sorted(&["g=2,id=2,sym=b,v=80", "g=2,id=4,sym=d,v=90", "g=3,id=7,sym=g,v=25", "g=1,id=8,sym=h,v=99"])
}

#[test]
fn a_crash_after_the_view_commit_clears_the_stale_sorted_rows_on_restart() {
	// Stale sorted keys keep the row deleted after V next to the rebuilt rows.
	crash_after_the_view_commit(
		"deferred_backfill_sorted_after_view_commit",
		&deferred_view(COLUMNS, SORTED),
		SORTED,
		Cols::WithRownum,
		stale_over_twenty(),
		fresh_over_twenty(),
		Image::CheckpointLost,
	);
}

#[test]
fn a_crash_after_the_view_commit_clears_the_stale_partitioned_rows_on_restart() {
	// Stale partitioned keys keep the row deleted after V next to the rebuilt rows.
	crash_after_the_view_commit(
		"deferred_backfill_partitioned_after_view_commit",
		&partitioned_view(FILTER),
		FILTER,
		Cols::WithRownum,
		stale_rows(),
		fresh_rows(),
		Image::CheckpointLost,
	);
}

#[test]
fn a_crash_after_the_view_commit_clears_the_stale_partitioned_sorted_rows_on_restart() {
	// Stale partitioned sorted keys keep the row deleted after V next to the rebuilt rows.
	crash_after_the_view_commit(
		"deferred_backfill_partitioned_sorted_after_view_commit",
		&partitioned_view(SORTED),
		SORTED,
		Cols::WithRownum,
		stale_over_twenty(),
		fresh_over_twenty(),
		Image::CheckpointLost,
	);
}

#[test]
fn a_crash_after_the_view_commit_clears_the_stale_series_rows_on_restart() {
	// Stale series keys keep the row deleted after V next to the rebuilt rows.
	crash_after_the_view_commit(
		"deferred_backfill_series_after_view_commit",
		&format!("CREATE DEFERRED SERIES VIEW bf::v {{ {COLUMNS} }} WITH {{ key: id }} AS {{ {FILTER} }}"),
		FILTER,
		Cols::WithRownum,
		stale_rows(),
		fresh_rows(),
		Image::CheckpointLost,
	);
}

#[test]
fn a_crash_after_the_view_commit_clears_the_stale_partitioned_series_rows_on_restart() {
	// Stale partitioned series keys keep the row deleted after V next to the rebuilt rows.
	crash_after_the_view_commit(
		"deferred_backfill_partitioned_series_after_view_commit",
		&format!(
			"CREATE DEFERRED SERIES VIEW bf::v {{ {COLUMNS} }} WITH {{ key: id, partition: {{ by: {{ sym }} }} }} AS {{ {FILTER} }}"
		),
		FILTER,
		Cols::WithRownum,
		stale_rows(),
		fresh_rows(),
		Image::CheckpointLost,
	);
}

#[test]
fn a_crash_after_the_view_commit_resets_the_ringbuffer_view_on_restart() {
	// Stale ring metadata or slots past the smaller new snapshot must never reach the rebuilt ring.
	ring_crash_after_the_view_commit(
		"deferred_backfill_ring_after_view_commit",
		&ring(3, "bf::src")("bf::v"),
		&["g=2,id=4,sym=d,v=30", "g=3,id=5,sym=e,v=70", "g=3,id=7,sym=g,v=25"],
		&[between("bf::src"), vec!["DELETE bf::src FILTER { id == 2 or id == 7 }".to_string()]].concat(),
		&["g=1,id=1,sym=a,v=10", "g=2,id=4,sym=d,v=90", "g=1,id=8,sym=h,v=99"],
		&[
			(
				"INSERT bf::src [{ id: 9, g: 2, sym: 'i', v: 75 }]",
				&["g=2,id=4,sym=d,v=90", "g=1,id=8,sym=h,v=99", "g=2,id=9,sym=i,v=75"],
			),
			(
				"UPDATE bf::src { v: 5 } FILTER { id == 8 }",
				&["g=2,id=4,sym=d,v=90", "g=1,id=8,sym=h,v=5", "g=2,id=9,sym=i,v=75"],
			),
			("DELETE bf::src FILTER { id == 4 }", &["g=1,id=8,sym=h,v=5", "g=2,id=9,sym=i,v=75"]),
			(
				"INSERT bf::src [{ id: 10, g: 3, sym: 'j', v: 1 }]",
				&["g=1,id=8,sym=h,v=5", "g=2,id=9,sym=i,v=75", "g=3,id=10,sym=j,v=1"],
			),
			(
				"INSERT bf::src [{ id: 11, g: 1, sym: 'k', v: 2 }]",
				&["g=2,id=9,sym=i,v=75", "g=3,id=10,sym=j,v=1", "g=1,id=11,sym=k,v=2"],
			),
		],
	);
}

#[test]
fn a_crash_after_the_view_commit_resets_the_partitioned_ringbuffer_view_on_restart() {
	// Stale per-partition ring metadata or slots must never reach the rebuilt partitions.
	ring_crash_after_the_view_commit(
		"deferred_backfill_partitioned_ring_after_view_commit",
		&format!(
			"CREATE DEFERRED RINGBUFFER VIEW bf::v {{ {COLUMNS} }} WITH {{ capacity: 2, partition: {{ by: {{ g }} }} }} AS {{ FROM bf::src }}"
		),
		&[
			"g=1,id=1,sym=a,v=60",
			"g=2,id=2,sym=b,v=80",
			"g=2,id=4,sym=d,v=30",
			"g=3,id=5,sym=e,v=70",
			"g=3,id=7,sym=g,v=25",
		],
		&between("bf::src"),
		&[
			"g=1,id=1,sym=a,v=10",
			"g=1,id=8,sym=h,v=99",
			"g=2,id=2,sym=b,v=80",
			"g=2,id=4,sym=d,v=90",
			"g=3,id=7,sym=g,v=25",
		],
		&[
			(
				"INSERT bf::src [{ id: 9, g: 1, sym: 'i', v: 75 }]",
				&[
					"g=1,id=8,sym=h,v=99",
					"g=1,id=9,sym=i,v=75",
					"g=2,id=2,sym=b,v=80",
					"g=2,id=4,sym=d,v=90",
					"g=3,id=7,sym=g,v=25",
				],
			),
			(
				"INSERT bf::src [{ id: 10, g: 3, sym: 'j', v: 1 }]",
				&[
					"g=1,id=8,sym=h,v=99",
					"g=1,id=9,sym=i,v=75",
					"g=2,id=2,sym=b,v=80",
					"g=2,id=4,sym=d,v=90",
					"g=3,id=7,sym=g,v=25",
					"g=3,id=10,sym=j,v=1",
				],
			),
			(
				"INSERT bf::src [{ id: 11, g: 3, sym: 'k', v: 2 }]",
				&[
					"g=1,id=8,sym=h,v=99",
					"g=1,id=9,sym=i,v=75",
					"g=2,id=2,sym=b,v=80",
					"g=2,id=4,sym=d,v=90",
					"g=3,id=10,sym=j,v=1",
					"g=3,id=11,sym=k,v=2",
				],
			),
			(
				"DELETE bf::src FILTER { id == 2 }",
				&[
					"g=1,id=8,sym=h,v=99",
					"g=1,id=9,sym=i,v=75",
					"g=2,id=4,sym=d,v=90",
					"g=3,id=10,sym=j,v=1",
					"g=3,id=11,sym=k,v=2",
				],
			),
			(
				"INSERT bf::src [{ id: 12, g: 2, sym: 'l', v: 3 }]",
				&[
					"g=1,id=8,sym=h,v=99",
					"g=1,id=9,sym=i,v=75",
					"g=2,id=4,sym=d,v=90",
					"g=2,id=12,sym=l,v=3",
					"g=3,id=10,sym=j,v=1",
					"g=3,id=11,sym=k,v=2",
				],
			),
		],
	);
}

#[test]
fn a_backfill_that_never_committed_is_redone_on_restart_after_the_ddl_cursor_passed_the_create() {
	// A flow with no checkpoint seeded at the persisted ddl cursor skips every row written before its create.
	let path = TempDbPath::new("deferred_backfill_never_committed");
	let refuse = Arc::new(Refuse {
		armed: AtomicBool::new(false),
		refused: Mutex::new(Vec::new()),
	});
	{
		let mut db = open(&path, true, Some(refuse.clone()));
		tables(&db);
		db.admin(&format!("CREATE DEFERRED VIEW bf::early {{ {COLUMNS} }} AS {{ {FILTER} }}"));
		settle(&db);
		run(&db, &history("bf::src"));
		settle(&db);
		await_rows(&db, "FROM bf::early", Cols::User, &stale_rows(), "after the history, fed live");
		refuse.armed.store(true, Ordering::SeqCst);
		db.admin(&format!("CREATE DEFERRED VIEW bf::late {{ {COLUMNS} }} AS {{ {FILTER} }}"));
		let create = db.engine().current_version().expect("the current version");
		sleep(PAST_CHECKPOINT_AGE.to_std());
		db.command("INSERT bf::src [{ id: 30, g: 1, sym: 'z', v: 99 }]");
		let cursor = poll_until(|| ddl_cursor(&db).filter(|cursor| *cursor >= create), DDL_CURSOR_WAIT);
		assert!(
			cursor.is_some(),
			"precondition: the ddl cursor must be persisted at or past the create {create:?}, got {:?}",
			ddl_cursor(&db)
		);
		assert_eq!(
			*refuse.refused.lock().first().expect("precondition: the late backfill must have been refused"),
			table_object(&db, "src"),
			"precondition: the refused scan must be the late view's source"
		);
		assert_eq!(
			checkpoint(&db, flow_named(&db, "late")),
			None,
			"precondition: a refused backfill must never commit a checkpoint"
		);
		assert_eq!(
			rows(&db, "FROM bf::late", Cols::User),
			Vec::<String>::new(),
			"precondition: a refused backfill must leave the view empty"
		);
		db.stop();
	}
	let mut db = open(&path, true, None);
	let nudged =
		sorted(&["g=1,id=1,sym=a,v=60", "g=2,id=2,sym=b,v=80", "g=3,id=5,sym=e,v=70", "g=1,id=30,sym=z,v=99"]);
	await_rows(&db, "FROM bf::late", Cols::User, &nudged, "after a restart");
	agree(&db, "bf::early", "bf::late", Cols::WithRownum, "after a restart");
	for rql in later("bf::src") {
		db.command(&rql);
		agree(&db, "bf::early", "bf::late", Cols::WithRownum, &format!("after the restart and: {rql}"));
	}
	db.stop();
}

#[test]
fn a_crash_after_the_operator_store_write_resumes_from_the_checkpoint_without_a_scan() {
	// With a checkpoint the flow resumes at V; a rescan would feed state that already holds every row.
	let path = TempDbPath::new("deferred_backfill_after_store_write");
	{
		let mut db = open(&path, true, None);
		tables(&db);
		run(&db, &history("bf::src"));
		db.admin(&format!("CREATE DEFERRED VIEW bf::v {{ {AGGREGATE_COLUMNS} }} AS {{ {AGGREGATE} }}"));
		settle(&db);
		await_rows(&db, "FROM bf::v", Cols::User, &stale_aggregates(), "after the first backfill");
		db.stop();
	}
	{
		let mut db = open(&path, false, None);
		let flow = only_flow(&db);
		assert!(checkpoint(&db, flow).is_some(), "precondition: a clean stop must leave the checkpoint");
		run(&db, &between("bf::src"));
		stop_without_flow(&mut db);
	}
	let recorder = Recorder::new();
	let mut db = open(&path, true, Some(recorder.clone()));
	await_rows(&db, "FROM bf::v", Cols::User, &fresh_aggregates(), "after a restart from the checkpoint");
	follows_the_source(&db, "bf::v", AGGREGATE, Cols::User, "after a restart from the checkpoint");
	keeps_following(&db, "bf::v", AGGREGATE, Cols::User);
	assert_eq!(*recorder.opens.lock(), Vec::<ObjectId>::new(), "a flow with a checkpoint must never scan again");
	db.stop();
}

#[test]
fn a_write_just_before_v_and_one_just_after_reach_the_view_exactly_once() {
	// A change at or below V is in the snapshot and must be dropped live; one above V must arrive live, once.
	let path = TempDbPath::new("deferred_backfill_hand_off");
	{
		let mut db = open(&path, false, None);
		tables(&db);
		run(&db, &history("bf::src"));
		db.admin(&format!("CREATE DEFERRED VIEW bf::v {{ {AGGREGATE_COLUMNS} }} AS {{ {AGGREGATE} }}"));
		db.command(
			"INSERT bf::src [{ id: 10, g: 1, sym: 'j', v: 7 }]; UPDATE bf::src { v: 61 } FILTER { id == 1 }",
		);
		stop_without_flow(&mut db);
	}
	let writer = Writer::new(
		"INSERT bf::src [{ id: 11, g: 2, sym: 'k', v: 3 }]; UPDATE bf::src { v: 81 } FILTER { id == 2 }; DELETE bf::src FILTER { id == 4 }",
	);
	let mut db = open(&path, true, Some(writer.clone()));
	writer.bind(&db);
	let want = sorted(&["g=1,hi=61,n=2,total=68", "g=2,hi=81,n=2,total=84", "g=3,hi=70,n=2,total=95"]);
	await_rows(&db, "FROM bf::v", Cols::User, &want, "after a write on each side of V");
	let written = writer.written.lock().clone();
	assert_eq!(written.len(), 1, "the write after V must run exactly once, inside the backfill: {written:?}");
	assert!(written[0].is_ok(), "the write after V failed: {written:?}");
	follows_the_source(&db, "bf::v", AGGREGATE, Cols::User, "after a write on each side of V");
	keeps_following(&db, "bf::v", AGGREGATE, Cols::User);
	writer.release();
	db.stop();
}

#[test]
fn a_source_write_committed_with_the_create_is_counted_exactly_once() {
	// V is the create's own commit here, so a live cursor one below V feeds that commit's writes a second time.
	let db = memory();
	run(&db, &history("bf::src"));
	let probe = Arc::new(Probe {
		engine: Mutex::new(Some(db.engine().clone())),
		seen: Mutex::new(Vec::new()),
	});
	db.engine().ioc().register_service(InstalledScanHooks(probe.clone()));
	let src = table_object(&db, "src");
	let before = db.engine().current_version().expect("the current version");
	db.admin(&format!(
		"INSERT bf::src [{{ id: 10, g: 1, sym: 'j', v: 7 }}, {{ id: 11, g: 3, sym: 'k', v: 9 }}]; UPDATE bf::src {{ v: 61 }} FILTER {{ id == 1 }}; DELETE bf::src FILTER {{ id == 5 }}; CREATE DEFERRED VIEW bf::v {{ {AGGREGATE_COLUMNS} }} AS {{ {AGGREGATE} }}"
	));
	let created = poll_until(
		|| {
			let commits: Vec<CommitVersion> = stamps(&db, src)
				.into_iter()
				.map(|stamp| stamp.commit)
				.filter(|commit| *commit > before)
				.collect();
			(!commits.is_empty()).then_some(commits)
		},
		TIMEOUT,
	)
	.expect("the create commit never reached the cdc store");
	assert_eq!(created.len(), 1, "precondition: the writes and the create must share one commit: {created:?}");
	let seen = await_value(1, TIMEOUT, || probe.seen.lock().len());
	assert_eq!(seen, 1, "precondition: the backfill must open its one source exactly once");
	assert_eq!(
		*probe.seen.lock(),
		vec![Ok(created[0])],
		"precondition: nothing may commit between the create and the snapshot, so V is the create's own commit"
	);
	let want = sorted(&["g=1,hi=61,n=2,total=68", "g=2,hi=80,n=2,total=110", "g=3,hi=25,n=2,total=34"]);
	await_rows(&db, "FROM bf::v", Cols::User, &want, "after writes committed with the create");
	follows_the_source(&db, "bf::v", AGGREGATE, Cols::User, "after writes committed with the create");
	keeps_following(&db, "bf::v", AGGREGATE, Cols::User);
	probe.engine.lock().take();
}

fn floor(db: &TestDb) -> Option<CommitVersion> {
	db.engine().operator_state().checkpoint_floor().expect("the checkpoint floor")
}

#[test]
fn a_backfill_holds_the_cdc_floor_at_v_until_its_checkpoint_commits() {
	// With no floor at V between the snapshot and the checkpoint, cdc retention can reap the changes after V.
	let db = memory();
	run(&db, &bulk("bf::src"));
	let floors = Arc::new(Floors {
		engine: Mutex::new(Some(db.engine().clone())),
		seen: Mutex::new(Vec::new()),
	});
	db.engine().ioc().register_service(InstalledScanHooks(floors.clone()));
	let stop = Arc::new(AtomicBool::new(false));
	let (started, sampling) = channel();
	let sampler = {
		let store = db.engine().operator_state();
		let stop = stop.clone();
		spawn(move || {
			let sample = || store.checkpoint_floor().map_err(|error| format!("{error:?}"));
			let mut changes = vec![sample()];
			started.send(()).expect("the test waits for the first sample");
			while !stop.load(Ordering::SeqCst) {
				let read = sample();
				if changes.last() != Some(&read) {
					changes.push(read);
				}
				yield_now();
			}
			changes
		})
	};
	sampling.recv_timeout(TIMEOUT.to_std()).expect("precondition: the floor sampler must run before the create");
	db.admin(&format!("CREATE DEFERRED VIEW bf::late {{ {COLUMNS} }} AS {{ FROM bf::src }}"));
	follows_the_source(&db, "bf::late", "FROM bf::src", Cols::WithRownum, "right after the late create");
	let flow = only_flow(&db);
	let committed =
		poll_until(|| checkpoint(&db, flow), TIMEOUT).expect("the backfill never committed a checkpoint");
	stop.store(true, Ordering::SeqCst);
	let changes = sampler.join().expect("the floor sampler");
	floors.engine.lock().take();

	let late = view_object(&db, "late");
	let first = poll_until(|| stamps(&db, late).first().copied(), TIMEOUT)
		.expect("the backfill commit never reached the cdc");
	let v = CommitVersion(first.source.0);
	assert!(committed >= v, "precondition: the checkpoint {committed:?} must be at or past V {v:?}");
	let seen = floors.seen.lock().clone();
	assert!(!seen.is_empty(), "precondition: the backfill must have opened and pulled its source");
	assert!(
		seen.iter().all(|read| matches!(read, Ok(Some(floor)) if *floor <= v)),
		"while the backfill scans, the checkpoint floor must hold at or below V {v:?}: {seen:?}"
	);
	assert_eq!(
		changes.first(),
		Some(&Ok(None)),
		"precondition: before the create no flow holds a floor: {changes:?}"
	);
	assert!(
		changes.iter().skip(1).all(|read| matches!(read, Ok(Some(_)))),
		"once held, the floor must never lapse before the checkpoint commits: {changes:?}"
	);

	db.command("INSERT bf::src [{ id: 99, g: 1, sym: 'z', v: 99 }]");
	settle(&db);
	let moved = await_value(true, TIMEOUT, || {
		checkpoint(&db, flow).is_some_and(|checkpoint| checkpoint > v && floor(&db) == Some(checkpoint))
	});
	assert!(
		moved,
		"after the backfill commits, the floor must follow the checkpoint past V {v:?}: floor {:?}, checkpoint {:?}",
		floor(&db),
		checkpoint(&db, flow)
	);
}

const MANAGED_KEYS: &str = "FROM system::metrics::flow::state::current FILTER { keyspace == 'CUSTOM_MANAGED' }";

const G_COLUMNS: &[OperatorColumn] = &[OperatorColumn {
	name: "g",
	type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
	description: "group key",
}];

const FOUR_GROUPS: [&str; 2] = [
	r#"INSERT bf::timed [{ id: 1, g: 1, ts: "2026-01-01T00:00:00Z" }, { id: 2, g: 2, ts: "2026-01-01T00:00:00Z" }, { id: 3, g: 3, ts: "2026-01-01T00:00:00Z" }]"#,
	r#"INSERT bf::timed [{ id: 4, g: 4, ts: "2026-01-01T00:00:02.999Z" }]"#,
];

const REWRITTEN_GROUP: [&str; 2] = [
	r#"INSERT bf::timed [{ id: 1, g: 1, ts: "2026-01-01T00:00:00Z" }, { id: 2, g: 2, ts: "2026-01-01T00:00:00Z" }]"#,
	r#"INSERT bf::timed [{ id: 3, g: 1, ts: "2026-01-01T00:00:02Z" }]"#,
];

struct Tally;

impl OperatorMetadata for Tally {
	const NAME: &'static str = "tally";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test-only managed operator that keeps one key per group and one in ROOT";
	const INPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl ManagedOperator for Tally {
	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(Tally)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext<Managed>, change: impl ChangeView) -> SdkResult<()> {
		// A ROOT key no group owns must survive every sweep, so the census floor is exactly one key.
		ctx.state().set(&managed_key_in(GroupId::ROOT, &[]).expect("an empty id fits the keyspace"), &1i64)?;
		for i in 0..change.diff_count() {
			let Some(diff) = change.diff(i) else {
				continue;
			};
			let Some(post) = diff.post() else {
				continue;
			};
			for r in 0..post.row_count() {
				let g = post
					.row(r)
					.expect("a post row")
					.i32("g")
					.expect("the g column reads")
					.expect("a g");
				let group = GroupId::of(&EncodedKey::new(g.to_be_bytes()));
				ctx.state().set(
					&managed_key_in(group, &[]).expect("an empty id fits the keyspace"),
					&1i64,
				)?;
			}
		}
		Ok(())
	}
}

struct ManagedTwins {
	early: Vec<OperatorId>,
	late: Vec<OperatorId>,
}

impl ManagedTwins {
	fn of(db: &TestDb) -> Self {
		Self {
			early: operators(db, flow_named(db, "early")),
			late: operators(db, flow_named(db, "late")),
		}
	}
}

fn managed_memory(batch: u16) -> TestDb {
	let db = TestDb::from(
		embedded::memory()
			.with_runtime_config(runtime())
			.with_config(ConfigKey::QueryRowBatchSize, Value::Uint2(batch))
			.with_config(ConfigKey::MetricsFlushInterval, Value::duration_milliseconds(10))
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.with_flow(|f| f.register_managed_operator::<Tally>())
			.build()
			.expect("build memory db with flow and the tally operator"),
	);
	timed(&db);
	db
}

fn managed_sqlite(path: &TempDbPath) -> TestDb {
	TestDb::from(
		embedded::sqlite(SqliteConfig::new(path))
			.with_runtime_config(runtime())
			.with_config(ConfigKey::QueryRowBatchSize, Value::Uint2(4))
			.with_config(ConfigKey::MetricsFlushInterval, Value::duration_milliseconds(10))
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.with_flow(|f| f.register_managed_operator::<Tally>())
			.build()
			.expect("open the sqlite db with flow and the tally operator"),
	)
}

fn timed(db: &TestDb) {
	db.admin("CREATE NAMESPACE bf");
	db.admin("CREATE TABLE bf::timed { id: int4, g: int4, ts: datetime } with { time: event(ts) }");
}

fn tally(name: &str) -> String {
	format!(
		"CREATE DEFERRED VIEW {name} {{ g: int4 }} AS {{ FROM bf::timed APPLY tally{{}} WITH {{ lateness: 2s }} }}"
	)
}

fn managed_keys(db: &TestDb, operators: &[OperatorId]) -> u64 {
	db.query(MANAGED_KEYS)
		.iter()
		.flat_map(|frame| frame.rows())
		.filter_map(|row| {
			let operator =
				row.get::<u64>("operator").expect("the operator column reads").expect("an operator id");
			operators
				.contains(&OperatorId(operator))
				.then(|| row.get::<u64>("keys").expect("the keys column reads").expect("a key count"))
		})
		.sum()
}

fn managed_twins(db: &TestDb, past: &[&str], fed_live: u64) -> ManagedTwins {
	db.admin(&tally("bf::early"));
	settle(db);
	for rql in past {
		db.command(rql);
	}
	settle(db);
	let early = operators(db, flow_named(db, "early"));
	let held = await_value(fed_live, TIMEOUT, || managed_keys(db, &early));
	assert_eq!(held, fed_live, "precondition: the early view must hold every group plus ROOT, fed live");
	db.admin(&tally("bf::late"));
	settle(db);
	ManagedTwins::of(db)
}

fn keys_agree(db: &TestDb, twins: &ManagedTwins, want: u64, step: &str) {
	let early = await_value(want, TIMEOUT, || managed_keys(db, &twins.early));
	assert_eq!(early, want, "precondition: the early view holds the wrong managed key count {step}");
	let late = await_value(early, TIMEOUT, || managed_keys(db, &twins.late));
	assert_eq!(late, early, "the late view's managed key count diverged from the early view's {step}");
}

fn advance(db: &TestDb, instant: &str) {
	db.admin(&format!("call storage::advance(bf::timed, cast('{instant}', datetime))"));
	settle(db);
}

fn backfilled_groups_are_freed_like_live_ones(batch: u16) {
	let db = managed_memory(batch);
	let twins = managed_twins(&db, &FOUR_GROUPS, 5);
	keys_agree(&db, &twins, 5, "at 2.999s, where no group is due yet");
	advance(&db, "2026-01-01T00:00:03Z");
	keys_agree(&db, &twins, 2, "at 3s, where groups 1 to 3 are due and group 4 and ROOT stay");
	advance(&db, "2026-01-01T00:00:05Z");
	keys_agree(&db, &twins, 1, "at 5s, where group 4 is due and ROOT is never freed");
}

#[test]
fn a_late_managed_view_frees_its_backfilled_groups_when_an_early_one_frees_them() {
	// Each backfilled row must arm reclaim from its own event time, or its group outlives the lateness.
	backfilled_groups_are_freed_like_live_ones(4);
}

#[test]
fn a_late_managed_view_built_one_row_per_chunk_frees_its_backfilled_groups_when_an_early_one_frees_them() {
	// Every chunk must arm reclaim for its own groups, not only the last chunk before the commit.
	backfilled_groups_are_freed_like_live_ones(1);
}

#[test]
fn a_late_managed_view_frees_a_group_rewritten_before_its_create_from_the_last_write() {
	// Group 2 must go at 3s from its own write and group 1 at 5s from its last one, whatever chunk carried them.
	let db = managed_memory(4);
	let twins = managed_twins(&db, &REWRITTEN_GROUP, 3);
	keys_agree(&db, &twins, 3, "at 2s, where no group is due yet");
	advance(&db, "2026-01-01T00:00:03Z");
	keys_agree(&db, &twins, 2, "at 3s, where group 2 is due and the rewritten group 1 stays with ROOT");
	advance(&db, "2026-01-01T00:00:05Z");
	keys_agree(&db, &twins, 1, "at 5s, where group 1 is due from its last write and ROOT is never freed");
}

#[test]
fn a_due_entry_armed_by_the_backfill_still_frees_its_group_after_a_restart() {
	// The backfill commit must store its due entries, otherwise a restart leaks every backfilled group.
	let path = TempDbPath::new("deferred_backfill_managed_restart");
	{
		let mut db = managed_sqlite(&path);
		timed(&db);
		let twins = managed_twins(&db, &FOUR_GROUPS, 5);
		keys_agree(&db, &twins, 5, "before the restart");
		assert!(
			poll_until(|| checkpoint(&db, flow_named(&db, "late")), TIMEOUT).is_some(),
			"precondition: the late backfill must commit its checkpoint before the stop"
		);
		db.stop();
	}
	let mut db = managed_sqlite(&path);
	let twins = ManagedTwins::of(&db);
	keys_agree(&db, &twins, 5, "after the restart, before the sweep can be judged");
	advance(&db, "2026-01-01T00:00:03Z");
	keys_agree(&db, &twins, 2, "at 3s after the restart, where groups 1 to 3 are due and group 4 and ROOT stay");
	advance(&db, "2026-01-01T00:00:05Z");
	keys_agree(&db, &twins, 1, "at 5s after the restart, where group 4 is due and ROOT is never freed");
	db.stop();
}
