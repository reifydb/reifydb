// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb::{
	SqliteConfig, WithSubsystem, embedded,
	testing::db::{TempDbPath, TestDb, await_value, poll_until},
};
use reifydb_cdc::lift::changed_objects;
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::{CommitVersion, WindowRequirements, WindowSizeDomain},
	interface::{
		catalog::{
			flow::{FlowId, OperatorId},
			id::{TableId, ViewId},
			object::ObjectId,
		},
		flow::OperatorCapability,
	},
	key::operator::state::GroupId,
	operator_with::ApplyWith,
};
use reifydb_sdk::{
	error::Result as SdkResult,
	flow::operator::{
		OperatorMetadata, UnmanagedOperator,
		column::operator::OperatorColumn,
		context::{GuestContext, Unmanaged},
		view::{ChangeView, ColumnsView, DiffView, RowView},
	},
	row,
};
use reifydb_store_cdc::storage::CdcStorage;
use reifydb_value::{
	config::ExtensionParams,
	value::{
		constraint::TypeConstraint, diff_type::DiffType, duration::Duration, frame::frame::Frame,
		value_type::ValueType,
	},
};

const TIMEOUT: Duration = Duration::from_seconds_const(10);

const CHAIN: [&str; 3] = [
	"CREATE DEFERRED VIEW fu::a { id: int4, g: int4, v: int4 } AS { FROM fu::src | filter { v > 10 } }",
	"CREATE DEFERRED VIEW fu::b { g: int4, n: int8, total: int4 } AS { FROM fu::a | aggregate { n: math::count(id), total: math::sum(v) } by { g } }",
	"CREATE DEFERRED VIEW fu::c { g: int4, n: int8, total: int4 } AS { FROM fu::b | filter { total < 1000 } }",
];

fn open(path: &TempDbPath, flow: bool) -> TestDb {
	let mut builder = embedded::sqlite(SqliteConfig::new(path));
	if flow {
		builder = builder.with_flow(|f| f);
	}
	TestDb::from(builder.build().expect("open the sqlite db"))
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

fn view_object(db: &TestDb, name: &str) -> ObjectId {
	ObjectId::View(ViewId(catalog_id(db, "views", name)))
}

fn table_object(db: &TestDb, name: &str) -> ObjectId {
	ObjectId::Table(TableId(catalog_id(db, "tables", name)))
}

fn checkpoint(db: &TestDb, view: &str) -> Option<CommitVersion> {
	let flow = FlowId(catalog_id(db, "flows", view));
	db.engine().operator_state().checkpoint_get(flow).expect("the checkpoint lookup")
}

fn commits(db: &TestDb, object: ObjectId) -> Vec<CommitVersion> {
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
	batch.items.iter().filter(|cdc| changed_objects(cdc).contains(&object)).map(|cdc| cdc.version.commit).collect()
}

fn groups(frames: &[Frame]) -> Vec<(i32, i64, i32)> {
	let mut groups: Vec<(i32, i64, i32)> = frames
		.iter()
		.flat_map(|frame| frame.rows())
		.map(|row| {
			(
				row.get::<i32>("g").unwrap().unwrap(),
				row.get::<i64>("n").unwrap().unwrap(),
				row.get::<i32>("total").unwrap().unwrap(),
			)
		})
		.collect();
	groups.sort();
	groups
}

fn await_groups(db: &TestDb, view: &str, want: &[(i32, i64, i32)], step: &str) {
	let got = await_value(want.to_vec(), TIMEOUT, || groups(&db.query(&format!("FROM {view}"))));
	assert_eq!(got, want, "{view} is wrong {step}");
}

fn fresh_chain(tag: &str) -> TempDbPath {
	// Creating the chain with no flow running makes the next life bootstrap every member from a backfill.
	let path = TempDbPath::new(tag);
	let mut db = open(&path, false);
	db.admin("CREATE NAMESPACE fu");
	db.admin("CREATE TABLE fu::src { id: int4, g: int4, v: int4 }");
	db.command("INSERT fu::src [{ id: 1, g: 1, v: 5 }, { id: 2, g: 1, v: 20 }, { id: 3, g: 2, v: 30 }]");
	for create in CHAIN {
		db.admin(create);
	}
	stop_without_flow(&mut db);
	path
}

fn follows_the_source(db: &TestDb, step: &str) {
	for (rql, want) in [
		("INSERT fu::src [{ id: 4, g: 2, v: 40 }]", vec![(1, 1, 20), (2, 2, 70)]),
		("UPDATE fu::src { v: 50 } FILTER { id == 1 }", vec![(1, 2, 70), (2, 2, 70)]),
		("DELETE fu::src FILTER { id == 3 }", vec![(1, 2, 70), (2, 1, 40)]),
		("UPDATE fu::src { v: 2000 } FILTER { id == 4 }", vec![(1, 2, 70)]),
	] {
		db.command(rql);
		settle(db);
		await_groups(db, "fu::c", &want, &format!("{step} after: {rql}"));
	}
}

#[test]
fn a_fresh_database_backfills_a_unit_as_one() {
	// Unfused, every member backfills in its own commit; a unit must write the whole chain in one.
	let path = fresh_chain("fused_chain_backfill");
	let mut db = open(&path, true);
	settle(&db);
	await_groups(&db, "fu::c", &[(1, 1, 20), (2, 1, 30)], "after the bootstrap backfill");
	let head = checkpoint(&db, "a");
	assert!(head.is_some(), "the head must hold a checkpoint after its backfill");
	for member in ["b", "c"] {
		assert_eq!(checkpoint(&db, member), head, "member {member} must share the head's checkpoint");
	}
	let backfills: Vec<Vec<CommitVersion>> =
		["a", "b", "c"].iter().map(|view| commits(&db, view_object(&db, view))).collect();
	assert_eq!(backfills[0].len(), 1, "the head must write its backfill in one commit: {backfills:?}");
	assert!(
		backfills.iter().all(|commits| commits == &backfills[0]),
		"every member must backfill in the head's commit: {backfills:?}"
	);
	db.stop();
}

fn units(db: &TestDb) -> Vec<(u64, u64)> {
	let mut units: Vec<(u64, u64)> = db
		.query("FROM system::flow::units")
		.iter()
		.flat_map(|frame| frame.rows())
		.map(|row| {
			let flow = row.get::<u64>("flow_id").expect("the flow_id column reads").expect("a flow id");
			let unit = row.get::<u64>("unit").expect("the unit column reads").expect("a unit");
			(flow, unit)
		})
		.collect();
	units.sort_unstable();
	units
}

fn started_units(db: &TestDb) -> Vec<(u64, u64)> {
	// a reopened engine registers its fused units after build returns, so an immediate read can see none yet.
	poll_until(
		|| {
			let units = units(db);
			(!units.is_empty()).then_some(units)
		},
		TIMEOUT,
	)
	.expect("the reopened engine never registered its flow units")
}

#[test]
fn the_flow_units_table_lists_every_member_under_its_head_and_no_unfused_flow() {
	// A table that misses a member or lists a lone flow reports fusion that did not run.
	let path = TempDbPath::new("fused_chain_units_table");
	{
		let mut db = open(&path, false);
		db.admin("CREATE NAMESPACE fu");
		db.admin("CREATE TABLE fu::src { id: int4, g: int4, v: int4 }");
		for create in CHAIN {
			db.admin(create);
		}
		db.admin(
			"CREATE DEFERRED VIEW fu::side { id: int4, g: int4, v: int4 } AS { FROM fu::src | filter { v > 100 } }",
		);
		stop_without_flow(&mut db);
	}
	let mut db = open(&path, true);
	settle(&db);
	let head = catalog_id(&db, "flows", "a");
	let mut want: Vec<(u64, u64)> =
		["a", "b", "c"].iter().map(|flow| (catalog_id(&db, "flows", flow), head)).collect();
	want.sort_unstable();
	assert_eq!(units(&db), want, "the unit table must list a, b and c under a, and never side");
	db.stop();
}

struct SymIdRow {
	id: i32,
	sym: i64,
}

row!(SymIdRow {
	id: i32,
	sym: i64
});

struct SymIds;

impl OperatorMetadata for SymIds {
	const NAME: &'static str = "sym_ids";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test-only operator that reads sym as a dictionary id";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[
		OperatorColumn {
			name: "id",
			type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
			description: "row id",
		},
		OperatorColumn {
			name: "sym",
			type_constraint: TypeConstraint::unconstrained(ValueType::DictionaryId),
			description: "the symbol as its dictionary entry id",
		},
	];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[
		OperatorColumn {
			name: "id",
			type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
			description: "row id",
		},
		OperatorColumn {
			name: "sym",
			type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
			description: "the dictionary entry id read from the input",
		},
	];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl UnmanagedOperator for SymIds {
	const UNMANAGED_BECAUSE: &'static str = "test operator";
	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: false,
		kinds: &[],
		domain: WindowSizeDomain::Time,
		needs_pane: false,
	};

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(SymIds)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext<Unmanaged>, change: impl ChangeView) -> SdkResult<()> {
		// Reading the id is the read that fails when the input carries the symbol text instead of its entry id.
		for i in 0..change.diff_count() {
			let Some(diff) = change.diff(i) else {
				continue;
			};
			if !matches!(diff.kind(), DiffType::Insert) {
				continue;
			}
			let Some(post) = diff.post() else {
				continue;
			};
			for r in 0..post.row_count() {
				let row = post.row(r).expect("row");
				let id = row.i32("id")?.expect("id");
				let sym = row.dictionary_id("sym")?.expect("a sym entry id").to_u128() as i64;
				let key = EncodedKey::new(id.to_be_bytes());
				let (row_number, _) =
					ctx.get_or_create_row_numbers(GroupId::of(&key), &[key])?.remove(0);
				ctx.emit_insert(
					&[SymIdRow {
						id,
						sym,
					}],
					&[row_number],
				)?;
			}
		}
		Ok(())
	}
}

fn open_with_sym_ids(path: &TempDbPath) -> TestDb {
	TestDb::from(
		embedded::sqlite(SqliteConfig::new(path))
			.with_flow(|f| f.register_unmanaged_operator::<SymIds>())
			.build()
			.expect("open the sqlite db"),
	)
}

fn sym_ids(db: &TestDb) -> Vec<(i32, i64)> {
	let mut rows: Vec<(i32, i64)> = db
		.query("FROM fu::b")
		.iter()
		.flat_map(|frame| frame.rows())
		.map(|row| (row.get::<i32>("id").unwrap().unwrap(), row.get::<i64>("sym").unwrap().unwrap()))
		.collect();
	rows.sort();
	rows
}

#[test]
fn a_fused_child_reads_a_dictionary_column_its_parent_got_as_text_as_entry_ids() {
	// A fused child fed the parent's text where a cdc child gets entry ids fails every id read of that column.
	let path = TempDbPath::new("fused_chain_dictionary_ids");
	{
		let mut db = open_with_sym_ids(&path);
		db.admin("CREATE NAMESPACE fu");
		db.admin("CREATE DICTIONARY fu::syms FOR utf8 AS uint4");
		db.admin("CREATE TABLE fu::src { id: int4, sym: utf8 }");
		db.admin(
			"CREATE DEFERRED VIEW fu::a { id: int4, sym: utf8 with { dictionary: fu::syms } } AS { FROM fu::src }",
		);
		db.admin("CREATE DEFERRED VIEW fu::b { id: int4, sym: int8 } AS { FROM fu::a | APPLY sym_ids{} }");
		db.command("INSERT fu::src [{ id: 1, sym: 'red' }, { id: 2, sym: 'green' }]");
		settle(&db);
		db.stop();
	}
	let mut db = open_with_sym_ids(&path);
	let head = catalog_id(&db, "flows", "a");
	assert!(
		units(&db).contains(&(catalog_id(&db, "flows", "b"), head)),
		"b must run fused under a for this test to mean anything: {:?}",
		units(&db)
	);
	settle(&db);
	db.command("INSERT fu::src [{ id: 3, sym: 'red' }]");
	settle(&db);
	let rows = sym_ids(&db);
	assert_eq!(rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 2, 3], "every row reaches b: {rows:?}");
	assert_eq!(rows[0].1, rows[2].1, "both red rows must carry one entry id: {rows:?}");
	assert_ne!(rows[0].1, rows[1].1, "red and green must carry different entry ids: {rows:?}");
	db.stop();
}

#[test]
fn a_fused_child_of_a_series_view_reads_its_dictionary_column_as_entry_ids() {
	// A series sink emits its own coerced batch, so a child fed it without the id lookup fails every id read.
	let path = TempDbPath::new("fused_chain_series_dictionary_ids");
	{
		let mut db = open_with_sym_ids(&path);
		db.admin("CREATE NAMESPACE fu");
		db.admin("CREATE DICTIONARY fu::syms FOR utf8 AS uint4");
		db.admin("CREATE TABLE fu::src { id: int4, sym: utf8 }");
		db.admin(
			"CREATE DEFERRED SERIES VIEW fu::a { id: int4, sym: utf8 with { dictionary: fu::syms } } WITH { key: id } AS { FROM fu::src }",
		);
		db.admin("CREATE DEFERRED VIEW fu::b { id: int4, sym: int8 } AS { FROM fu::a | APPLY sym_ids{} }");
		db.command("INSERT fu::src [{ id: 1, sym: 'red' }, { id: 2, sym: 'green' }]");
		settle(&db);
		db.stop();
	}
	let mut db = open_with_sym_ids(&path);
	let head = catalog_id(&db, "flows", "a");
	let started = started_units(&db);
	assert!(
		started.contains(&(catalog_id(&db, "flows", "b"), head)),
		"b must run fused under the series view a for this test to mean anything: {started:?}"
	);
	settle(&db);
	db.command("INSERT fu::src [{ id: 3, sym: 'red' }]");
	settle(&db);
	let rows = sym_ids(&db);
	assert_eq!(rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 2, 3], "every row reaches b: {rows:?}");
	assert_eq!(rows[0].1, rows[2].1, "both red rows must carry one entry id: {rows:?}");
	assert_ne!(rows[0].1, rows[1].1, "red and green must carry different entry ids: {rows:?}");
	db.stop();
}

fn ids(db: &TestDb, flows: &[&str]) -> Vec<u64> {
	flows.iter().map(|flow| catalog_id(db, "flows", flow)).collect()
}

fn under(db: &TestDb, head: &str, flows: &[&str]) -> Vec<(u64, u64)> {
	let head = catalog_id(db, "flows", head);
	let mut want: Vec<(u64, u64)> = ids(db, flows).into_iter().map(|flow| (flow, head)).collect();
	want.sort_unstable();
	want
}

#[test]
fn dropping_a_fused_member_leaves_its_siblings_running() {
	// A unit that keeps running a dropped member writes into a view that no longer exists and stalls its siblings.
	let path = fresh_chain("fused_chain_drop_member");
	let mut db = open(&path, true);
	settle(&db);
	assert_eq!(units(&db), under(&db, "a", &["a", "b", "c"]), "the chain must start fused");
	let (a, b) = (ids(&db, &["a"])[0], ids(&db, &["b"])[0]);

	db.admin("DROP VIEW fu::c");
	let mut want = vec![(a, a), (b, a)];
	want.sort_unstable();
	let got = await_value(want.clone(), TIMEOUT, || units(&db));
	assert_eq!(got, want, "the re-formed unit must list a and b under a, and no longer c");

	db.command("INSERT fu::src [{ id: 4, g: 2, v: 40 }]");
	settle(&db);
	await_groups(&db, "fu::b", &[(1, 1, 20), (2, 2, 70)], "after a write that follows the drop");
	db.stop();
}

#[test]
fn dropping_every_member_of_a_unit_leaves_its_head_running_alone() {
	// A head left with no members must run as a plain flow, or the unit table keeps reporting fusion that is gone.
	let path = fresh_chain("fused_chain_drop_all_members");
	let mut db = open(&path, true);
	settle(&db);
	assert_eq!(units(&db), under(&db, "a", &["a", "b", "c"]), "the chain must start fused");

	db.admin("DROP VIEW fu::c");
	let a = ids(&db, &["a"])[0];
	let b = ids(&db, &["b"])[0];
	await_value(vec![(a, a), (b, a)], TIMEOUT, || units(&db));
	db.admin("DROP VIEW fu::b");
	let got = await_value(Vec::new(), TIMEOUT, || units(&db));
	assert_eq!(got, Vec::new(), "a lone head is no unit, so the table must list nothing");

	db.command("INSERT fu::src [{ id: 4, g: 2, v: 40 }]");
	settle(&db);
	db.await_row_count("FROM fu::a", 3, TIMEOUT);
	db.stop();
}

#[test]
fn a_view_created_at_runtime_joins_its_parents_unit() {
	// A view created at runtime that never joins keeps a commit per hop until the next restart.
	let path = fresh_chain("fused_chain_runtime_join");
	let mut db = open(&path, true);
	settle(&db);
	db.admin("CREATE DEFERRED VIEW fu::d { g: int4, n: int8, total: int4 } AS { FROM fu::c | filter { n > 0 } }");
	settle(&db);
	await_groups(&db, "fu::d", &[(1, 1, 20), (2, 1, 30)], "after its backfill");
	let want = under(&db, "a", &["a", "b", "c", "d"]);
	let got = await_value(want.clone(), TIMEOUT, || units(&db));
	assert_eq!(got, want, "d must join a's unit without a restart");

	db.command("INSERT fu::src [{ id: 4, g: 2, v: 40 }]");
	settle(&db);
	await_groups(&db, "fu::d", &[(1, 1, 20), (2, 2, 70)], "after a write that follows the join");
	let head = commits(&db, view_object(&db, "a"));
	let joined = commits(&db, view_object(&db, "d"));
	assert_eq!(
		head.last(),
		joined.last(),
		"the joined member must land in the head's commit: a {head:?}, d {joined:?}"
	);
	db.stop();
}

#[test]
fn a_view_created_at_runtime_on_a_middle_member_reads_it_and_the_unit_splits_at_the_next_restart() {
	// A middle member read by a new flow must still publish its view, or the new reader sees nothing.
	let path = fresh_chain("fused_chain_middle_reader");
	{
		let mut db = open(&path, true);
		settle(&db);
		assert_eq!(units(&db), under(&db, "a", &["a", "b", "c"]), "the chain must start fused");
		db.admin(
			"CREATE DEFERRED VIEW fu::side { g: int4, n: int8, total: int4 } AS { FROM fu::b | filter { n > 0 } }",
		);
		settle(&db);
		await_groups(&db, "fu::side", &[(1, 1, 20), (2, 1, 30)], "after its backfill");
		db.command("INSERT fu::src [{ id: 4, g: 2, v: 40 }]");
		settle(&db);
		await_groups(&db, "fu::side", &[(1, 1, 20), (2, 2, 70)], "after a write through the running unit");
		await_groups(&db, "fu::c", &[(1, 1, 20), (2, 2, 70)], "after a write through the running unit");
		db.stop();
	}
	let mut db = open(&path, true);
	assert_eq!(
		started_units(&db),
		under(&db, "a", &["a", "b"]),
		"b now has two readers, so only a and b may fuse at the next start"
	);
	settle(&db);
	db.command("INSERT fu::src [{ id: 5, g: 1, v: 30 }]");
	settle(&db);
	await_groups(&db, "fu::side", &[(1, 2, 50), (2, 2, 70)], "after the restart and a write");
	await_groups(&db, "fu::c", &[(1, 2, 50), (2, 2, 70)], "after the restart and a write");
	db.stop();
}

#[test]
fn a_deferred_chain_follows_the_source_when_fused() {
	// A fused child that is also routed its parent's change stream counts every source row twice.
	let path = fresh_chain("fused_chain_follows");
	{
		let mut db = open(&path, true);
		settle(&db);
		follows_the_source(&db, "in the first fused life");
		db.stop();
	}
	let mut db = open(&path, true);
	settle(&db);
	await_groups(&db, "fu::c", &[(1, 2, 70)], "after a restart that resumes the unit");
	db.command("INSERT fu::src [{ id: 5, g: 3, v: 11 }]");
	settle(&db);
	await_groups(&db, "fu::c", &[(1, 2, 70), (3, 1, 11)], "after a restart and a new write");
	db.stop();
}

#[test]
fn a_chain_of_three_deferred_views_commits_once_per_source_version() {
	// Unfused, every hop commits at its own version; fused, one source write lands in exactly one commit.
	let path = fresh_chain("fused_chain_one_commit");
	let mut db = open(&path, true);
	settle(&db);
	let views = ["a", "b", "c"].map(|view| view_object(&db, view));
	let before = views.map(|view| commits(&db, view).len());
	db.command("INSERT fu::src [{ id: 4, g: 2, v: 40 }]");
	settle(&db);
	await_groups(&db, "fu::c", &[(1, 1, 20), (2, 2, 70)], "after the write");
	let fresh: Vec<Vec<CommitVersion>> =
		views.iter().zip(before).map(|(view, before)| commits(&db, *view)[before..].to_vec()).collect();
	assert_eq!(fresh[0].len(), 1, "the head must commit the write once: {fresh:?}");
	assert!(
		fresh.iter().all(|commits| commits == &fresh[0]),
		"every member must land in the head's commit: {fresh:?}"
	);
	assert!(
		commits(&db, table_object(&db, "src")).iter().all(|source| !fresh[0].contains(source)),
		"the unit commit must be its own commit, not the source write"
	);
	db.stop();
}

#[test]
fn dictionary_columns_reach_a_fused_child_as_they_reach_a_cdc_child() {
	// A cdc child sees the parent's dictionary ids decoded to text; a fused child fed ids would filter and show
	// ids.
	let path = TempDbPath::new("fused_chain_dictionary");
	{
		let mut db = open(&path, false);
		db.admin("CREATE NAMESPACE fu");
		db.admin("CREATE DICTIONARY fu::syms FOR utf8 AS uint4");
		db.admin("CREATE TABLE fu::src { id: int4, sym: utf8 with { dictionary: fu::syms } }");
		db.admin(
			"CREATE DEFERRED VIEW fu::a { id: int4, sym: utf8 with { dictionary: fu::syms } } AS { FROM fu::src }",
		);
		db.admin(
			"CREATE DEFERRED VIEW fu::b { id: int4, sym: utf8 } AS { FROM fu::a | filter { sym != 'green' } }",
		);
		db.command("INSERT fu::src [{ id: 1, sym: 'red' }, { id: 2, sym: 'green' }]");
		stop_without_flow(&mut db);
	}
	let mut db = open(&path, true);
	let syms = || -> Vec<(i32, String)> {
		let mut syms: Vec<(i32, String)> = db
			.query("FROM fu::b")
			.iter()
			.flat_map(|frame| frame.rows())
			.map(|row| (row.get::<i32>("id").unwrap().unwrap(), row.get::<String>("sym").unwrap().unwrap()))
			.collect();
		syms.sort();
		syms
	};
	settle(&db);
	for (rql, want) in [
		("INSERT fu::src [{ id: 3, sym: 'blue' }]", vec![(1, "red"), (3, "blue")]),
		("UPDATE fu::src { sym: 'green' } FILTER { id == 1 }", vec![(3, "blue")]),
		("UPDATE fu::src { sym: 'red' } FILTER { id == 2 }", vec![(2, "red"), (3, "blue")]),
		("DELETE fu::src FILTER { id == 3 }", vec![(2, "red")]),
	] {
		db.command(rql);
		settle(&db);
		let want: Vec<(i32, String)> = want.into_iter().map(|(id, sym)| (id, sym.to_string())).collect();
		assert_eq!(await_value(want.clone(), TIMEOUT, syms), want, "fu::b after: {rql}");
	}
	let (a, b) = (view_object(&db, "a"), view_object(&db, "b"));
	assert_eq!(commits(&db, a), commits(&db, b), "precondition: fu::b must run fused with fu::a");
	db.stop();
}

#[test]
fn a_chain_built_unfused_fuses_on_the_next_restart() {
	// A member caught up with its parent must join the unit, or a database built before fusion never fuses.
	let path = TempDbPath::new("fused_chain_upgrade");
	{
		let mut db = open(&path, true);
		db.admin("CREATE NAMESPACE fu");
		db.admin("CREATE TABLE fu::src { id: int4, g: int4, v: int4 }");
		for create in CHAIN {
			db.admin(create);
		}
		db.command("INSERT fu::src [{ id: 1, g: 1, v: 5 }, { id: 2, g: 1, v: 20 }, { id: 3, g: 2, v: 30 }]");
		settle(&db);
		await_groups(&db, "fu::c", &[(1, 1, 20), (2, 1, 30)], "in the unfused first life");
		db.stop();
	}
	{
		// A reader's checkpoint counts source versions, so a member above the head's has applied all the parent
		// wrote.
		let mut db = open(&path, false);
		let head = checkpoint(&db, "a").expect("precondition: the head must hold a checkpoint");
		for member in ["b", "c"] {
			let flow = FlowId(catalog_id(&db, "flows", member));
			db.engine()
				.operator_state()
				.checkpoint_set(flow, CommitVersion(head.0 + 1))
				.expect("raise the checkpoint");
		}
		stop_without_flow(&mut db);
	}
	let mut db = open(&path, true);
	settle(&db);
	let views = ["a", "b", "c"].map(|view| view_object(&db, view));
	let before = views.map(|view| commits(&db, view).len());
	db.command("INSERT fu::src [{ id: 4, g: 2, v: 40 }]");
	settle(&db);
	await_groups(&db, "fu::c", &[(1, 1, 20), (2, 2, 70)], "after the restart and a write");
	let fresh: Vec<Vec<CommitVersion>> =
		views.iter().zip(before).map(|(view, before)| commits(&db, *view)[before..].to_vec()).collect();
	assert_eq!(fresh[0].len(), 1, "the head must commit the write once: {fresh:?}");
	assert!(
		fresh.iter().all(|commits| commits == &fresh[0]),
		"every member must land in the head's commit: {fresh:?}"
	);
	db.stop();
}

#[test]
fn a_member_behind_its_head_restarts_unfused_and_catches_up() {
	// A member fused while behind its head is fed only what the head emits from then on and never gets the gap.
	let path = TempDbPath::new("fused_chain_member_behind");
	{
		let mut db = open(&path, true);
		db.admin("CREATE NAMESPACE fu");
		db.admin("CREATE TABLE fu::src { id: int4, g: int4, v: int4 }");
		db.admin(CHAIN[0]);
		db.command("INSERT fu::src [{ id: 1, g: 1, v: 5 }, { id: 2, g: 1, v: 20 }, { id: 3, g: 2, v: 30 }]");
		settle(&db);
		assert!(checkpoint(&db, "a").is_some(), "precondition: the head must hold a checkpoint");
		db.stop();
	}
	{
		let mut db = open(&path, false);
		db.admin(CHAIN[1]);
		assert_eq!(
			checkpoint(&db, "b"),
			None,
			"precondition: b created with no flow running holds no checkpoint"
		);
		stop_without_flow(&mut db);
	}
	let mut db = open(&path, true);
	let b = catalog_id(&db, "flows", "b");
	assert!(
		!units(&db).iter().any(|(flow, _)| *flow == b),
		"b starts behind a, so it must not be fused at bootstrap: {:?}",
		units(&db)
	);
	settle(&db);
	await_groups(&db, "fu::b", &[(1, 1, 20), (2, 1, 30)], "after b caught up on its own");
	db.command("INSERT fu::src [{ id: 4, g: 2, v: 40 }]");
	settle(&db);
	await_groups(&db, "fu::b", &[(1, 1, 20), (2, 2, 70)], "after a write that follows the catch-up");
	db.stop();
}
