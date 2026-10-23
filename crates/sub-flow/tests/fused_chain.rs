// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb::{
	SqliteConfig, WithSubsystem, embedded,
	testing::db::{TempDbPath, TestDb, await_value, poll_until},
};
use reifydb_cdc::lift::changed_objects;
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		flow::FlowId,
		id::{TableId, ViewId},
		object::ObjectId,
	},
};
use reifydb_store_cdc::storage::CdcStorage;
use reifydb_value::value::{duration::Duration, frame::frame::Frame};

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
