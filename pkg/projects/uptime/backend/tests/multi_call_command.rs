// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{
	Database, Error, IdentityId, Value, WithSubsystem, server,
	value::{
		params::Params,
		value::{duration::Duration, frame::frame::Frame, into::IntoValue, uuid::Uuid7},
	},
};
use reifydb_uptime::migration_path;

const CREATE_WITH_TWO_REGIONS: &str = "CALL uptime::create_monitor($id, $name, $kind, $target, $interval, $timeout, \
	 $http_method, $expected_status, $keyword, $expected_ip, $failure_threshold, $enabled); \
	 CALL uptime::add_monitor_region($id, $region_a); \
	 CALL uptime::add_monitor_region($id, $region_b)";

fn build() -> Database {
	server::memory().with_flow(|f| f).with_migrations(migration_path()).build().expect("build memory db")
}

fn admin(db: &Database, rql: &str) {
	let r = db.engine().admin_as(IdentityId::root(), rql, Params::None);
	if let Some(e) = r.error {
		panic!("admin failed for [{rql}]: {e:?}");
	}
}

fn command_as(db: &Database, id: IdentityId, rql: &str, params: Params) -> Result<Vec<Frame>, Error> {
	let r = db.engine().command_as(id, rql, params);
	match r.error {
		Some(e) => Err(e),
		None => Ok(r.frames),
	}
}

fn root_query(db: &Database, rql: &str, params: Params) -> Vec<Frame> {
	let r = db.engine().query_as(IdentityId::root(), rql, params);
	if let Some(e) = r.error {
		panic!("root query failed for [{rql}]: {e:?}");
	}
	r.frames
}

fn params(entries: &[(&str, Value)]) -> Params {
	let map: HashMap<String, Value> = entries.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect();
	Params::from(map)
}

fn rows(frames: &[Frame]) -> usize {
	frames.first().map(Frame::row_count).unwrap_or(0)
}

fn column(frames: &[Frame], name: &str) -> Value {
	let frame = frames.first().expect("frame");
	assert_eq!(frame.row_count(), 1, "expected exactly one row when reading column {name}");
	frame.columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("column {name}")).data.get_value(0)
}

fn new_user(db: &Database, name: &str) -> IdentityId {
	admin(db, &format!("CREATE USER {name}"));
	let frames = root_query(
		db,
		"from system::identities filter { name == $name } map { id }",
		params(&[("name", Value::Utf8(name.to_string()))]),
	);
	match column(&frames, "id") {
		Value::IdentityId(id) => id,
		other => panic!("unexpected identity value for {name}: {other:?}"),
	}
}

fn new_id(db: &Database) -> Uuid7 {
	Uuid7::generate(db.clock(), db.engine().rng())
}

fn region(db: &Database, label: &str) -> Uuid7 {
	let frames = root_query(
		db,
		"from uptime::regions filter { label == $label } map { id }",
		params(&[("label", Value::Utf8(label.to_string()))]),
	);
	match column(&frames, "id") {
		Value::Uuid7(id) => id,
		other => panic!("unexpected region id for {label}: {other:?}"),
	}
}

fn create_params(id: Uuid7, region_a: Uuid7, region_b: Uuid7) -> Params {
	params(&[
		("id", id.into_value()),
		("name", Value::Utf8("Home page".to_string())),
		("kind", Value::Utf8("http".to_string())),
		("target", Value::Utf8("https://example.com".to_string())),
		("interval", Duration::from_seconds(60).unwrap().into_value()),
		("timeout", Duration::from_seconds(10).unwrap().into_value()),
		("http_method", Value::Utf8("GET".to_string())),
		("expected_status", Value::Int2(200)),
		("keyword", Value::none()),
		("expected_ip", Value::none()),
		("failure_threshold", Value::Int2(3)),
		("enabled", Value::Boolean(true)),
		("region_a", region_a.into_value()),
		("region_b", region_b.into_value()),
	])
}

fn monitor_count(db: &Database, id: Uuid7) -> usize {
	rows(&root_query(
		db,
		"from uptime::monitors filter { id == $id } map { id }",
		params(&[("id", id.into_value())]),
	))
}

fn region_count(db: &Database, id: Uuid7) -> usize {
	rows(&root_query(
		db,
		"from uptime::monitor_regions filter { monitor_id == $id } map { region_id }",
		params(&[("id", id.into_value())]),
	))
}

#[test]
fn several_calls_in_one_command_commit_together() {
	// The web form creates a monitor and its regions in one round trip; all rows must land.
	let db = build();
	let alice = new_user(&db, "alice");
	let id = new_id(&db);

	command_as(
		&db,
		alice,
		CREATE_WITH_TWO_REGIONS,
		create_params(id, region(&db, "US East"), region(&db, "EU West")),
	)
	.expect("multi-call command");

	assert_eq!(monitor_count(&db, id), 1, "the monitor must be committed");
	assert_eq!(region_count(&db, id), 2, "both regions must be committed");
}

#[test]
fn a_failing_later_call_rolls_back_the_earlier_calls() {
	// Without one transaction per command a bad region would leave a monitor that no probe ever checks.
	let db = build();
	let alice = new_user(&db, "alice");
	let id = new_id(&db);

	let result =
		command_as(&db, alice, CREATE_WITH_TWO_REGIONS, create_params(id, region(&db, "US East"), new_id(&db)));

	match result {
		Ok(frames) => panic!("the unknown region must fail the command, got {} frame(s)", frames.len()),
		Err(e) => {
			assert_eq!(
				e.0.code, "ASSERT",
				"the failing call's own error must surface, got {:?}",
				e.0.message
			);
			assert!(e.0.message.contains("unknown region"), "unexpected message {:?}", e.0.message);
		}
	}
	assert_eq!(monitor_count(&db, id), 0, "the monitor from the first call must be rolled back");
	assert_eq!(region_count(&db, id), 0, "the region from the second call must be rolled back");
}

#[test]
fn a_denied_later_call_rolls_back_the_earlier_calls() {
	// A policy denial part-way through must undo the earlier calls exactly like a failed check does.
	let db = build();
	let alice = new_user(&db, "alice");
	let id = new_id(&db);
	let rql = "CALL uptime::create_monitor($id, $name, $kind, $target, $interval, $timeout, \
		 $http_method, $expected_status, $keyword, $expected_ip, $failure_threshold, $enabled); \
		 CALL uptime::add_monitor_region($id, $region_a); \
		 CALL uptime::find_monitor($id)";

	let result = command_as(&db, alice, rql, create_params(id, region(&db, "US East"), region(&db, "EU West")));

	match result {
		Ok(frames) => {
			panic!("find_monitor is service-only and must fail the command, got {} frame(s)", frames.len())
		}
		Err(e) => assert_eq!(e.0.code, "POLICY_001", "unexpected error {:?}", e.0.message),
	}
	assert_eq!(monitor_count(&db, id), 0, "the monitor must be rolled back");
	assert_eq!(region_count(&db, id), 0, "the region must be rolled back");
}
