// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{
	Database, IdentityId, Value, WithSubsystem, server,
	value::{
		params::Params,
		value::{duration::Duration, frame::frame::Frame, into::IntoValue, uuid::Uuid7},
	},
};
use reifydb_uptime::migration_path;

fn build() -> Database {
	server::memory().with_flow(|f| f).with_migrations(migration_path()).build().expect("build memory db")
}

fn params(entries: &[(&str, Value)]) -> Params {
	let map: HashMap<String, Value> = entries.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect();
	Params::from(map)
}

fn command(db: &Database, id: IdentityId, rql: &str, params: Params) {
	let r = db.engine().command_as(id, rql, params);
	if let Some(e) = r.error {
		panic!("command failed for [{rql}]: {e:?}");
	}
}

fn query_rows(db: &Database, id: IdentityId, rql: &str) -> usize {
	let r = db.engine().query_as(id, rql, Params::None);
	if let Some(e) = r.error {
		panic!("query failed for [{rql}]: {e:?}");
	}
	r.frames.first().map(Frame::row_count).unwrap_or(0)
}

fn new_user(db: &Database, name: &str) -> IdentityId {
	let r = db.engine().admin_as(IdentityId::root(), &format!("CREATE USER {name}"), Params::None);
	if let Some(e) = r.error {
		panic!("create user failed: {e:?}");
	}
	let r = db.engine().query_as(
		IdentityId::root(),
		"from system::identities filter { name == $name } map { id }",
		params(&[("name", Value::Utf8(name.to_string()))]),
	);
	match r.frames.first().expect("identity frame").columns[0].data.get_value(0) {
		Value::IdentityId(id) => id,
		other => panic!("unexpected identity value for {name}: {other:?}"),
	}
}

#[test]
fn a_let_bound_read_does_not_leak_another_owners_rows() {
	// A from-policy that guards only the top-level pipeline leaks every row through a let binding.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let monitor = Uuid7::generate(db.clock(), db.engine().rng());
	let page = Uuid7::generate(db.clock(), db.engine().rng());
	command(
		&db,
		alice,
		"CALL uptime::create_monitor($m, $name, $kind, $target, $interval, $timeout, none, none, none, none, 1, true); \
		 CALL uptime::create_status_page($p, $slug, $title); \
		 CALL uptime::add_status_page_monitors($p, $monitor_ids)",
		params(&[
			("m", monitor.into_value()),
			("name", Value::Utf8("m".to_string())),
			("kind", Value::Utf8("http".to_string())),
			("target", Value::Utf8("https://example.com".to_string())),
			("interval", Duration::from_seconds(60).unwrap().into_value()),
			("timeout", Duration::from_seconds(10).unwrap().into_value()),
			("p", page.into_value()),
			("slug", Value::Utf8("status".to_string())),
			("title", Value::Utf8("Status".to_string())),
			("monitor_ids", Value::List(vec![monitor.into_value()])),
		]),
	);

	let leaks: Vec<(&str, usize)> = ["uptime::monitors", "uptime::status_pages", "uptime::status_page_monitors"]
		.into_iter()
		.map(|table| {
			assert_eq!(
				query_rows(&db, bob, &format!("from {table}")),
				0,
				"bob must not see {table} directly"
			);
			(table, query_rows(&db, bob, &format!("let $rows = from {table}; from $rows")))
		})
		.filter(|(_, leaked)| *leaked > 0)
		.collect();
	assert!(leaks.is_empty(), "bob read alice's rows through a let binding: {leaks:?}");
}
