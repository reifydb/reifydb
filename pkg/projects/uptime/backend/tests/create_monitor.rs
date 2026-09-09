// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
// Guards for uptime::create_monitor, the procedure the web app calls instead
// of the HTTP create route. The point of the procedure is that the server, not
// the client, decides who owns the monitor and what its id, creation time and
// initial state are: the owner is the caller identity, the id and created_at
// are generated in the body, and the row starts as an unchecked monitor. These
// tests load the real migrations via #[path] on src/schema.rs so a regression
// in the shipped DDL or its policies fails here.

#[path = "../src/schema.rs"]
mod schema;

use std::collections::HashMap;

use reifydb::{
	Database, IdentityId, Value, WithSubsystem, server,
	value::{
		params::Params,
		value::{
			duration::Duration, frame::frame::Frame, identity::IdentityKind, into::IntoValue, uuid::Uuid7,
		},
	},
};

fn build() -> Database {
	server::memory().with_flow(|f| f).with_migrations(schema::migrations()).build().expect("build memory db")
}

fn admin(db: &Database, rql: &str) {
	let r = db.engine().admin_as(IdentityId::root(), rql, Params::None);
	if let Some(e) = r.error {
		panic!("admin failed for [{rql}]: {e:?}");
	}
}

fn query_as(db: &Database, id: IdentityId, rql: &str, params: Params) -> Result<Vec<Frame>, String> {
	let r = db.engine().query_as(id, rql, params);
	match r.error {
		Some(e) => Err(format!("{e:?}")),
		None => Ok(r.frames),
	}
}

fn command_as(db: &Database, id: IdentityId, rql: &str, params: Params) -> Result<Vec<Frame>, String> {
	let r = db.engine().command_as(id, rql, params);
	match r.error {
		Some(e) => Err(format!("{e:?}")),
		None => Ok(r.frames),
	}
}

fn params(entries: &[(&str, Value)]) -> Params {
	let map: HashMap<String, Value> = entries.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect();
	Params::from(map)
}

fn lookup_identity(db: &Database, name: &str) -> IdentityId {
	let frames = query_as(
		db,
		IdentityId::root(),
		"from system::identities filter { name == $name } map { id }",
		params(&[("name", Value::Utf8(name.to_string()))]),
	)
	.expect("identity lookup");
	match column(&frames, "id") {
		Value::IdentityId(id) => id,
		other => panic!("unexpected identity value for {name}: {other:?}"),
	}
}

fn new_user(db: &Database, name: &str) -> IdentityId {
	admin(db, &format!("CREATE USER {name}"));
	lookup_identity(db, name)
}

fn new_guest(db: &Database) -> IdentityId {
	let mut txn = db.engine().begin_admin(IdentityId::root()).expect("begin admin");
	let guest = db
		.catalog()
		.create_identity(&mut txn, "guest:visitor", IdentityKind::Guest, db.clock(), db.engine().rng())
		.expect("create guest");
	txn.commit().expect("commit guest");
	guest.id
}

fn rows(frames: &[Frame]) -> usize {
	frames.first().map(Frame::row_count).unwrap_or(0)
}

fn column(frames: &[Frame], name: &str) -> Value {
	let frame = frames.first().expect("frame");
	assert_eq!(frame.row_count(), 1, "expected exactly one row when reading column {name}");
	frame.columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("column {name}")).data.get_value(0)
}

const CREATE_CALL: &str = "CALL uptime::create_monitor($name, $kind, $target, $interval, $timeout, \
	 $http_method, $expected_status, $keyword, $expected_ip, $failure_threshold, $enabled)";

fn create_params(name: &str, target: &str) -> Params {
	params(&[
		("name", Value::Utf8(name.to_string())),
		("kind", Value::Utf8("http".to_string())),
		("target", Value::Utf8(target.to_string())),
		("interval", Duration::from_seconds(60).unwrap().into_value()),
		("timeout", Duration::from_seconds(10).unwrap().into_value()),
		("http_method", Value::Utf8("GET".to_string())),
		("expected_status", Value::Int2(200)),
		("keyword", Value::none()),
		("expected_ip", Value::none()),
		("failure_threshold", Value::Int2(3)),
		("enabled", Value::Boolean(true)),
	])
}

fn create_as(db: &Database, caller: IdentityId, name: &str, target: &str) -> Uuid7 {
	let frames = command_as(db, caller, CREATE_CALL, create_params(name, target)).expect("create_monitor call");
	match column(&frames, "id") {
		Value::Uuid7(id) => id,
		other => panic!("create_monitor must return the generated id, got {other:?}"),
	}
}

fn monitor_row(db: &Database, id: Uuid7) -> Vec<Frame> {
	query_as(
		db,
		IdentityId::root(),
		"from uptime::monitors filter { id == $id } map { id, owner, name, kind, target, interval, timeout, \
		 http_method, expected_status, keyword, expected_ip, failure_threshold, enabled, created_at, \
		 last_checked_at, consecutive_failures, status }",
		params(&[("id", id.into_value())]),
	)
	.expect("root monitor read")
}

#[test]
fn user_creates_monitor_with_server_generated_id_time_and_initial_state() {
	// The client sends only what it knows; id, created_at, owner and the unchecked initial state
	// are the server's. If the procedure ever stopped generating them, or accepted them from the
	// caller, the row read back would differ from what the call returned or from the clock.
	let db = build();
	let alice = new_user(&db, "alice");

	let before_ms = db.clock().now().to_nanos() / 1_000_000;
	let id = create_as(&db, alice, "  Home page  ", "  https://example.com  ");
	let after_ms = db.clock().now().to_nanos() / 1_000_000;

	let row = monitor_row(&db, id);
	assert_eq!(rows(&row), 1, "the call must have inserted exactly the monitor it returned");
	assert_eq!(column(&row, "owner"), Value::IdentityId(alice), "owner must be the caller");
	assert_eq!(column(&row, "name"), Value::Utf8("Home page".to_string()), "name must be trimmed");
	assert_eq!(column(&row, "target"), Value::Utf8("https://example.com".to_string()), "target must be trimmed");
	assert_eq!(column(&row, "kind"), Value::Utf8("http".to_string()));
	assert_eq!(column(&row, "http_method"), Value::Utf8("GET".to_string()));
	assert_eq!(column(&row, "expected_status"), Value::Int2(200));
	assert!(matches!(column(&row, "keyword"), Value::None { .. }), "an absent keyword must stay none");
	assert!(matches!(column(&row, "expected_ip"), Value::None { .. }), "an absent expected_ip must stay none");
	assert_eq!(column(&row, "failure_threshold"), Value::Int2(3));
	assert_eq!(column(&row, "enabled"), Value::Boolean(true));
	assert_eq!(column(&row, "status"), Value::Utf8("unknown".to_string()), "a new monitor is unchecked");
	assert!(matches!(column(&row, "last_checked_at"), Value::None { .. }), "a new monitor has never been checked");
	assert_eq!(column(&row, "consecutive_failures"), Value::Int4(0));

	// datetime::now() carries millisecond precision, so compare on milliseconds.
	let created_ms = match column(&row, "created_at") {
		Value::DateTime(dt) => dt.to_nanos() / 1_000_000,
		other => panic!("created_at must be a datetime, got {other:?}"),
	};
	assert!(
		(before_ms..=after_ms).contains(&created_ms),
		"created_at {created_ms} must come from the database clock window {before_ms}..={after_ms}"
	);
}

#[test]
fn owner_is_the_caller_not_a_parameter() {
	// Two different identities calling the same procedure with the same input must end up with
	// two monitors, each owned by its own caller, and each caller must see only its own through
	// the owner read policy. If the owner were taken from input, or the procedure ran as root, one
	// user could create monitors under another.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");

	let alice_id = create_as(&db, alice, "shared", "https://example.com");
	let bob_id = create_as(&db, bob, "shared", "https://example.com");
	assert_ne!(alice_id, bob_id, "each call must generate its own id");

	assert_eq!(column(&monitor_row(&db, alice_id), "owner"), Value::IdentityId(alice));
	assert_eq!(column(&monitor_row(&db, bob_id), "owner"), Value::IdentityId(bob));

	let alice_sees = query_as(&db, alice, "from uptime::monitors map { id }", Params::None).expect("alice read");
	assert_eq!(rows(&alice_sees), 1, "alice must see only her own monitor");
	assert_eq!(column(&alice_sees, "id"), Value::Uuid7(alice_id));
}

#[test]
fn guest_creates_monitor_under_its_own_identity() {
	// Guest mode gives a visitor a real identity that later gets promoted in place, so a guest
	// must be able to create monitors it owns; the call policy admits kind guest for this reason.
	let db = build();
	let guest = new_guest(&db);

	let id = create_as(&db, guest, "guest monitor", "https://example.com");

	assert_eq!(column(&monitor_row(&db, id), "owner"), Value::IdentityId(guest), "guest must own its monitor");
}

#[test]
fn service_is_denied_at_the_call_gate() {
	// Probes run as service identities and must never create monitors. If the call policy widened
	// to `true` a probe token would be enough to fill a user's account with monitors.
	let db = build();
	admin(&db, "CREATE SERVICE probe_svc");
	let service = lookup_identity(&db, "probe_svc");

	let denied = command_as(&db, service, CREATE_CALL, create_params("m", "https://example.com"));
	assert!(denied.is_err(), "service must be denied create_monitor, got: {denied:?}");

	let all =
		query_as(&db, IdentityId::root(), "from uptime::monitors map { id }", Params::None).expect("root read");
	assert_eq!(rows(&all), 0, "a denied call must not have inserted anything");
}

#[test]
fn user_cannot_forge_the_owner_by_inserting_directly() {
	// The procedure inserts as the caller, so the monitors insert policy must be owner-bound
	// rather than kind-bound: a user may write only rows it owns. If the policy relaxed to
	// `$identity.kind == "user"` a direct insert could plant a monitor in another account.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let now = db.clock().now();

	let forged = command_as(
		&db,
		alice,
		"INSERT uptime::monitors [{ id: uuid::v7(), owner: $owner, name: \"m\", kind: \"http\", \
		 target: \"https://example.com\", interval: $iv, timeout: $iv, http_method: none, expected_status: none, \
		 keyword: none, expected_ip: none, failure_threshold: 1, enabled: true, created_at: $now, \
		 last_checked_at: none, consecutive_failures: 0, status: \"unknown\" }]",
		params(&[
			("owner", bob.into_value()),
			("iv", Duration::from_seconds(30).unwrap().into_value()),
			("now", now.into_value()),
		]),
	);
	assert!(forged.is_err(), "alice must not insert a monitor owned by bob, got: {forged:?}");

	let bobs = query_as(&db, bob, "from uptime::monitors map { id }", Params::None).expect("bob read");
	assert_eq!(rows(&bobs), 0, "no monitor must have been planted in bob's account");
}
