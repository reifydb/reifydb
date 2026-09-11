// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{
	Database, Error, IdentityId, Value, WithSubsystem, server,
	value::{
		params::Params,
		value::{
			duration::Duration, frame::frame::Frame, identity::IdentityKind, into::IntoValue, uuid::Uuid7,
		},
	},
};
use reifydb_uptime::migration_path;

const CREATE: &str = "CALL uptime::create_monitor($id, $name, $kind, $target, $interval, $timeout, \
	 $http_method, $expected_status, $keyword, $expected_ip, $failure_threshold, $enabled)";

const UPDATE: &str = "CALL uptime::update_monitor($id, $name, $kind, $target, $interval, $timeout, \
	 $http_method, $expected_status, $keyword, $expected_ip, $failure_threshold, $enabled)";

const ADD_REGION: &str = "CALL uptime::add_monitor_region($monitor_id, $region_id)";

const REMOVE_REGION: &str = "CALL uptime::remove_monitor_region($monitor_id, $region_id)";

const DELETE: &str = "CALL uptime::delete_monitor($id)";

const CHECK_REGIONS: &str = "CALL uptime::check_monitor_regions($monitor_id)";

type Input = Vec<(&'static str, Value)>;

fn build() -> Database {
	server::memory().with_flow(|f| f).with_migrations(migration_path()).build().expect("build memory db")
}

fn admin(db: &Database, rql: &str) {
	let r = db.engine().admin_as(IdentityId::root(), rql, Params::None);
	if let Some(e) = r.error {
		panic!("admin failed for [{rql}]: {e:?}");
	}
}

fn query_as(db: &Database, id: IdentityId, rql: &str, params: Params) -> Result<Vec<Frame>, Error> {
	let r = db.engine().query_as(id, rql, params);
	match r.error {
		Some(e) => Err(e),
		None => Ok(r.frames),
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
	query_as(db, IdentityId::root(), rql, params).unwrap_or_else(|e| panic!("root query failed for [{rql}]: {e:?}"))
}

fn root_cmd(db: &Database, rql: &str, params: Params) {
	command_as(db, IdentityId::root(), rql, params)
		.unwrap_or_else(|e| panic!("root command failed for [{rql}]: {e:?}"));
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

fn values(frames: &[Frame], name: &str) -> Vec<Value> {
	let Some(frame) = frames.first() else {
		return Vec::new();
	};
	let Some(col) = frame.columns.iter().find(|c| c.name == name) else {
		return Vec::new();
	};
	(0..frame.row_count()).map(|i| col.data.get_value(i)).collect()
}

fn expect_error(result: Result<Vec<Frame>, Error>, code: &str, message: &str) {
	match result {
		Ok(frames) => panic!("expected a {code} error containing {message:?}, got {} frame(s)", frames.len()),
		Err(e) => {
			assert_eq!(e.0.code, code, "wrong error code, message was {:?}", e.0.message);
			assert!(
				e.0.message.contains(message),
				"error message {:?} must contain {message:?}",
				e.0.message
			);
		}
	}
}

fn lookup_identity(db: &Database, name: &str) -> IdentityId {
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

fn new_user(db: &Database, name: &str) -> IdentityId {
	admin(db, &format!("CREATE USER {name}"));
	lookup_identity(db, name)
}

fn new_guest(db: &Database, name: &str) -> IdentityId {
	let mut txn = db.engine().begin_admin(IdentityId::root()).expect("begin admin");
	let guest = db
		.catalog()
		.create_identity(&mut txn, name, IdentityKind::Guest, db.clock(), db.engine().rng())
		.expect("create guest");
	txn.commit().expect("commit guest");
	guest.id
}

fn new_id(db: &Database) -> Uuid7 {
	Uuid7::generate(db.clock(), db.engine().rng())
}

fn secs(n: i64) -> Value {
	Duration::from_seconds(n).unwrap().into_value()
}

fn text(s: &str) -> Value {
	Value::Utf8(s.to_string())
}

fn monitor_input(id: Uuid7) -> Input {
	vec![
		("id", id.into_value()),
		("name", text("Home page")),
		("kind", text("http")),
		("target", text("https://example.com")),
		("interval", secs(60)),
		("timeout", secs(10)),
		("http_method", text("GET")),
		("expected_status", Value::Int2(200)),
		("keyword", Value::none()),
		("expected_ip", Value::none()),
		("failure_threshold", Value::Int2(3)),
		("enabled", Value::Boolean(true)),
	]
}

fn with(mut input: Input, key: &str, value: Value) -> Input {
	let slot = input.iter_mut().find(|(k, _)| *k == key).unwrap_or_else(|| panic!("no input field {key}"));
	slot.1 = value;
	input
}

fn create(db: &Database, caller: IdentityId) -> Uuid7 {
	let id = new_id(db);
	command_as(db, caller, CREATE, params(&monitor_input(id))).expect("create_monitor");
	id
}

fn region(db: &Database, label: &str) -> Uuid7 {
	let frames = root_query(
		db,
		"from uptime::regions filter { label == $label } map { id }",
		params(&[("label", text(label))]),
	);
	match column(&frames, "id") {
		Value::Uuid7(id) => id,
		other => panic!("unexpected region id for {label}: {other:?}"),
	}
}

fn region_params(monitor_id: Uuid7, region_id: Uuid7) -> Params {
	params(&[("monitor_id", monitor_id.into_value()), ("region_id", region_id.into_value())])
}

fn id_params(id: Uuid7) -> Params {
	params(&[("id", id.into_value())])
}

fn monitor_row(db: &Database, id: Uuid7) -> Vec<Frame> {
	root_query(
		db,
		"from uptime::monitors filter { id == $id } map { id, owner, name, kind, target, interval, timeout, \
		 http_method, expected_status, keyword, expected_ip, failure_threshold, enabled, created_at, \
		 last_checked_at, consecutive_failures, status }",
		id_params(id),
	)
}

fn monitor_regions(db: &Database, monitor_id: Uuid7) -> Vec<Value> {
	values(
		&root_query(
			db,
			"from uptime::monitor_regions filter { monitor_id == $m } map { region_id }",
			params(&[("m", monitor_id.into_value())]),
		),
		"region_id",
	)
}

fn count(db: &Database, table: &str, column_name: &str, id: Uuid7) -> usize {
	rows(&root_query(
		db,
		&format!("from uptime::{table} filter {{ {column_name} == $id }} map {{ {column_name} }}"),
		id_params(id),
	))
}

fn insert_result(db: &Database, monitor_id: Uuid7, owner: IdentityId, region_id: Uuid7) {
	root_cmd(
		db,
		"INSERT uptime::results [{ id: $id, monitor_id: $m, owner: $owner, region_id: $r, probe: none, \
		 checked_at: $now, success: true, response_time: none, status_code: none, error: none }]",
		params(&[
			("id", new_id(db).into_value()),
			("m", monitor_id.into_value()),
			("owner", owner.into_value()),
			("r", region_id.into_value()),
			("now", db.clock().now().into_value()),
		]),
	);
}

#[test]
fn owner_updates_only_the_editable_fields() {
	// A regression that let update_monitor rewrite check state would erase a monitor's history on every edit.
	let db = build();
	let alice = new_user(&db, "alice");
	let id = create(&db, alice);
	let checked_at = db.clock().now();
	root_cmd(
		&db,
		"update uptime::monitors { status: \"up\", last_checked_at: $at, consecutive_failures: 2 } filter { id == $id }",
		params(&[("id", id.into_value()), ("at", checked_at.into_value())]),
	);
	let created_at = column(&monitor_row(&db, id), "created_at");

	let edited = with(monitor_input(id), "name", text("  Renamed  "));
	let edited = with(edited, "kind", text("tcp"));
	let edited = with(edited, "target", text("  example.com:443  "));
	let edited = with(edited, "interval", secs(120));
	let edited = with(edited, "timeout", secs(20));
	let edited = with(edited, "http_method", Value::none());
	let edited = with(edited, "expected_status", Value::none());
	let edited = with(edited, "keyword", text("welcome"));
	let edited = with(edited, "expected_ip", text("192.0.2.1"));
	let edited = with(edited, "failure_threshold", Value::Int2(5));
	let edited = with(edited, "enabled", Value::Boolean(false));
	command_as(&db, alice, UPDATE, params(&edited)).expect("owner update");

	let row = monitor_row(&db, id);
	assert_eq!(column(&row, "name"), text("Renamed"), "name must be trimmed");
	assert_eq!(column(&row, "kind"), text("tcp"));
	assert_eq!(column(&row, "target"), text("example.com:443"), "target must be trimmed");
	assert_eq!(column(&row, "interval"), secs(120));
	assert_eq!(column(&row, "timeout"), secs(20));
	assert!(matches!(column(&row, "http_method"), Value::None { .. }), "http_method must be cleared");
	assert!(matches!(column(&row, "expected_status"), Value::None { .. }), "expected_status must be cleared");
	assert_eq!(column(&row, "keyword"), text("welcome"));
	assert_eq!(column(&row, "expected_ip"), text("192.0.2.1"));
	assert_eq!(column(&row, "failure_threshold"), Value::Int2(5));
	assert_eq!(column(&row, "enabled"), Value::Boolean(false));
	assert_eq!(column(&row, "owner"), Value::IdentityId(alice), "owner must not change");
	assert_eq!(column(&row, "created_at"), created_at, "created_at must not change");
	assert_eq!(column(&row, "status"), text("up"), "status must not change");
	assert_eq!(column(&row, "last_checked_at"), checked_at.into_value(), "last_checked_at must not change");
	assert_eq!(column(&row, "consecutive_failures"), Value::Int4(2), "consecutive_failures must not change");
}

#[test]
fn another_owner_cannot_update_a_monitor() {
	// Neither the procedure nor a direct UPDATE may touch a row owned by someone else.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let id = create(&db, alice);

	let hijack = with(monitor_input(id), "name", text("hijacked"));
	expect_error(command_as(&db, bob, UPDATE, params(&hijack)), "ASSERT", "monitor not found");
	expect_error(
		command_as(
			&db,
			bob,
			"update uptime::monitors { name: \"hijacked\" } filter { id == $id }",
			id_params(id),
		),
		"POLICY_001",
		"denied update",
	);

	assert_eq!(column(&monitor_row(&db, id), "name"), text("Home page"), "alice's monitor must be untouched");
}

#[test]
fn guest_updates_its_own_monitor() {
	// Guests own real rows, so the update call policy and the owner filter must admit them.
	let db = build();
	let guest = new_guest(&db, "guest:one");
	let id = create(&db, guest);

	command_as(&db, guest, UPDATE, params(&with(monitor_input(id), "name", text("guest edit"))))
		.expect("guest update");

	assert_eq!(column(&monitor_row(&db, id), "name"), text("guest edit"));
}

#[test]
fn owner_adds_regions_as_unchecked_rows_it_owns() {
	// Region rows must start unchecked and be owned by the caller, or the owner read policy hides them.
	let db = build();
	let alice = new_user(&db, "alice");
	let id = create(&db, alice);
	let us = region(&db, "US East");
	let eu = region(&db, "EU West");

	command_as(&db, alice, ADD_REGION, region_params(id, us)).expect("add us");
	command_as(&db, alice, ADD_REGION, region_params(id, eu)).expect("add eu");

	let frames = root_query(
		&db,
		"from uptime::monitor_regions filter { monitor_id == $m } \
		 map { region_id, owner, status, last_checked_at, consecutive_failures }",
		params(&[("m", id.into_value())]),
	);
	assert_eq!(rows(&frames), 2, "both regions must be attached");
	let mut regions = values(&frames, "region_id");
	regions.sort_by_key(|v| format!("{v:?}"));
	let mut expected = vec![us.into_value(), eu.into_value()];
	expected.sort_by_key(|v| format!("{v:?}"));
	assert_eq!(regions, expected);
	assert!(values(&frames, "owner").iter().all(|v| *v == Value::IdentityId(alice)), "rows must be owned by alice");
	assert!(values(&frames, "status").iter().all(|v| *v == text("unknown")), "rows must start unknown");
	assert!(
		values(&frames, "last_checked_at").iter().all(|v| matches!(v, Value::None { .. })),
		"rows must start never checked"
	);
	assert!(values(&frames, "consecutive_failures").iter().all(|v| *v == Value::Int4(0)));

	let visible =
		query_as(&db, alice, "from uptime::monitor_regions map { region_id }", Params::None).expect("read");
	assert_eq!(rows(&visible), 2, "alice must see her region rows through the owner read policy");
}

#[test]
fn owner_removes_a_region_and_only_its_results() {
	// Removing one region must not drop the other region or its history.
	let db = build();
	let alice = new_user(&db, "alice");
	let id = create(&db, alice);
	let us = region(&db, "US East");
	let eu = region(&db, "EU West");
	command_as(&db, alice, ADD_REGION, region_params(id, us)).expect("add us");
	command_as(&db, alice, ADD_REGION, region_params(id, eu)).expect("add eu");
	insert_result(&db, id, alice, us);
	insert_result(&db, id, alice, eu);

	command_as(&db, alice, REMOVE_REGION, region_params(id, us)).expect("remove us");

	assert_eq!(monitor_regions(&db, id), vec![eu.into_value()], "only the EU region must remain");
	let results = root_query(
		&db,
		"from uptime::results filter { monitor_id == $m } map { region_id }",
		params(&[("m", id.into_value())]),
	);
	assert_eq!(values(&results, "region_id"), vec![eu.into_value()], "only the removed region's results must go");
}

#[test]
fn another_owner_cannot_add_or_remove_regions_on_a_monitor() {
	// Attaching regions to someone else's monitor would make the probes check it on their behalf.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let id = create(&db, alice);
	let us = region(&db, "US East");
	let eu = region(&db, "EU West");
	command_as(&db, alice, ADD_REGION, region_params(id, us)).expect("add us");

	expect_error(command_as(&db, bob, ADD_REGION, region_params(id, eu)), "ASSERT", "monitor not found");
	expect_error(command_as(&db, bob, REMOVE_REGION, region_params(id, us)), "ASSERT", "monitor not found");
	expect_error(
		command_as(
			&db,
			bob,
			"delete uptime::monitor_regions filter { monitor_id == $monitor_id }",
			region_params(id, us),
		),
		"POLICY_001",
		"denied delete",
	);

	assert_eq!(monitor_regions(&db, id), vec![us.into_value()], "alice's regions must be untouched");
}

#[test]
fn guest_adds_and_removes_regions_on_its_own_monitor() {
	// Guests must be able to manage regions of monitors they own.
	let db = build();
	let guest = new_guest(&db, "guest:one");
	let id = create(&db, guest);
	let us = region(&db, "US East");
	let eu = region(&db, "EU West");

	command_as(&db, guest, ADD_REGION, region_params(id, us)).expect("guest add us");
	command_as(&db, guest, ADD_REGION, region_params(id, eu)).expect("guest add eu");
	command_as(&db, guest, REMOVE_REGION, region_params(id, us)).expect("guest remove us");

	assert_eq!(monitor_regions(&db, id), vec![eu.into_value()]);
}

#[test]
fn add_region_rejects_an_unknown_region_a_duplicate_and_the_21st_region() {
	// Without these checks a client could attach a phantom region, double a region, or exceed the 20-region cap.
	let db = build();
	let alice = new_user(&db, "alice");
	let id = create(&db, alice);
	let us = region(&db, "US East");

	expect_error(command_as(&db, alice, ADD_REGION, region_params(id, new_id(&db))), "ASSERT", "unknown region");
	command_as(&db, alice, ADD_REGION, region_params(id, us)).expect("add us");
	expect_error(command_as(&db, alice, ADD_REGION, region_params(id, us)), "ASSERT", "already on this monitor");
	assert_eq!(monitor_regions(&db, id), vec![us.into_value()], "a rejected add must not insert a row");

	let mut extra = Vec::new();
	for i in 0..20 {
		let region_id = new_id(&db);
		root_cmd(
			&db,
			"INSERT uptime::regions [{ id: $id, label: $label }]",
			params(&[("id", region_id.into_value()), ("label", text(&format!("extra {i}")))]),
		);
		extra.push(region_id);
	}
	for region_id in &extra[..19] {
		command_as(&db, alice, ADD_REGION, region_params(id, *region_id)).expect("add up to 20 regions");
	}
	assert_eq!(monitor_regions(&db, id).len(), 20, "exactly 20 regions must be allowed");

	expect_error(command_as(&db, alice, ADD_REGION, region_params(id, extra[19])), "ASSERT", "at most 20 regions");
	assert_eq!(monitor_regions(&db, id).len(), 20, "the 21st region must not be inserted");
}

#[test]
fn owner_deletes_a_monitor_with_its_regions_results_and_page_memberships() {
	// A leftover region row keeps the scheduler checking a deleted monitor; a leftover member leaks it onto a page.
	let db = build();
	let alice = new_user(&db, "alice");
	let id = create(&db, alice);
	let kept = create(&db, alice);
	let us = region(&db, "US East");
	for monitor in [id, kept] {
		command_as(&db, alice, ADD_REGION, region_params(monitor, us)).expect("add region");
		insert_result(&db, monitor, alice, us);
	}
	let page = new_id(&db);
	command_as(
		&db,
		alice,
		"CALL uptime::create_status_page($page, $slug, $title); \
		 CALL uptime::add_status_page_monitor($page, $m0, $p0); \
		 CALL uptime::add_status_page_monitor($page, $m1, $p1)",
		params(&[
			("page", page.into_value()),
			("slug", text("status")),
			("title", text("Status")),
			("m0", id.into_value()),
			("p0", Value::Int2(0)),
			("m1", kept.into_value()),
			("p1", Value::Int2(1)),
		]),
	)
	.expect("page with both monitors");

	command_as(&db, alice, DELETE, id_params(id)).expect("owner delete");

	assert_eq!(count(&db, "monitors", "id", id), 0, "monitor must be gone");
	assert_eq!(count(&db, "monitor_regions", "monitor_id", id), 0, "its regions must be gone");
	assert_eq!(count(&db, "results", "monitor_id", id), 0, "its results must be gone");
	assert_eq!(count(&db, "status_page_monitors", "monitor_id", id), 0, "its page memberships must be gone");
	assert_eq!(count(&db, "monitors", "id", kept), 1, "the other monitor must survive");
	assert_eq!(count(&db, "monitor_regions", "monitor_id", kept), 1, "the other monitor's region must survive");
	assert_eq!(count(&db, "results", "monitor_id", kept), 1, "the other monitor's results must survive");
	assert_eq!(count(&db, "status_page_monitors", "monitor_id", kept), 1, "the other membership must survive");
}

#[test]
fn another_owner_cannot_delete_a_monitor() {
	// A denied delete must fail loudly and leave every related row in place.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let id = create(&db, alice);
	let us = region(&db, "US East");
	command_as(&db, alice, ADD_REGION, region_params(id, us)).expect("add region");
	insert_result(&db, id, alice, us);

	expect_error(command_as(&db, bob, DELETE, id_params(id)), "ASSERT", "monitor not found");
	expect_error(
		command_as(&db, bob, "delete uptime::monitors filter { id == $id }", id_params(id)),
		"POLICY_001",
		"denied delete",
	);
	expect_error(
		command_as(&db, bob, "delete uptime::results filter { monitor_id == $id }", id_params(id)),
		"POLICY_001",
		"denied delete",
	);

	assert_eq!(count(&db, "monitors", "id", id), 1);
	assert_eq!(count(&db, "monitor_regions", "monitor_id", id), 1);
	assert_eq!(count(&db, "results", "monitor_id", id), 1);
}

#[test]
fn guest_deletes_its_own_monitor() {
	// Guests must be able to delete what they own, including the monitor's region rows.
	let db = build();
	let guest = new_guest(&db, "guest:one");
	let id = create(&db, guest);
	command_as(&db, guest, ADD_REGION, region_params(id, region(&db, "US East"))).expect("guest add region");

	command_as(&db, guest, DELETE, id_params(id)).expect("guest delete");

	assert_eq!(count(&db, "monitors", "id", id), 0);
	assert_eq!(count(&db, "monitor_regions", "monitor_id", id), 0);
}

#[test]
fn out_of_range_monitor_input_is_rejected_by_create_and_update() {
	// Each check must fail the call with its own message on both procedures, and never write a row.
	let db = build();
	let alice = new_user(&db, "alice");
	let existing = create(&db, alice);
	let cases: Vec<(&str, Box<dyn Fn(Input) -> Input>)> = vec![
		("name must be between 1 and 200 characters", Box::new(|i| with(i, "name", text("   ")))),
		("name must be between 1 and 200 characters", Box::new(|i| with(i, "name", text(&"n".repeat(201))))),
		("target must be between 1 and 500 characters", Box::new(|i| with(i, "target", text("")))),
		(
			"target must be between 1 and 500 characters",
			Box::new(|i| with(i, "target", text(&"t".repeat(501)))),
		),
		(
			"interval must be at least 5 seconds",
			Box::new(|i| with(with(i, "interval", secs(4)), "timeout", secs(1))),
		),
		(
			"timeout must be at least 1 second",
			Box::new(|i| with(i, "timeout", Duration::from_milliseconds(999).unwrap().into_value())),
		),
		(
			"timeout must not exceed the interval",
			Box::new(|i| with(with(i, "interval", secs(10)), "timeout", secs(11))),
		),
		("failure threshold must be at least 1", Box::new(|i| with(i, "failure_threshold", Value::Int2(0)))),
		(
			"expected status must be a valid HTTP status code",
			Box::new(|i| with(i, "expected_status", Value::Int2(99))),
		),
		(
			"expected status must be a valid HTTP status code",
			Box::new(|i| with(i, "expected_status", Value::Int2(600))),
		),
		("kind must be one of http, tcp, ping, dns", Box::new(|i| with(i, "kind", text("smtp")))),
		("http method must be GET or HEAD", Box::new(|i| with(i, "http_method", text("POST")))),
	];

	for (message, mutate) in &cases {
		let id = new_id(&db);
		expect_error(command_as(&db, alice, CREATE, params(&mutate(monitor_input(id)))), "ASSERT", message);
		assert_eq!(count(&db, "monitors", "id", id), 0, "a rejected create must not insert ({message})");

		let bad_update = with(mutate(monitor_input(existing)), "id", existing.into_value());
		expect_error(command_as(&db, alice, UPDATE, params(&bad_update)), "ASSERT", message);
		let row = monitor_row(&db, existing);
		assert_eq!(column(&row, "name"), text("Home page"), "a rejected update must not write ({message})");
		assert_eq!(column(&row, "interval"), secs(60), "a rejected update must not write ({message})");
	}
}

#[test]
fn boundary_monitor_input_is_accepted() {
	// Off-by-one in any range check would reject values the HTTP route accepted.
	let db = build();
	let alice = new_user(&db, "alice");
	let cases: Vec<(&str, Box<dyn Fn(Input) -> Input>)> = vec![
		("200 character name", Box::new(|i| with(i, "name", text(&"n".repeat(200))))),
		("500 character target", Box::new(|i| with(i, "target", text(&"t".repeat(500))))),
		("5s interval equal to timeout", Box::new(|i| with(with(i, "interval", secs(5)), "timeout", secs(5)))),
		("1s timeout", Box::new(|i| with(i, "timeout", secs(1)))),
		("threshold 1", Box::new(|i| with(i, "failure_threshold", Value::Int2(1)))),
		("status 100", Box::new(|i| with(i, "expected_status", Value::Int2(100)))),
		("status 599", Box::new(|i| with(i, "expected_status", Value::Int2(599)))),
		("no expected status", Box::new(|i| with(i, "expected_status", Value::none()))),
		("HEAD", Box::new(|i| with(i, "http_method", text("HEAD")))),
		("no method", Box::new(|i| with(i, "http_method", Value::none()))),
		(
			"method ignored for tcp",
			Box::new(|i| with(with(i, "kind", text("tcp")), "http_method", text("POST"))),
		),
		("ping", Box::new(|i| with(i, "kind", text("ping")))),
		("dns", Box::new(|i| with(i, "kind", text("dns")))),
	];

	for (label, mutate) in &cases {
		let id = new_id(&db);
		command_as(&db, alice, CREATE, params(&mutate(monitor_input(id))))
			.unwrap_or_else(|e| panic!("create with {label} must succeed: {e:?}"));
		command_as(&db, alice, UPDATE, params(&mutate(monitor_input(id))))
			.unwrap_or_else(|e| panic!("update with {label} must succeed: {e:?}"));
		assert_eq!(count(&db, "monitors", "id", id), 1, "{label}");
	}
}

#[test]
fn an_existing_monitor_id_cannot_be_reused_by_another_owner() {
	// Ids come from the client, so a second row with a taken id would split the probes' view of alice's monitor.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let id = create(&db, alice);

	expect_error(command_as(&db, bob, CREATE, params(&monitor_input(id))), "INDEX_001", "");

	let row = monitor_row(&db, id);
	assert_eq!(rows(&row), 1, "there must still be exactly one monitor with this id");
	assert_eq!(column(&row, "owner"), Value::IdentityId(alice), "the id must still belong to alice");
}

#[test]
fn service_is_denied_every_owner_monitor_procedure() {
	// Probe tokens must never be enough to edit, re-region or delete a user's monitor.
	let db = build();
	let alice = new_user(&db, "alice");
	admin(&db, "CREATE SERVICE probe_svc");
	let service = lookup_identity(&db, "probe_svc");
	let id = create(&db, alice);
	let us = region(&db, "US East");
	command_as(&db, alice, ADD_REGION, region_params(id, us)).expect("add region");

	expect_error(
		command_as(&db, service, UPDATE, params(&with(monitor_input(id), "name", text("svc")))),
		"POLICY_001",
		"denied call",
	);
	expect_error(
		command_as(&db, service, ADD_REGION, region_params(id, region(&db, "EU West"))),
		"POLICY_001",
		"denied call",
	);
	expect_error(command_as(&db, service, REMOVE_REGION, region_params(id, us)), "POLICY_001", "denied call");
	expect_error(command_as(&db, service, DELETE, id_params(id)), "POLICY_001", "denied call");

	assert_eq!(column(&monitor_row(&db, id), "name"), text("Home page"));
	assert_eq!(monitor_regions(&db, id), vec![us.into_value()]);
}

fn monitor_id_params(id: Uuid7) -> Params {
	params(&[("monitor_id", id.into_value())])
}

fn region_owners(db: &Database, monitor_id: Uuid7) -> Vec<Value> {
	values(
		&root_query(
			db,
			"from uptime::monitor_regions filter { monitor_id == $m } map { owner }",
			params(&[("m", monitor_id.into_value())]),
		),
		"owner",
	)
}

#[test]
fn direct_update_cannot_take_another_owners_monitor() {
	// The update policy must hold on the old row, or rewriting owner hands any monitor to the caller.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let id = create(&db, alice);

	for filter in ["filter { id == $id }", "filter { true }"] {
		expect_error(
			command_as(
				&db,
				bob,
				&format!("update uptime::monitors {{ owner: $identity.id }} {filter}"),
				id_params(id),
			),
			"POLICY_001",
			"denied update",
		);
	}

	assert_eq!(
		column(&monitor_row(&db, id), "owner"),
		Value::IdentityId(alice),
		"the monitor must still be alice's"
	);
}

#[test]
fn direct_update_cannot_give_a_monitor_to_another_owner() {
	// The update policy must hold on the new row, or rewriting owner plants a monitor in another account.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let id = create(&db, alice);

	expect_error(
		command_as(
			&db,
			alice,
			"update uptime::monitors { owner: $owner } filter { id == $id }",
			params(&[("id", id.into_value()), ("owner", bob.into_value())]),
		),
		"POLICY_001",
		"denied update",
	);

	assert_eq!(
		column(&monitor_row(&db, id), "owner"),
		Value::IdentityId(alice),
		"the monitor must still be alice's"
	);
}

#[test]
fn direct_update_cannot_move_region_rows_between_owners() {
	// Region rows drive the probes and the rollup, so a direct UPDATE must never take or give one away.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let id = create(&db, alice);
	command_as(&db, alice, ADD_REGION, region_params(id, region(&db, "US East"))).expect("add region");
	let reassign = "update uptime::monitor_regions { owner: $owner } filter { monitor_id == $id }";

	for (caller, new_owner) in [(bob, bob), (alice, bob)] {
		expect_error(
			command_as(
				&db,
				caller,
				reassign,
				params(&[("id", id.into_value()), ("owner", new_owner.into_value())]),
			),
			"POLICY_001",
			"denied update",
		);
	}

	assert_eq!(region_owners(&db, id), vec![Value::IdentityId(alice)], "the region row must still be alice's");
}

#[test]
fn region_check_fails_until_the_monitor_has_a_region_of_its_own() {
	// The web ends every monitor save with this check, so a monitor without its own region must fail it.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let id = create(&db, alice);
	let us = region(&db, "US East");
	command_as(
		&db,
		bob,
		"INSERT uptime::monitor_regions [{ monitor_id: $m, owner: $owner, region_id: $r, status: \"unknown\", \
		 last_checked_at: none, consecutive_failures: 0 }]",
		params(&[("m", id.into_value()), ("owner", bob.into_value()), ("r", us.into_value())]),
	)
	.expect("the insert policy admits a region row the caller owns");

	expect_error(
		command_as(&db, alice, CHECK_REGIONS, monitor_id_params(id)),
		"ASSERT",
		"a monitor must use at least one region",
	);

	command_as(&db, alice, ADD_REGION, region_params(id, us)).expect("add region");
	command_as(&db, alice, CHECK_REGIONS, monitor_id_params(id)).expect("one region must pass the check");
}

#[test]
fn another_owner_or_a_service_cannot_run_the_region_check() {
	// A foreign monitor must look exactly like a missing one, and probe tokens must never reach owner procedures.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	admin(&db, "CREATE SERVICE probe_svc");
	let service = lookup_identity(&db, "probe_svc");
	let id = create(&db, alice);
	command_as(&db, alice, ADD_REGION, region_params(id, region(&db, "US East"))).expect("add region");

	expect_error(command_as(&db, bob, CHECK_REGIONS, monitor_id_params(id)), "ASSERT", "monitor not found");
	expect_error(command_as(&db, service, CHECK_REGIONS, monitor_id_params(id)), "POLICY_001", "denied call");
}

#[test]
fn guest_passes_the_region_check_on_its_own_monitor() {
	// Guests save monitors through the same command, so the check's call policy must admit them.
	let db = build();
	let guest = new_guest(&db, "guest:one");
	let id = create(&db, guest);
	command_as(&db, guest, ADD_REGION, region_params(id, region(&db, "US East"))).expect("guest add region");

	command_as(&db, guest, CHECK_REGIONS, monitor_id_params(id)).expect("guest check");
}

#[test]
fn a_monitor_command_ending_in_the_region_check_rolls_back_without_a_region() {
	// The check is the last CALL of a save, so a zero count must undo every earlier CALL in the command.
	let db = build();
	let alice = new_user(&db, "alice");
	let us = region(&db, "US East");

	let created = new_id(&db);
	let mut input = monitor_input(created);
	input.push(("monitor_id", created.into_value()));
	expect_error(
		command_as(&db, alice, &format!("{CREATE}; {CHECK_REGIONS}"), params(&input)),
		"ASSERT",
		"a monitor must use at least one region",
	);
	assert_eq!(count(&db, "monitors", "id", created), 0, "a create without a region must be rolled back");

	let id = create(&db, alice);
	command_as(&db, alice, ADD_REGION, region_params(id, us)).expect("add region");
	let mut input = with(monitor_input(id), "name", text("renamed"));
	input.push(("monitor_id", id.into_value()));
	input.push(("region_id", us.into_value()));
	expect_error(
		command_as(&db, alice, &format!("{UPDATE}; {REMOVE_REGION}; {CHECK_REGIONS}"), params(&input)),
		"ASSERT",
		"a monitor must use at least one region",
	);
	assert_eq!(column(&monitor_row(&db, id), "name"), text("Home page"), "the edit must be rolled back");
	assert_eq!(monitor_regions(&db, id), vec![us.into_value()], "the removed region must be restored");
}

#[test]
fn padded_name_and_target_are_measured_after_trimming() {
	// Values are stored trimmed, so the length limits must apply to the trimmed text, not the padding.
	let db = build();
	let alice = new_user(&db, "alice");
	let id = new_id(&db);
	let name = format!("  {}  ", "n".repeat(200));
	let target = format!("  {}  ", "t".repeat(500));
	let input = with(with(monitor_input(id), "name", text(&name)), "target", text(&target));

	command_as(&db, alice, CREATE, params(&input)).expect("padded create");
	command_as(&db, alice, UPDATE, params(&input)).expect("padded update");

	let row = monitor_row(&db, id);
	assert_eq!(column(&row, "name"), text(&"n".repeat(200)));
	assert_eq!(column(&row, "target"), text(&"t".repeat(500)));
}

#[test]
fn keyword_and_expected_ip_are_length_limited_by_create_and_update() {
	// Without a limit any caller could store a keyword or expected ip of unbounded size on a monitor row.
	let db = build();
	let alice = new_user(&db, "alice");
	let existing = create(&db, alice);
	let cases = [
		("keyword", "k".repeat(201), "keyword must be at most 200 characters"),
		("expected_ip", "1".repeat(46), "expected ip must be at most 45 characters"),
	];

	for (field, value, message) in &cases {
		let id = new_id(&db);
		expect_error(
			command_as(&db, alice, CREATE, params(&with(monitor_input(id), field, text(value)))),
			"ASSERT",
			message,
		);
		assert_eq!(count(&db, "monitors", "id", id), 0, "a rejected create must not insert ({message})");
		let bad_update = with(monitor_input(existing), field, text(value));
		expect_error(command_as(&db, alice, UPDATE, params(&bad_update)), "ASSERT", message);
		assert!(
			matches!(column(&monitor_row(&db, existing), field), Value::None { .. }),
			"a rejected update must not write"
		);
	}

	let id = new_id(&db);
	let at_limit =
		with(with(monitor_input(id), "keyword", text(&"k".repeat(200))), "expected_ip", text(&"1".repeat(45)));
	command_as(&db, alice, CREATE, params(&at_limit)).expect("create at the limits");
	command_as(&db, alice, UPDATE, params(&at_limit)).expect("update at the limits");
}
