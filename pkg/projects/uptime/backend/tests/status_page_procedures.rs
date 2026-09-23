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

const CREATE_MONITOR: &str = "CALL uptime::create_monitor($id, $name, $kind, $target, $interval, $timeout, \
	 $http_method, $expected_status, $keyword, $expected_ip, $failure_threshold, $enabled)";

const CREATE: &str = "CALL uptime::create_status_page($id, $slug, $title)";

const UPDATE: &str = "CALL uptime::update_status_page($id, $slug, $title)";

const ADD_MEMBER: &str = "CALL uptime::add_status_page_monitors($status_page_id, $monitor_ids)";

const CLEAR: &str = "CALL uptime::clear_status_page_monitors($status_page_id)";

const DELETE: &str = "CALL uptime::delete_status_page($id)";

const CHECK_MEMBERS: &str = "CALL uptime::check_status_page_monitors($status_page_id)";

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

fn text(s: &str) -> Value {
	Value::Utf8(s.to_string())
}

fn monitor(db: &Database, caller: IdentityId) -> Uuid7 {
	let id = new_id(db);
	command_as(
		db,
		caller,
		CREATE_MONITOR,
		params(&[
			("id", id.into_value()),
			("name", text("Home page")),
			("kind", text("http")),
			("target", text("https://example.com")),
			("interval", Duration::from_seconds(60).unwrap().into_value()),
			("timeout", Duration::from_seconds(10).unwrap().into_value()),
			("http_method", Value::none()),
			("expected_status", Value::none()),
			("keyword", Value::none()),
			("expected_ip", Value::none()),
			("failure_threshold", Value::Int2(1)),
			("enabled", Value::Boolean(true)),
		]),
	)
	.expect("create_monitor");
	id
}

fn page_params(id: Uuid7, slug: &str, title: &str) -> Params {
	params(&[("id", id.into_value()), ("slug", text(slug)), ("title", text(title))])
}

fn member_params(page: Uuid7, monitor_ids: &[Uuid7]) -> Params {
	params(&[
		("status_page_id", page.into_value()),
		("monitor_ids", Value::List(monitor_ids.iter().map(|m| m.into_value()).collect())),
	])
}

fn page_id_params(page: Uuid7) -> Params {
	params(&[("status_page_id", page.into_value())])
}

fn id_params(id: Uuid7) -> Params {
	params(&[("id", id.into_value())])
}

fn create(db: &Database, caller: IdentityId, slug: &str) -> Uuid7 {
	let id = new_id(db);
	command_as(db, caller, CREATE, page_params(id, slug, "Status")).expect("create_status_page");
	id
}

fn add(db: &Database, caller: IdentityId, page: Uuid7, monitor_ids: &[Uuid7]) {
	command_as(db, caller, ADD_MEMBER, member_params(page, monitor_ids)).expect("add_status_page_monitors");
}

fn page_row(db: &Database, id: Uuid7) -> Vec<Frame> {
	root_query(
		db,
		"from uptime::status_pages filter { id == $id } map { id, owner, slug, title, created_at }",
		id_params(id),
	)
}

fn members(db: &Database, page: Uuid7) -> Vec<(Value, Value)> {
	let frames = root_query(
		db,
		"from uptime::status_page_monitors filter { status_page_id == $p } map { monitor_id, position, owner }",
		params(&[("p", page.into_value())]),
	);
	let mut pairs: Vec<(Value, Value)> =
		values(&frames, "position").into_iter().zip(values(&frames, "monitor_id")).collect();
	pairs.sort_by_key(|(position, _)| format!("{position:?}"));
	pairs
}

fn member_owners(db: &Database, page: Uuid7) -> Vec<Value> {
	values(
		&root_query(
			db,
			"from uptime::status_page_monitors filter { status_page_id == $p } map { owner }",
			params(&[("p", page.into_value())]),
		),
		"owner",
	)
}

#[test]
fn owner_creates_a_page_it_owns_with_server_time() {
	// The page id is the caller's, but owner and created_at must never come from input.
	let db = build();
	let alice = new_user(&db, "alice");
	let id = new_id(&db);

	let before_ms = db.clock().now().to_nanos() / 1_000_000;
	let frames = command_as(&db, alice, CREATE, page_params(id, "my-status", "  My Status  ")).expect("create");
	let after_ms = db.clock().now().to_nanos() / 1_000_000;

	assert_eq!(column(&frames, "id"), id.into_value(), "create_status_page must return the id the caller passed");
	let row = page_row(&db, id);
	assert_eq!(column(&row, "owner"), Value::IdentityId(alice), "owner must be the caller");
	assert_eq!(column(&row, "slug"), text("my-status"));
	assert_eq!(column(&row, "title"), text("My Status"), "title must be trimmed");
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
fn another_owner_cannot_take_over_an_existing_page_id() {
	// Ids come from the client, so reusing a taken id must fail instead of creating a second row with it.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let id = create(&db, alice, "status");

	expect_error(command_as(&db, bob, CREATE, page_params(id, "bobs", "Bob")), "INDEX_001", "");

	let row = page_row(&db, id);
	assert_eq!(rows(&row), 1, "there must still be exactly one page with this id");
	assert_eq!(column(&row, "owner"), Value::IdentityId(alice), "the id must still belong to alice");
	assert_eq!(column(&row, "slug"), text("status"));
}

#[test]
fn user_cannot_forge_the_page_owner_by_inserting_directly() {
	// Owners hold insert rights for the procedures, so the insert policy itself must refuse foreign owners.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let page = create(&db, bob, "bobs");
	let bobs_monitor = monitor(&db, bob);

	expect_error(
		command_as(
			&db,
			alice,
			"INSERT uptime::status_pages [{ id: $id, owner: $owner, slug: \"planted\", title: \"x\", created_at: $now }]",
			params(&[
				("id", new_id(&db).into_value()),
				("owner", bob.into_value()),
				("now", db.clock().now().into_value()),
			]),
		),
		"POLICY_001",
		"denied insert",
	);
	expect_error(
		command_as(
			&db,
			alice,
			"INSERT uptime::status_page_monitors [{ status_page_id: $p, owner: $owner, monitor_id: $m, position: 0 }]",
			params(&[
				("p", page.into_value()),
				("owner", bob.into_value()),
				("m", bobs_monitor.into_value()),
			]),
		),
		"POLICY_001",
		"denied insert",
	);

	let pages = root_query(&db, "from uptime::status_pages map { id }", Params::None);
	assert_eq!(rows(&pages), 1, "no page must have been planted in bob's account");
	assert!(members(&db, page).is_empty(), "no member must have been planted on bob's page");
}

#[test]
fn guest_creates_a_page_it_owns() {
	// Guests own real rows, so the create call policy must admit them and stamp their identity.
	let db = build();
	let guest = new_guest(&db, "guest:one");

	let id = create(&db, guest, "guest-page");

	assert_eq!(column(&page_row(&db, id), "owner"), Value::IdentityId(guest));
}

#[test]
fn owner_updates_slug_and_title_only() {
	// An update must not move the page to another owner or reset its creation time.
	let db = build();
	let alice = new_user(&db, "alice");
	let id = create(&db, alice, "status");
	let created_at = column(&page_row(&db, id), "created_at");

	command_as(&db, alice, UPDATE, page_params(id, "renamed", "  Renamed  ")).expect("owner update");

	let row = page_row(&db, id);
	assert_eq!(column(&row, "slug"), text("renamed"));
	assert_eq!(column(&row, "title"), text("Renamed"), "title must be trimmed");
	assert_eq!(column(&row, "owner"), Value::IdentityId(alice));
	assert_eq!(column(&row, "created_at"), created_at, "created_at must not change");
}

#[test]
fn another_owner_cannot_update_a_page() {
	// Neither the procedure nor a direct UPDATE may rewrite a page owned by someone else.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let id = create(&db, alice, "status");

	expect_error(
		command_as(&db, bob, UPDATE, page_params(id, "hijacked", "Hijacked")),
		"ASSERT",
		"status page not found",
	);
	expect_error(
		command_as(
			&db,
			bob,
			"update uptime::status_pages { title: \"Hijacked\" } filter { id == $id }",
			id_params(id),
		),
		"POLICY_001",
		"denied update",
	);

	let row = page_row(&db, id);
	assert_eq!(column(&row, "slug"), text("status"), "alice's page must be untouched");
	assert_eq!(column(&row, "title"), text("Status"), "alice's page must be untouched");
}

#[test]
fn guest_updates_its_own_page() {
	// Guests must be able to edit pages they own.
	let db = build();
	let guest = new_guest(&db, "guest:one");
	let id = create(&db, guest, "guest-page");

	command_as(&db, guest, UPDATE, page_params(id, "guest-edit", "Guest edit")).expect("guest update");

	assert_eq!(column(&page_row(&db, id), "slug"), text("guest-edit"));
}

#[test]
fn owner_adds_its_monitors_as_members_it_owns() {
	// Member rows must carry the caller as owner, or the owner read policy hides them from the caller.
	let db = build();
	let alice = new_user(&db, "alice");
	let page = create(&db, alice, "status");
	let first = monitor(&db, alice);
	let second = monitor(&db, alice);

	add(&db, alice, page, &[second, first]);

	assert_eq!(
		members(&db, page),
		vec![(Value::Int2(0), second.into_value()), (Value::Int2(1), first.into_value())],
		"members must keep the positions the caller gave"
	);
	assert!(
		member_owners(&db, page).iter().all(|o| *o == Value::IdentityId(alice)),
		"members must be owned by alice"
	);
	let visible = query_as(&db, alice, "from uptime::status_page_monitors map { monitor_id }", Params::None)
		.expect("alice read");
	assert_eq!(rows(&visible), 2, "alice must see her members through the owner read policy");
}

#[test]
fn owner_cannot_put_a_monitor_it_does_not_own_on_its_page() {
	// A page must never publish another account's monitor status.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let page = create(&db, alice, "status");
	let bobs_monitor = monitor(&db, bob);

	expect_error(
		command_as(&db, alice, ADD_MEMBER, member_params(page, &[bobs_monitor])),
		"ASSERT",
		"unknown monitor id",
	);
	expect_error(
		command_as(&db, alice, ADD_MEMBER, member_params(page, &[new_id(&db)])),
		"ASSERT",
		"unknown monitor id",
	);

	assert!(members(&db, page).is_empty(), "no member must have been added");
}

#[test]
fn another_owner_cannot_add_members_to_a_page() {
	// Adding to someone else's page would let a stranger publish monitors under their slug.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let page = create(&db, alice, "status");
	let bobs_monitor = monitor(&db, bob);

	expect_error(
		command_as(&db, bob, ADD_MEMBER, member_params(page, &[bobs_monitor])),
		"ASSERT",
		"status page not found",
	);

	assert!(members(&db, page).is_empty(), "alice's page must stay empty");
}

#[test]
fn guest_adds_its_monitor_to_its_page() {
	// Guests must be able to build pages from monitors they own.
	let db = build();
	let guest = new_guest(&db, "guest:one");
	let page = create(&db, guest, "guest-page");
	let m = monitor(&db, guest);

	add(&db, guest, page, &[m]);

	assert_eq!(members(&db, page), vec![(Value::Int2(0), m.into_value())]);
}

#[test]
fn add_member_rejects_a_duplicate_and_the_101st_monitor() {
	// Without these checks a page could list a monitor twice or grow past the 100-monitor cap.
	let db = build();
	let alice = new_user(&db, "alice");
	let page = create(&db, alice, "status");
	let monitors: Vec<Uuid7> = (0..101).map(|_| monitor(&db, alice)).collect();

	add(&db, alice, page, &[monitors[0]]);
	expect_error(
		command_as(&db, alice, ADD_MEMBER, member_params(page, &[monitors[0]])),
		"ASSERT",
		"already on this status page",
	);
	assert_eq!(members(&db, page).len(), 1, "a duplicate must not insert a second row");

	for m in monitors.iter().take(100).skip(1) {
		add(&db, alice, page, &[*m]);
	}
	assert_eq!(members(&db, page).len(), 100, "exactly 100 members must be allowed");

	expect_error(
		command_as(&db, alice, ADD_MEMBER, member_params(page, &[monitors[100]])),
		"ASSERT",
		"at most 100 monitors",
	);
	assert_eq!(members(&db, page).len(), 100, "the 101st member must not be inserted");
}

#[test]
fn owner_clears_only_the_members_of_one_page() {
	// Clearing one page must never empty the owner's other pages.
	let db = build();
	let alice = new_user(&db, "alice");
	let page = create(&db, alice, "status");
	let other = create(&db, alice, "other");
	let m = monitor(&db, alice);
	add(&db, alice, page, &[m]);
	add(&db, alice, other, &[m]);

	command_as(&db, alice, CLEAR, page_id_params(page)).expect("owner clear");

	assert!(members(&db, page).is_empty(), "the cleared page must have no members");
	assert_eq!(members(&db, other), vec![(Value::Int2(0), m.into_value())], "the other page must keep its member");
	assert_eq!(rows(&page_row(&db, page)), 1, "clearing must not delete the page itself");
}

#[test]
fn another_owner_cannot_clear_a_page() {
	// Neither the procedure nor a direct DELETE may remove members from someone else's page.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let page = create(&db, alice, "status");
	let m = monitor(&db, alice);
	add(&db, alice, page, &[m]);

	expect_error(command_as(&db, bob, CLEAR, page_id_params(page)), "ASSERT", "status page not found");
	expect_error(
		command_as(
			&db,
			bob,
			"delete uptime::status_page_monitors filter { status_page_id == $status_page_id }",
			page_id_params(page),
		),
		"POLICY_001",
		"denied delete",
	);

	assert_eq!(members(&db, page), vec![(Value::Int2(0), m.into_value())], "alice's member must survive");
}

#[test]
fn guest_clears_its_own_page() {
	// Guests must be able to clear pages they own.
	let db = build();
	let guest = new_guest(&db, "guest:one");
	let page = create(&db, guest, "guest-page");
	add(&db, guest, page, &[monitor(&db, guest)]);

	command_as(&db, guest, CLEAR, page_id_params(page)).expect("guest clear");

	assert!(members(&db, page).is_empty());
}

#[test]
fn owner_deletes_one_page_with_its_members_only() {
	// A leftover member row would resurface if the page id were ever reused; the sibling page must survive intact.
	let db = build();
	let alice = new_user(&db, "alice");
	let page = create(&db, alice, "status");
	let other = create(&db, alice, "other");
	let m = monitor(&db, alice);
	add(&db, alice, page, &[m]);
	add(&db, alice, other, &[m]);

	command_as(&db, alice, DELETE, id_params(page)).expect("owner delete");

	assert_eq!(rows(&page_row(&db, page)), 0, "the page must be gone");
	assert!(members(&db, page).is_empty(), "its members must be gone");
	assert_eq!(rows(&page_row(&db, other)), 1, "the other page must survive");
	assert_eq!(members(&db, other), vec![(Value::Int2(0), m.into_value())], "the other page's member must survive");
	let monitors = root_query(&db, "from uptime::monitors filter { id == $id } map { id }", id_params(m));
	assert_eq!(rows(&monitors), 1, "deleting a page must not delete the monitors on it");
}

#[test]
fn another_owner_cannot_delete_a_page() {
	// A denied delete must fail loudly and leave the page and its members in place.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let page = create(&db, alice, "status");
	add(&db, alice, page, &[monitor(&db, alice)]);

	expect_error(command_as(&db, bob, DELETE, id_params(page)), "ASSERT", "status page not found");
	expect_error(
		command_as(&db, bob, "delete uptime::status_pages filter { id == $id }", id_params(page)),
		"POLICY_001",
		"denied delete",
	);

	assert_eq!(rows(&page_row(&db, page)), 1);
	assert_eq!(members(&db, page).len(), 1);
}

#[test]
fn guest_deletes_its_own_page() {
	// Guests must be able to delete pages they own, members included.
	let db = build();
	let guest = new_guest(&db, "guest:one");
	let page = create(&db, guest, "guest-page");
	add(&db, guest, page, &[monitor(&db, guest)]);

	command_as(&db, guest, DELETE, id_params(page)).expect("guest delete");

	assert_eq!(rows(&page_row(&db, page)), 0);
	assert!(members(&db, page).is_empty());
}

#[test]
fn another_owners_pages_and_members_are_invisible() {
	// The from-policies are the only thing keeping one account's pages out of another's queries and subscriptions.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let guest = new_guest(&db, "guest:one");
	let page = create(&db, alice, "status");
	add(&db, alice, page, &[monitor(&db, alice)]);

	for (who, id) in [("bob", bob), ("guest", guest)] {
		let pages =
			query_as(&db, id, "from uptime::status_pages map { id }", Params::None).expect("read pages");
		assert_eq!(rows(&pages), 0, "{who} must not see alice's page");
		let filtered =
			query_as(&db, id, "from uptime::status_pages filter { id == $id } map { id }", id_params(page))
				.expect("read page by id");
		assert_eq!(rows(&filtered), 0, "{who} must not see alice's page by id");
		let members = query_as(&db, id, "from uptime::status_page_monitors map { monitor_id }", Params::None)
			.expect("read members");
		assert_eq!(rows(&members), 0, "{who} must not see alice's members");
	}

	let own = query_as(&db, alice, "from uptime::status_pages map { id }", Params::None).expect("alice read");
	assert_eq!(values(&own, "id"), vec![page.into_value()], "alice must see exactly her page");
}

#[test]
fn slug_is_unique_per_owner_but_not_across_owners() {
	// Two pages with one slug in one account would make the public page ambiguous; other accounts are independent.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let first = create(&db, alice, "status");
	let second = create(&db, alice, "other");

	let dup = new_id(&db);
	expect_error(
		command_as(&db, alice, CREATE, page_params(dup, "status", "Dup")),
		"ASSERT",
		"this slug is already taken",
	);
	assert_eq!(rows(&page_row(&db, dup)), 0, "a rejected create must not insert");

	expect_error(
		command_as(&db, alice, UPDATE, page_params(second, "status", "Other")),
		"ASSERT",
		"this slug is already taken",
	);
	assert_eq!(column(&page_row(&db, second), "slug"), text("other"), "a rejected update must not write");

	command_as(&db, alice, UPDATE, page_params(first, "status", "Renamed"))
		.expect("keeping its own slug must pass");
	create(&db, bob, "status");
}

#[test]
fn invalid_title_or_slug_is_rejected_by_create_and_update() {
	// Each check must fail both procedures with its message and never write.
	let db = build();
	let alice = new_user(&db, "alice");
	let existing = create(&db, alice, "status");
	let long_title = "t".repeat(201);
	let long_slug = "s".repeat(65);
	let cases = [
		("status", "   ", "title must be between 1 and 200 characters"),
		("status", long_title.as_str(), "title must be between 1 and 200 characters"),
		("", "Title", "slug must be between 1 and 64 characters"),
		(long_slug.as_str(), "Title", "slug must be between 1 and 64 characters"),
		("-status", "Title", "slug must not start with a hyphen"),
	];

	for (slug, title, message) in cases {
		let id = new_id(&db);
		expect_error(command_as(&db, alice, CREATE, page_params(id, slug, title)), "ASSERT", message);
		assert_eq!(rows(&page_row(&db, id)), 0, "a rejected create must not insert ({message})");

		expect_error(command_as(&db, alice, UPDATE, page_params(existing, slug, title)), "ASSERT", message);
		let row = page_row(&db, existing);
		assert_eq!(column(&row, "slug"), text("status"), "a rejected update must not write ({message})");
		assert_eq!(column(&row, "title"), text("Status"), "a rejected update must not write ({message})");
	}
}

#[test]
fn boundary_title_and_slug_are_accepted() {
	// Off-by-one in the length checks would reject pages the HTTP route accepted.
	let db = build();
	let alice = new_user(&db, "alice");
	let title = "t".repeat(200);
	let slug = "s".repeat(64);

	for (slug, title) in [(slug.as_str(), "Title"), ("a", title.as_str()), ("status-", "Title"), ("0-9", "T")] {
		let id = new_id(&db);
		command_as(&db, alice, CREATE, page_params(id, slug, title))
			.unwrap_or_else(|e| panic!("create with slug {slug:?} must succeed: {e:?}"));
		command_as(&db, alice, UPDATE, page_params(id, slug, title))
			.unwrap_or_else(|e| panic!("update with slug {slug:?} must succeed: {e:?}"));
		assert_eq!(rows(&page_row(&db, id)), 1);
	}
}

#[test]
fn a_page_edit_in_one_command_replaces_members_or_changes_nothing() {
	// The web form saves with one multi-CALL command; a failure part-way must not leave the page half-edited.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let page = create(&db, alice, "status");
	let old = monitor(&db, alice);
	let first = monitor(&db, alice);
	let second = monitor(&db, alice);
	add(&db, alice, page, &[old]);
	let save = "CALL uptime::update_status_page($id, $slug, $title); \
		 CALL uptime::clear_status_page_monitors($id); \
		 CALL uptime::add_status_page_monitors($id, $monitor_ids)";
	let save_params = |m1: Uuid7| {
		params(&[
			("id", page.into_value()),
			("slug", text("renamed")),
			("title", text("Renamed")),
			("monitor_ids", Value::List(vec![second.into_value(), m1.into_value()])),
		])
	};

	expect_error(command_as(&db, alice, save, save_params(monitor(&db, bob))), "ASSERT", "unknown monitor id");
	assert_eq!(column(&page_row(&db, page), "slug"), text("status"), "the failed save must not rename the page");
	assert_eq!(
		members(&db, page),
		vec![(Value::Int2(0), old.into_value())],
		"the failed save must keep old members"
	);

	command_as(&db, alice, save, save_params(first)).expect("save");
	assert_eq!(column(&page_row(&db, page), "slug"), text("renamed"));
	assert_eq!(
		members(&db, page),
		vec![(Value::Int2(0), second.into_value()), (Value::Int2(1), first.into_value())],
		"the save must replace the members in the given order"
	);
}

#[test]
fn service_is_denied_every_status_page_procedure() {
	// Probe tokens must never be enough to create, edit or delete a user's page.
	let db = build();
	let alice = new_user(&db, "alice");
	admin(&db, "CREATE SERVICE probe_svc");
	let service = lookup_identity(&db, "probe_svc");
	let page = create(&db, alice, "status");
	let m = monitor(&db, alice);
	add(&db, alice, page, &[m]);

	expect_error(
		command_as(&db, service, CREATE, page_params(new_id(&db), "svc", "Svc")),
		"POLICY_001",
		"denied call",
	);
	expect_error(command_as(&db, service, UPDATE, page_params(page, "svc", "Svc")), "POLICY_001", "denied call");
	expect_error(command_as(&db, service, ADD_MEMBER, member_params(page, &[m])), "POLICY_001", "denied call");
	expect_error(command_as(&db, service, CLEAR, page_id_params(page)), "POLICY_001", "denied call");
	expect_error(command_as(&db, service, DELETE, id_params(page)), "POLICY_001", "denied call");

	let pages = root_query(&db, "from uptime::status_pages map { slug }", Params::None);
	assert_eq!(values(&pages, "slug"), vec![text("status")], "no page must be created or changed");
	assert_eq!(members(&db, page), vec![(Value::Int2(0), m.into_value())]);
}

#[test]
fn direct_update_cannot_take_another_owners_page() {
	// The update policy must hold on the old row, or rewriting owner hands any page and its slug to the caller.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let id = create(&db, alice, "status");

	for filter in ["filter { id == $id }", "filter { true }"] {
		expect_error(
			command_as(
				&db,
				bob,
				&format!("update uptime::status_pages {{ owner: $identity.id }} {filter}"),
				id_params(id),
			),
			"POLICY_001",
			"denied update",
		);
	}

	assert_eq!(column(&page_row(&db, id), "owner"), Value::IdentityId(alice), "the page must still be alice's");
}

#[test]
fn direct_update_cannot_give_a_page_to_another_owner() {
	// The update policy must hold on the new row, or rewriting owner plants a page in another account.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let id = create(&db, alice, "status");

	expect_error(
		command_as(
			&db,
			alice,
			"update uptime::status_pages { owner: $owner } filter { id == $id }",
			params(&[("id", id.into_value()), ("owner", bob.into_value())]),
		),
		"POLICY_001",
		"denied update",
	);

	assert_eq!(column(&page_row(&db, id), "owner"), Value::IdentityId(alice), "the page must still be alice's");
}

#[test]
fn direct_update_cannot_move_members_between_owners() {
	// Member rows publish monitors, so a direct UPDATE must never take or give one away.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let page = create(&db, alice, "status");
	add(&db, alice, page, &[monitor(&db, alice)]);
	let reassign = "update uptime::status_page_monitors { owner: $owner } filter { status_page_id == $id }";

	for (caller, new_owner) in [(bob, bob), (alice, bob)] {
		expect_error(
			command_as(
				&db,
				caller,
				reassign,
				params(&[("id", page.into_value()), ("owner", new_owner.into_value())]),
			),
			"POLICY_002",
			"No update policy defined for update on uptime::status_page_monitors",
		);
	}

	assert_eq!(member_owners(&db, page), vec![Value::IdentityId(alice)], "the member must still be alice's");
}

#[test]
fn member_check_fails_until_the_page_has_a_member_of_its_own() {
	// The web ends every page save with this check, so a page without its own member must fail it.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	let page = create(&db, alice, "status");
	command_as(
		&db,
		bob,
		"INSERT uptime::status_page_monitors [{ status_page_id: $p, owner: $owner, monitor_id: $m, position: 0 }]",
		params(&[("p", page.into_value()), ("owner", bob.into_value()), ("m", monitor(&db, bob).into_value())]),
	)
	.expect("the insert policy admits a member row the caller owns");

	expect_error(
		command_as(&db, alice, CHECK_MEMBERS, page_id_params(page)),
		"ASSERT",
		"a status page must contain at least one monitor",
	);

	add(&db, alice, page, &[monitor(&db, alice)]);
	command_as(&db, alice, CHECK_MEMBERS, page_id_params(page)).expect("one member must pass the check");
}

#[test]
fn another_owner_or_a_service_cannot_run_the_member_check() {
	// A foreign page must look exactly like a missing one, and probe tokens must never reach owner procedures.
	let db = build();
	let alice = new_user(&db, "alice");
	let bob = new_user(&db, "bob");
	admin(&db, "CREATE SERVICE probe_svc");
	let service = lookup_identity(&db, "probe_svc");
	let page = create(&db, alice, "status");
	add(&db, alice, page, &[monitor(&db, alice)]);

	expect_error(command_as(&db, bob, CHECK_MEMBERS, page_id_params(page)), "ASSERT", "status page not found");
	expect_error(command_as(&db, service, CHECK_MEMBERS, page_id_params(page)), "POLICY_001", "denied call");
}

#[test]
fn guest_passes_the_member_check_on_its_own_page() {
	// Guests save pages through the same command, so the check's call policy must admit them.
	let db = build();
	let guest = new_guest(&db, "guest:one");
	let page = create(&db, guest, "guest-page");
	add(&db, guest, page, &[monitor(&db, guest)]);

	command_as(&db, guest, CHECK_MEMBERS, page_id_params(page)).expect("guest check");
}

#[test]
fn a_page_command_ending_in_the_member_check_rolls_back_without_members() {
	// The check is the last CALL of a save, so a zero count must undo every earlier CALL in the command.
	let db = build();
	let alice = new_user(&db, "alice");
	let check = "CALL uptime::check_status_page_monitors($id)";

	let created = new_id(&db);
	expect_error(
		command_as(&db, alice, &format!("{CREATE}; {check}"), page_params(created, "status", "Status")),
		"ASSERT",
		"a status page must contain at least one monitor",
	);
	assert_eq!(rows(&page_row(&db, created)), 0, "a create without members must be rolled back");

	let page = create(&db, alice, "status");
	let m = monitor(&db, alice);
	add(&db, alice, page, &[m]);
	expect_error(
		command_as(
			&db,
			alice,
			&format!("{UPDATE}; CALL uptime::clear_status_page_monitors($id); {check}"),
			page_params(page, "renamed", "Renamed"),
		),
		"ASSERT",
		"a status page must contain at least one monitor",
	);
	assert_eq!(column(&page_row(&db, page), "slug"), text("status"), "the edit must be rolled back");
	assert_eq!(members(&db, page), vec![(Value::Int2(0), m.into_value())], "the cleared member must be restored");
}

#[test]
fn padded_title_is_measured_after_trimming() {
	// Titles are stored trimmed, so the length limit must apply to the trimmed text, not the padding.
	let db = build();
	let alice = new_user(&db, "alice");
	let id = new_id(&db);
	let title = format!("  {}  ", "t".repeat(200));

	command_as(&db, alice, CREATE, page_params(id, "status", &title)).expect("padded create");
	command_as(&db, alice, UPDATE, page_params(id, "status", &title)).expect("padded update");

	assert_eq!(column(&page_row(&db, id), "title"), text(&"t".repeat(200)));
}

#[test]
fn slug_with_a_character_outside_lowercase_letters_digits_and_hyphens_is_rejected() {
	// Every character must be checked, not only the first, or a URL-unsafe slug reaches the public path.
	let db = build();
	let alice = new_user(&db, "alice");
	let existing = create(&db, alice, "status");
	let message = "slug must contain only lowercase letters, digits, and hyphens";

	for slug in ["Status", "stAtus", "status_", "sta tus", "a.b", "a/b", "caf\u{e9}", "\u{430}bc"] {
		let id = new_id(&db);
		expect_error(command_as(&db, alice, CREATE, page_params(id, slug, "Title")), "ASSERT", message);
		assert_eq!(rows(&page_row(&db, id)), 0, "a rejected create must not insert ({slug:?})");

		expect_error(command_as(&db, alice, UPDATE, page_params(existing, slug, "Title")), "ASSERT", message);
		let row = page_row(&db, existing);
		assert_eq!(column(&row, "slug"), text("status"), "a rejected update must not write ({slug:?})");
	}
}
