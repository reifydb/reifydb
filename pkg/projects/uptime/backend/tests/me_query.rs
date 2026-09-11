// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{
	Database, IdentityId, Value, WithSubsystem, server,
	value::{
		params::Params,
		value::{frame::frame::Frame, identity::IdentityKind},
	},
};
use reifydb_uptime::migration_path;

const ME: &str = "map { id: $identity.id, name: $identity.name, kind: $identity.kind }";

fn build() -> Database {
	server::memory().with_flow(|f| f).with_migrations(migration_path()).build().expect("build memory db")
}

fn params(entries: &[(&str, Value)]) -> Params {
	let map: HashMap<String, Value> = entries.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect();
	Params::from(map)
}

fn column(frames: &[Frame], name: &str) -> Value {
	let frame = frames.first().expect("frame");
	assert_eq!(frame.row_count(), 1, "expected exactly one row when reading column {name}");
	frame.columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("column {name}")).data.get_value(0)
}

fn column_names(frames: &[Frame]) -> Vec<&str> {
	frames.first().expect("frame").columns.iter().map(|c| c.name.as_str()).collect()
}

fn me(db: &Database, id: IdentityId) -> Vec<Frame> {
	let r = db.engine().query_as(id, ME, Params::None);
	if let Some(e) = r.error {
		panic!("me query failed: {e:?}");
	}
	r.frames
}

fn lookup_identity(db: &Database, name: &str) -> IdentityId {
	let r = db.engine().query_as(
		IdentityId::root(),
		"from system::identities filter { name == $name } map { id }",
		params(&[("name", Value::Utf8(name.to_string()))]),
	);
	if let Some(e) = r.error {
		panic!("identity lookup failed: {e:?}");
	}
	match column(&r.frames, "id") {
		Value::IdentityId(id) => id,
		other => panic!("unexpected identity value for {name}: {other:?}"),
	}
}

fn sign_up(db: &Database, email: &str) -> IdentityId {
	let r = db.engine().admin_as(
		IdentityId::root(),
		&format!("CREATE USER `{email}` {{ email: $email }}"),
		params(&[("email", Value::Utf8(email.to_string()))]),
	);
	if let Some(e) = r.error {
		panic!("sign up failed: {e:?}");
	}
	lookup_identity(db, email)
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

#[test]
fn me_returns_the_calling_user_named_by_its_email() {
	// The web app takes the email from the name, so the query must never name anyone but the caller.
	let db = build();
	let alice = sign_up(&db, "alice@example.com");
	sign_up(&db, "bob@example.com");

	let frames = me(&db, alice);

	assert_eq!(column_names(&frames), ["id", "name", "kind"]);
	assert_eq!(column(&frames, "id"), Value::IdentityId(alice));
	assert_eq!(column(&frames, "name"), Value::Utf8("alice@example.com".to_string()));
	assert_eq!(column(&frames, "kind"), Value::Utf8("user".to_string()));
}

#[test]
fn me_marks_a_guest_by_its_kind() {
	// Without the guest kind, the web app reads a guest's name as an email and hides the sign-up prompt.
	let db = build();
	let guest = new_guest(&db);

	let frames = me(&db, guest);

	assert_eq!(column_names(&frames), ["id", "name", "kind"]);
	assert_eq!(column(&frames, "id"), Value::IdentityId(guest));
	assert_eq!(column(&frames, "name"), Value::Utf8("guest:visitor".to_string()));
	assert_eq!(column(&frames, "kind"), Value::Utf8("guest".to_string()));
}
