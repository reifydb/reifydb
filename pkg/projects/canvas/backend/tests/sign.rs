// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{
	Database, IdentityId, WithSubsystem,
	auth::service::AuthResponse,
	server,
	value::{params::Params, value::frame::frame::Frame},
};
use reifydb_canvas::{migration_path, routines};

fn build() -> Database {
	server::memory()
		.with_flow(|f| f)
		.with_routines(routines)
		.with_migrations(migration_path())
		.build()
		.expect("build memory db")
}

fn agent(db: &Database, name: &str) -> IdentityId {
	let credentials = [("identifier", name), ("password", "x")].iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
	match db.auth_service().authenticate_as(IdentityId::anonymous(), "password", credentials).expect("login") {
		AuthResponse::Authenticated {
			identity,
			..
		} => identity,
		other => panic!("password login did not authenticate: {other:?}"),
	}
}

fn call(db: &Database, identity: IdentityId, rql: &str) -> Result<(), String> {
	let r = db.engine().command_as(identity, rql, Params::None);
	match r.error {
		Some(e) => Err(format!("{} {}", e.code, e.message)),
		None => Ok(()),
	}
}

fn query(db: &Database, identity: IdentityId, rql: &str) -> Vec<Frame> {
	let r = db.engine().query_as(identity, rql, Params::None);
	if let Some(e) = r.error {
		panic!("query failed for [{rql}]: {e:?}");
	}
	r.frames
}

fn signs(db: &Database, identity: IdentityId) -> Vec<(String, String, String)> {
	query(db, identity, "from canvas::sign map { name, topic, joke }")
		.iter()
		.flat_map(|f| f.rows())
		.map(|r| {
			(
				r.get::<String>("name").unwrap().unwrap(),
				r.get::<String>("topic").unwrap().unwrap(),
				r.get::<String>("joke").unwrap().unwrap(),
			)
		})
		.collect()
}

#[test]
fn a_user_records_a_sign_and_anyone_can_read_it() {
	// Agents read the log anonymously before choosing, so a recorded topic must be visible to every identity.
	let db = build();
	let a = agent(&db, "agent-a");
	call(&db, a, r#"CALL canvas::record_sign("freezer ransom", "KEEP FROZEN", 10, 20, 192, 128)"#).expect("record");
	let expected = vec![("agent-a".to_string(), "freezer ransom".to_string(), "KEEP FROZEN".to_string())];
	assert_eq!(signs(&db, IdentityId::anonymous()), expected);
	assert_eq!(signs(&db, a), expected);
}

#[test]
fn an_anonymous_caller_cannot_record_a_sign() {
	// Anonymous viewers must not be able to fill the log with topics nobody painted.
	let db = build();
	let err = call(&db, IdentityId::anonymous(), r#"CALL canvas::record_sign("t", "j", 0, 0, 192, 128)"#)
		.expect_err("anonymous is refused");
	assert!(!err.is_empty());
	assert!(signs(&db, IdentityId::root()).is_empty(), "a refused record writes nothing");
}

#[test]
fn an_empty_or_too_long_topic_or_joke_is_refused_and_writes_nothing() {
	// An empty topic tells later agents nothing, and an unbounded one blows up every signs() read.
	let db = build();
	let a = agent(&db, "agent-a");
	let long = "x".repeat(141);
	for (topic, joke, needle) in [
		("", "j", "topic must be 1 to 140 characters"),
		(long.as_str(), "j", "topic must be 1 to 140 characters"),
		("t", "", "joke must be 1 to 140 characters"),
		("t", long.as_str(), "joke must be 1 to 140 characters"),
	] {
		let err = call(&db, a, &format!(r#"CALL canvas::record_sign("{topic}", "{joke}", 0, 0, 192, 128)"#))
			.expect_err("out of range is refused");
		assert!(err.contains(needle), "{err}");
	}
	let edge = "y".repeat(140);
	call(&db, a, &format!(r#"CALL canvas::record_sign("{edge}", "{edge}", 0, 0, 192, 128)"#)).expect("140 is allowed");
	assert_eq!(signs(&db, IdentityId::root()).len(), 1, "only the in-range record is stored");
}

#[test]
fn a_user_cannot_insert_into_the_sign_log_directly() {
	// Only the procedure checks the limits, so a direct insert would bypass them.
	let db = build();
	let a = agent(&db, "agent-a");
	let err = call(
		&db,
		a,
		r#"insert canvas::sign [{ owner: $identity.id, name: "x", topic: "t", joke: "j", x: 0, y: 0, w: 1, h: 1 }]"#,
	)
	.expect_err("direct insert is refused");
	assert!(!err.is_empty());
	assert!(signs(&db, IdentityId::root()).is_empty());
}

#[test]
fn repainting_the_same_cell_updates_its_log_entry_instead_of_adding_one() {
	// Each fix after look() logged the sign again, so signs() filled with copies and agents stopped fixing flaws.
	let db = build();
	let a = agent(&db, "agent-a");
	call(&db, a, r#"CALL canvas::record_sign("freezer ransom", "KEEP FROZEN", 10, 20, 192, 128)"#).expect("first paint");
	call(&db, a, r#"CALL canvas::record_sign("freezer ransom", "KEEP FROZEN UNTIL PAID", 10, 20, 192, 128)"#).expect("repaint");
	call(&db, a, r#"CALL canvas::record_sign("ram prices", "DONATE RAM", 10, 20, 192, 160)"#).expect("another cell");
	let mut got = signs(&db, IdentityId::root());
	got.sort();
	assert_eq!(
		got,
		vec![
			("agent-a".to_string(), "freezer ransom".to_string(), "KEEP FROZEN UNTIL PAID".to_string()),
			("agent-a".to_string(), "ram prices".to_string(), "DONATE RAM".to_string()),
		]
	);
}
