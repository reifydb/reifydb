// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{
	Database, IdentityId, Value, WithSubsystem,
	auth::service::AuthResponse,
	server,
	value::{params::Params, value::blob::Blob},
};
use reifydb_place::{TILE_BYTES, encode_tile, migration_path, routines, tile_key};

fn build() -> Database {
	server::memory()
		.with_flow(|f| f)
		.with_routines(routines)
		.with_migrations(migration_path())
		.build()
		.expect("build memory db")
}

fn agent(db: &Database) -> IdentityId {
	let credentials = [("identifier", "agent-a"), ("password", "x")]
		.iter()
		.map(|(k, v)| (k.to_string(), v.to_string()))
		.collect();
	match db.auth_service().authenticate_as(IdentityId::anonymous(), "password", credentials).expect("login") {
		AuthResponse::Authenticated {
			identity,
			..
		} => identity,
		other => panic!("password login did not authenticate: {other:?}"),
	}
}

fn run(db: &Database, identity: IdentityId, rql: &str) -> Option<String> {
	let mut map = HashMap::new();
	map.insert("pixels".to_string(), Value::Blob(Blob::new(encode_tile(&[5u8; TILE_BYTES]))));
	db.engine().command_as(identity, rql, Params::from(map)).error.map(|e| e.code.clone())
}

fn rows(db: &Database, rql: &str) -> usize {
	let r = db.engine().query_as(IdentityId::root(), rql, Params::None);
	assert!(r.error.is_none(), "root query failed: {rql}");
	r.frames.iter().map(|f| f.row_count()).sum()
}

#[test]
fn an_agent_cannot_write_place_tables_without_the_procedure() {
	// Every direct write must be refused, so the cap, cooldown and note in the draw procedures cannot be skipped.
	let db = build();
	let root = IdentityId::root();
	for rql in [
		format!("insert place::tile [{{ tile: {}, pixels: $pixels }}]", tile_key(0, 0)),
		"insert place::state [{ key: \"canvas\", ready: true }]".to_string(),
		"insert place::edit [{ owner: identity::id(), name: \"seed\", tile: 0, pixels: $pixels, painted: 1 }]"
			.to_string(),
	] {
		assert_eq!(run(&db, root, &rql), None, "setup must succeed: {rql}");
	}
	let agent = agent(&db);

	for rql in [
		"update place::tile { pixels: $pixels } filter { true }",
		"insert place::edit [{ owner: $identity.id, name: \"fake\", tile: 0, pixels: $pixels, painted: 1 }]",
		"delete place::tile filter { true }",
		"delete place::edit filter { true }",
		"insert place::tile [{ tile: 1, pixels: $pixels }]",
	] {
		assert_eq!(run(&db, agent, rql).as_deref(), Some("POLICY_002"), "direct write must be refused: {rql}");
	}
	assert_eq!(rows(&db, "from place::tile"), 1, "no tile was added or removed");
	assert_eq!(rows(&db, "from place::edit"), 1, "no edit was added or removed");
}
