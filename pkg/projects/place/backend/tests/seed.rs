// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{
	Database, IdentityId, Value, WithSubsystem, server,
	value::{params::Params, value::blob::Blob},
};
use reifydb_place::{TILE_BYTES, decode_tile, encode_tile, migration_path, routines, seed};

fn build() -> Database {
	server::memory()
		.with_flow(|f| f)
		.with_routines(routines)
		.with_migrations(migration_path())
		.build()
		.expect("build memory db")
}

fn count(db: &Database, rql: &str) -> i64 {
	let r = db.engine().query_as(IdentityId::root(), rql, Params::None);
	if let Some(e) = r.error {
		panic!("count failed for [{rql}]: {e:?}");
	}
	r.frames.first().and_then(|f| f.rows().next()).map(|row| row.get::<i64>("n").unwrap().unwrap()).unwrap_or(0)
}

fn stored(db: &Database, tx: u8, ty: u8) -> Vec<u8> {
	let rql = format!("from place::tile filter {{ tile_x == {tx} and tile_y == {ty} }} map {{ tile_x, pixels }}");
	let r = db.engine().query_as(IdentityId::root(), &rql, Params::None);
	if let Some(e) = r.error {
		panic!("reading tile ({tx},{ty}) failed: {e:?}");
	}
	let row = r.frames.first().and_then(|f| f.rows().next()).expect("the tile row");
	row.get::<Blob>("pixels").unwrap().unwrap().as_bytes().to_vec()
}

fn tile(db: &Database, tx: u8, ty: u8) -> Vec<u8> {
	decode_tile(&stored(db, tx, ty)).expect("a stored tile decodes")
}

fn root(db: &Database, rql: &str, params: Params) {
	let r = db.engine().command_as(IdentityId::root(), rql, params);
	assert!(r.error.is_none(), "[{rql}]: {:?}", r.error);
}

fn blob_param(bytes: Vec<u8>) -> Params {
	let mut map = HashMap::new();
	map.insert("pixels".to_string(), Value::Blob(Blob::new(bytes)));
	Params::from(map)
}

#[test]
fn seed_fills_every_tile_white_once_and_refuses_a_damaged_tile() {
	// One seeded db serves all checks so every check reads the same state the binary boots into.
	let db = build();
	seed(&db).expect("first seed");

	// Every tile of the 8192 x 8192 canvas must exist, or a commit to it silently updates zero rows.
	let tiles = "from place::tile aggregate { n: math::count(tile_x) }";
	assert_eq!(count(&db, tiles), 16_384);

	// A tile must hold exactly 64 x 64 one-byte pixels, all white, at both corners of the canvas.
	assert_eq!(tile(&db, 1, 2), vec![0u8; TILE_BYTES]);
	assert_eq!(tile(&db, 127, 127), vec![0u8; TILE_BYTES]);
	assert!(stored(&db, 1, 2).len() < 64, "a blank tile must be stored compressed, not as 4,096 raw bytes");

	// Commits stay refused until the seed marks the canvas ready, so the flag must be set exactly once.
	let ready =
		"from place::state filter { key == \"canvas\" and ready == true } aggregate { n: math::count(key) }";
	assert_eq!(count(&db, ready), 1);

	// A restart must not duplicate tiles, or one commit would update two rows of the same tile.
	seed(&db).expect("second seed");
	assert_eq!(count(&db, tiles), 16_384);
	assert_eq!(count(&db, ready), 1);

	// A tile stored twice means a broken seed and must stop the boot, never be skipped.
	root(
		&db,
		"insert place::tile [{ tile_x: 3, tile_y: 3, pixels: $pixels }]",
		blob_param(encode_tile(&[0u8; TILE_BYTES])),
	);
	assert_eq!(seed(&db).unwrap_err(), "tile (3,3) is stored more than once");

	// A tile of the wrong size would make paint fail on every commit to it, so the boot must refuse it.
	root(&db, "delete place::tile filter { tile_x == 3 and tile_y == 3 }", Params::None);
	root(
		&db,
		"insert place::tile [{ tile_x: 3, tile_y: 3, pixels: $pixels }]",
		blob_param(encode_tile(&[0u8; 10])),
	);
	assert_eq!(seed(&db).unwrap_err(), "tile (3,3): tile decodes to 10 bytes, expected 4096");

	// A blob that is not a compressed tile at all must also stop the boot.
	root(&db, "delete place::tile filter { tile_x == 3 and tile_y == 3 }", Params::None);
	root(&db, "insert place::tile [{ tile_x: 3, tile_y: 3, pixels: $pixels }]", blob_param(vec![0xff; 16]));
	assert!(seed(&db).unwrap_err().starts_with("tile (3,3): tile does not decode"));
}
