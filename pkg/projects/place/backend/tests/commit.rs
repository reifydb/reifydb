// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{
	Database, IdentityId, Value, WithSubsystem, server,
	value::{
		params::Params,
		value::{blob::Blob, frame::frame::Frame},
	},
};
use reifydb_place::{TILE_BYTES, decode_tile, encode_edit, encode_tile, migration_path, routines, tile_key};

fn build() -> Database {
	server::memory()
		.with_flow(|f| f)
		.with_routines(routines)
		.with_migrations(migration_path())
		.build()
		.expect("build memory db")
}

fn root_query(db: &Database, rql: &str) -> Vec<Frame> {
	let r = db.engine().query_as(IdentityId::root(), rql, Params::None);
	if let Some(e) = r.error {
		panic!("root query failed for [{rql}]: {e:?}");
	}
	r.frames
}

fn seed_tile(db: &Database, tx: u8, ty: u8) {
	let mut map = HashMap::new();
	map.insert("pixels".to_string(), Value::Blob(Blob::new(encode_tile(&[0u8; TILE_BYTES]))));
	let rql = format!("insert place::tile [{{ tile: {}, pixels: $pixels }}]", tile_key(tx, ty));
	let r = db.engine().command_as(IdentityId::root(), &rql, Params::from(map));
	if let Some(e) = r.error {
		panic!("seeding tile ({tx},{ty}) failed: {e:?}");
	}
}

fn mark_ready(db: &Database) {
	let r = db.engine().command_as(
		IdentityId::root(),
		"insert place::state [{ key: \"canvas\", ready: true }]",
		Params::None,
	);
	if let Some(e) = r.error {
		panic!("marking the canvas ready failed: {e:?}");
	}
}

fn tiles(db: &Database) -> i64 {
	let frames = root_query(db, "from place::tile aggregate { n: math::count(tile) }");
	frames.first().and_then(|f| f.rows().next()).map(|r| r.get::<i64>("n").unwrap().unwrap()).unwrap_or(0)
}

fn tile(db: &Database, tx: u8, ty: u8) -> Vec<u8> {
	let frames = root_query(
		db,
		&format!("from place::tile filter {{ tile == {} }} map {{ tile, pixels }}", tile_key(tx, ty)),
	);
	let rows: Vec<Vec<u8>> = frames
		.iter()
		.flat_map(|f| f.rows())
		.map(|r| {
			decode_tile(r.get::<Blob>("pixels").unwrap().unwrap().as_bytes())
				.expect("a stored tile decodes")
		})
		.collect();
	assert_eq!(rows.len(), 1, "tile ({tx},{ty}) must be stored exactly once");
	rows.into_iter().next().unwrap()
}

fn pixel(db: &Database, x: u16, y: u16) -> u8 {
	tile(db, (x / 64) as u8, (y / 64) as u8)[(y as usize % 64) * 64 + x as usize % 64]
}

fn pack(x: u64, y: u64, color: u64) -> (u64, u64, u64) {
	(x, y, color)
}

fn edits(pixels: &[(u64, u64, u64)]) -> Vec<Value> {
	let mut tiles: Vec<((u8, u8), Vec<(usize, u8)>)> = Vec::new();
	for &(x, y, color) in pixels {
		let key = ((x / 64) as u8, (y / 64) as u8);
		let pixel = (((y % 64) * 64 + x % 64) as usize, color as u8);
		match tiles.iter_mut().find(|(tile, _)| *tile == key) {
			Some((_, list)) => list.push(pixel),
			None => tiles.push((key, vec![pixel])),
		}
	}
	tiles.into_iter().map(|((tx, ty), list)| Value::Blob(Blob::new(encode_edit(tx, ty, &list)))).collect()
}

fn commit(db: &Database, pixels: Vec<(u64, u64, u64)>) -> Result<(), String> {
	commit_edits(db, edits(&pixels))
}

fn commit_edits(db: &Database, tiles: Vec<Value>) -> Result<(), String> {
	let mut map = HashMap::new();
	map.insert("tiles".to_string(), Value::List(tiles));
	let r = db.engine().command_as(IdentityId::anonymous(), "CALL place::commit($tiles)", Params::from(map));
	match r.error {
		Some(e) => Err(format!("{} {}", e.code, e.message)),
		None => Ok(()),
	}
}

fn anonymous(db: &Database, rql: &str) -> Result<Vec<Frame>, String> {
	let r = db.engine().command_as(IdentityId::anonymous(), rql, Params::None);
	match r.error {
		Some(e) => Err(format!("{} {}", e.code, e.message)),
		None => Ok(r.frames),
	}
}

#[test]
fn a_commit_recolors_exactly_its_pixels() {
	// A commit must change the named bytes and nothing else, or one pixel's paint smears into its neighbours.
	let db = build();
	seed_tile(&db, 0, 0);
	mark_ready(&db);
	commit(&db, vec![pack(1, 1, 12), pack(2, 1, 13), pack(3, 1, 14)]).expect("commit");
	let mut expected = vec![0u8; TILE_BYTES];
	expected[65] = 12;
	expected[66] = 13;
	expected[67] = 14;
	assert_eq!(tile(&db, 0, 0), expected);
	assert_eq!(tiles(&db), 1, "a commit must never add a tile row");
}

#[test]
fn repainting_a_pixel_keeps_the_last_color() {
	// Two commits to one pixel must leave the second color, or the canvas shows a stale pixel to every subscriber.
	let db = build();
	seed_tile(&db, 0, 0);
	mark_ready(&db);
	commit(&db, vec![pack(5, 5, 10)]).expect("first commit");
	commit(&db, vec![pack(5, 5, 11)]).expect("second commit");
	assert_eq!(pixel(&db, 5, 5), 11);
	assert_eq!(tiles(&db), 1);
}

#[test]
fn more_than_4096_pixels_are_refused_whole() {
	// The cap is the only bound on one anonymous write, so going over it must write nothing at all.
	let db = build();
	seed_tile(&db, 0, 0);
	seed_tile(&db, 0, 1);
	mark_ready(&db);
	let pixels = (0..4097u64).map(|i| pack(i % 64, i / 64, 7)).collect();
	let err = commit(&db, pixels).unwrap_err();
	assert!(err.starts_with("ASSERT"), "{err}");
	assert_eq!(tile(&db, 0, 0), vec![0u8; TILE_BYTES]);
	assert_eq!(tile(&db, 0, 1), vec![0u8; TILE_BYTES]);
}

#[test]
fn exactly_4096_pixels_are_accepted() {
	// The cap must be inclusive, or the client's 4,096-pixel limit produces commits the server rejects.
	let db = build();
	seed_tile(&db, 0, 0);
	seed_tile(&db, 0, 1);
	mark_ready(&db);
	let pixels = (0..4096u64).map(|i| pack(i % 64, 32 + i / 64, 8)).collect();
	commit(&db, pixels).expect("4,096 pixels");
	let upper = tile(&db, 0, 0);
	let lower = tile(&db, 0, 1);
	assert!(upper[..2048].iter().all(|&b| b == 0), "pixels above the commit must stay white");
	assert!(upper[2048..].iter().all(|&b| b == 8), "every committed pixel must be painted");
	assert!(lower[..2048].iter().all(|&b| b == 8), "every committed pixel must be painted");
	assert!(lower[2048..].iter().all(|&b| b == 0), "pixels past the commit must stay white");
}

#[test]
fn pixels_outside_the_canvas_are_refused_and_roll_back_the_whole_commit() {
	// An out-of-bounds pixel must fail the call and undo the valid pixels before it.
	let db = build();
	seed_tile(&db, 0, 0);
	seed_tile(&db, 127, 127);
	mark_ready(&db);

	let err = commit(&db, vec![pack(8192, 0, 1)]).unwrap_err();
	assert!(err.contains("tile (128,0) is outside the canvas"), "x = 8192: {err}");
	let err = commit(&db, vec![pack(0, 8192, 1)]).unwrap_err();
	assert!(err.contains("tile (0,128) is outside the canvas"), "y = 8192: {err}");

	let err = commit(&db, vec![pack(1, 1, 5), pack(2, 1, 6), pack(8192, 1, 7)]).unwrap_err();
	assert!(err.contains("outside the canvas"), "{err}");
	assert_eq!(tile(&db, 0, 0), vec![0u8; TILE_BYTES]);

	commit(&db, vec![pack(8191, 8191, 9)]).expect("the last pixel is inside the canvas");
	assert_eq!(tile(&db, 127, 127)[TILE_BYTES - 1], 9);
}

#[test]
fn a_pixel_lands_at_the_byte_derived_from_its_coordinates() {
	// The procedure must pick tile x / 64, y / 64 and byte (y % 64) * 64 + x % 64, or paint lands on another pixel.
	let db = build();
	seed_tile(&db, 2, 1);
	mark_ready(&db);
	commit(&db, vec![pack(130, 70, 23)]).expect("commit");
	let bytes = tile(&db, 2, 1);
	assert_eq!(bytes[386], 23);
	assert_eq!(bytes.iter().filter(|&&b| b != 0).count(), 1, "exactly one byte of the tile may change");
}

#[test]
fn anonymous_can_read_tiles_but_never_insert_or_delete() {
	// Anonymous writes are limited to painting; inserting or deleting tiles would break the dense canvas.
	let db = build();
	seed_tile(&db, 0, 0);
	mark_ready(&db);

	let frames = anonymous(&db, "from place::tile filter { tile == 0 }").expect("anonymous read");
	assert_eq!(frames.first().map(Frame::row_count).unwrap_or(0), 1);

	let err = anonymous(&db, "insert place::tile [{ tile: 1, pixels: blob::hex(\"00\") }]").unwrap_err();
	assert!(err.starts_with("POLICY_002"), "{err}");
	let err = anonymous(&db, "delete place::tile filter { tile == 0 }").unwrap_err();
	assert!(err.starts_with("POLICY_002"), "{err}");
	assert_eq!(tiles(&db), 1);
}

#[test]
fn a_commit_before_the_canvas_is_ready_is_refused() {
	// During boot the WS port is open before every tile exists; a commit then must fail loudly, never update zero
	// rows.
	let db = build();
	seed_tile(&db, 0, 0);
	let err = commit(&db, vec![pack(1, 1, 12)]).unwrap_err();
	assert_eq!(err, "ASSERT canvas not ready");
	assert_eq!(pixel(&db, 1, 1), 0);

	mark_ready(&db);
	commit(&db, vec![pack(1, 1, 12)]).expect("commit once ready");
	assert_eq!(pixel(&db, 1, 1), 12);
}

#[test]
fn a_color_outside_the_32_color_palette_is_refused_whole() {
	// A color index past the palette has no swatch, so it must fail the commit and undo the pixels before it.
	let db = build();
	seed_tile(&db, 0, 0);
	mark_ready(&db);

	let err = commit(&db, vec![pack(1, 1, 5), pack(2, 1, 32)]).unwrap_err();
	assert!(err.contains("color 32 is outside the palette"), "color 32: {err}");
	assert_eq!(tile(&db, 0, 0), vec![0u8; TILE_BYTES]);

	commit(&db, vec![pack(1, 1, 31)]).expect("the last palette color");
	assert_eq!(pixel(&db, 1, 1), 31);
}

#[test]
fn a_malformed_tile_edit_is_refused_whole() {
	// A blob that is not a tile edit must fail the call before any tile is touched, never paint garbage bytes.
	let db = build();
	seed_tile(&db, 0, 0);
	mark_ready(&db);

	let good = edits(&[pack(1, 1, 5)]).remove(0);
	let err = commit_edits(&db, vec![good, Value::Blob(Blob::new(vec![0, 0, 1]))]).unwrap_err();
	assert!(err.contains("malformed tile edit of 3 bytes"), "{err}");
	let err = commit_edits(&db, vec![Value::Blob(Blob::new(vec![0, 0, 0x00, 0x10, 1]))]).unwrap_err();
	assert!(err.contains("pixel index 4096 is outside the tile"), "{err}");
	assert_eq!(tile(&db, 0, 0), vec![0u8; TILE_BYTES]);
}

#[test]
fn the_same_tile_twice_in_one_commit_applies_its_edits_in_order() {
	// one update paints each tile once, so two edits of one tile must merge with the later edit winning.
	let db = build();
	seed_tile(&db, 2, 1);
	mark_ready(&db);
	let first = Value::Blob(Blob::new(encode_edit(2, 1, &[(5, 4), (6, 4)])));
	let second = Value::Blob(Blob::new(encode_edit(2, 1, &[(5, 9)])));

	commit_edits(&db, vec![first, second]).expect("both edits of one tile commit together");

	let bytes = tile(&db, 2, 1);
	assert_eq!((bytes[5], bytes[6]), (9, 4), "the later edit must win on a shared pixel and keep the rest");
}

#[test]
fn pixel_count_of_a_constant_tiles_list_answers_every_row() {
	// The constant $tiles arrives as one row, so the row count must come from the query, not from that argument.
	let db = build();
	seed_tile(&db, 0, 0);
	seed_tile(&db, 1, 0);
	seed_tile(&db, 2, 0);
	let mut map = HashMap::new();
	map.insert("tiles".to_string(), Value::List(edits(&[pack(1, 1, 3), pack(65, 1, 4), pack(66, 1, 5)])));
	let r = db.engine().query_as(
		IdentityId::root(),
		"from place::tile map { tile, n: place::pixel_count($tiles) }",
		Params::from(map),
	);
	if let Some(e) = r.error {
		panic!("pixel count query failed: {e:?}");
	}
	let counts: Vec<i32> =
		r.frames.iter().flat_map(|f| f.rows()).map(|row| row.get::<i32>("n").unwrap().unwrap()).collect();
	assert_eq!(counts, vec![3, 3, 3], "every tile row must see the same pixel count of the constant list");
}

#[test]
fn tile_keys_of_a_constant_tiles_list_answers_every_row() {
	// Otherwise only the first row gets the key list and the rest are cut off or none.
	let db = build();
	seed_tile(&db, 0, 0);
	seed_tile(&db, 1, 0);
	seed_tile(&db, 2, 0);
	let mut map = HashMap::new();
	map.insert("tiles".to_string(), Value::List(edits(&[pack(1, 1, 3), pack(65, 1, 4)])));
	let r = db.engine().query_as(
		IdentityId::root(),
		"from place::tile map { tile, k: place::tile_keys($tiles) }",
		Params::from(map),
	);
	if let Some(e) = r.error {
		panic!("tile keys query failed: {e:?}");
	}
	let expected = Value::List(vec![Value::Uint2(tile_key(0, 0)), Value::Uint2(tile_key(1, 0))]);
	let keys: Vec<Value> =
		r.frames.iter().flat_map(|f| f.rows()).map(|row| row.get::<Value>("k").unwrap().unwrap()).collect();
	assert_eq!(keys.len(), 3, "one key list per tile row");
	assert!(keys.iter().all(|k| *k == expected || *k == Value::Any(Box::new(expected.clone()))), "{keys:?}");
}

#[test]
fn one_commit_paints_three_tiles_at_once() {
	// Paint runs over several tile rows with the constant $tiles, so each row must read its own tile and key.
	let db = build();
	seed_tile(&db, 0, 0);
	seed_tile(&db, 1, 0);
	seed_tile(&db, 2, 0);
	mark_ready(&db);
	commit(&db, vec![pack(1, 1, 3), pack(65, 2, 4), pack(130, 3, 5)]).expect("commit");
	assert_eq!(pixel(&db, 1, 1), 3);
	assert_eq!(pixel(&db, 65, 2), 4);
	assert_eq!(pixel(&db, 130, 3), 5);
	assert_eq!(pixel(&db, 66, 2), 0, "a neighbour pixel must stay untouched");
	assert_eq!(tiles(&db), 3, "a commit must never add a tile row");
}
