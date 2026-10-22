// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{
	Database, IdentityId, Value, WithSubsystem, server,
	value::{params::Params, value::frame::frame::Frame},
};
use reifydb_place::migration_path;

fn build() -> Database {
	server::memory().with_flow(|f| f).with_migrations(migration_path()).build().expect("build memory db")
}

fn root_query(db: &Database, rql: &str) -> Vec<Frame> {
	let r = db.engine().query_as(IdentityId::root(), rql, Params::None);
	if let Some(e) = r.error {
		panic!("root query failed for [{rql}]: {e:?}");
	}
	r.frames
}

fn seed_tile(db: &Database, tx: u8, ty: u8) {
	let mut rows = Vec::with_capacity(4096);
	for dy in 0..64u16 {
		for dx in 0..64u16 {
			let (x, y) = (tx as u16 * 64 + dx, ty as u16 * 64 + dy);
			rows.push(format!("{{ tile_x: {tx}, tile_y: {ty}, x: {x}, y: {y}, color: 0 }}"));
		}
	}
	let rql = format!("insert place::live [{}]", rows.join(", "));
	let r = db.engine().command_as(IdentityId::root(), &rql, Params::None);
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

fn total(db: &Database) -> i64 {
	let frames = root_query(db, "from place::live aggregate { n: math::count(x) }");
	frames.first().and_then(|f| f.rows().next()).map(|r| r.get::<i64>("n").unwrap().unwrap()).unwrap_or(0)
}

fn pixel(db: &Database, x: u16, y: u16) -> Vec<(u8, u8, u8)> {
	let frames = root_query(
		db,
		&format!("from place::live filter {{ x == {x} and y == {y} }} map {{ tile_x, tile_y, color }}"),
	);
	match frames.first() {
		None => vec![],
		Some(f) => {
			f.rows().map(|r| {
				(
					r.get::<u8>("tile_x").unwrap().unwrap(),
					r.get::<u8>("tile_y").unwrap().unwrap(),
					r.get::<u8>("color").unwrap().unwrap(),
				)
			})
			.collect()
		}
	}
}

fn pack(x: u64, y: u64, color: u64) -> Value {
	Value::Uint8(x * 2097152 + y * 256 + color)
}

fn commit(db: &Database, pixels: Vec<Value>) -> Result<(), String> {
	let mut map = HashMap::new();
	map.insert("pixels".to_string(), Value::List(pixels));
	let r = db.engine().command_as(IdentityId::anonymous(), "CALL place::commit($pixels)", Params::from(map));
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
	// A commit must change the named pixels and add no rows, or the dense canvas drifts out of shape.
	let db = build();
	seed_tile(&db, 0, 0);
	mark_ready(&db);
	commit(&db, vec![pack(1, 1, 42), pack(2, 1, 43), pack(3, 1, 44)]).expect("commit");
	assert_eq!(pixel(&db, 1, 1), vec![(0, 0, 42)]);
	assert_eq!(pixel(&db, 2, 1), vec![(0, 0, 43)]);
	assert_eq!(pixel(&db, 3, 1), vec![(0, 0, 44)]);
	assert_eq!(pixel(&db, 4, 1), vec![(0, 0, 0)], "an untouched neighbour must stay white");
	assert_eq!(total(&db), 4096);
}

#[test]
fn repainting_a_pixel_keeps_one_row_with_the_last_color() {
	// Two commits to one pixel must leave one row, or one pixel shows two colors to subscribers.
	let db = build();
	seed_tile(&db, 0, 0);
	mark_ready(&db);
	commit(&db, vec![pack(5, 5, 10)]).expect("first commit");
	commit(&db, vec![pack(5, 5, 11)]).expect("second commit");
	assert_eq!(pixel(&db, 5, 5), vec![(0, 0, 11)]);
}

#[test]
fn more_than_1024_pixels_are_refused_whole() {
	// The cap is the only bound on one anonymous write, so going over it must write nothing at all.
	let db = build();
	seed_tile(&db, 0, 0);
	mark_ready(&db);
	let pixels = (0..1025u64).map(|i| pack(i % 64, i / 64, 7)).collect();
	let err = commit(&db, pixels).unwrap_err();
	assert!(err.starts_with("ASSERT"), "{err}");
	assert_eq!(pixel(&db, 0, 0), vec![(0, 0, 0)]);
}

#[test]
fn exactly_1024_pixels_are_accepted() {
	// The cap must be inclusive, or the client's 1,024-pixel limit produces commits the server rejects.
	let db = build();
	seed_tile(&db, 0, 0);
	mark_ready(&db);
	let pixels = (0..1024u64).map(|i| pack(i % 64, i / 64, 8)).collect();
	commit(&db, pixels).expect("1,024 pixels");
	assert_eq!(pixel(&db, 63, 15), vec![(0, 0, 8)]);
}

#[test]
fn pixels_outside_the_canvas_are_refused_and_roll_back_the_whole_commit() {
	// An out-of-bounds pixel must fail the call and undo the valid pixels before it.
	let db = build();
	seed_tile(&db, 0, 0);
	seed_tile(&db, 31, 31);
	mark_ready(&db);

	let err = commit(&db, vec![pack(2048, 0, 1)]).unwrap_err();
	assert!(err.starts_with("ASSERT"), "x = 2048: {err}");
	let err = commit(&db, vec![pack(0, 2048, 1)]).unwrap_err();
	assert!(err.starts_with("ASSERT"), "y = 2048: {err}");

	let err = commit(&db, vec![pack(1, 1, 5), pack(2, 1, 6), pack(2048, 1, 7)]).unwrap_err();
	assert!(err.starts_with("ASSERT"), "{err}");
	assert_eq!(pixel(&db, 1, 1), vec![(0, 0, 0)]);
	assert_eq!(pixel(&db, 2, 1), vec![(0, 0, 0)]);

	commit(&db, vec![pack(2047, 2047, 9)]).expect("the last pixel is inside the canvas");
	assert_eq!(pixel(&db, 2047, 2047), vec![(31, 31, 9)]);
}

#[test]
fn a_pixel_lands_in_the_tile_derived_from_its_coordinates() {
	// The procedure must compute the tile as x / 64 and y / 64, or the update filter matches no row.
	let db = build();
	seed_tile(&db, 2, 1);
	mark_ready(&db);
	commit(&db, vec![pack(130, 70, 33)]).expect("commit");
	assert_eq!(pixel(&db, 130, 70), vec![(2, 1, 33)]);
}

#[test]
fn anonymous_can_read_live_but_never_insert_or_delete() {
	// Anonymous writes are limited to recoloring; inserting or deleting would break the dense canvas.
	let db = build();
	seed_tile(&db, 0, 0);
	mark_ready(&db);

	let frames = anonymous(&db, "from place::live filter { tile_x == 0 and tile_y == 0 }").expect("anonymous read");
	assert_eq!(frames.first().map(Frame::row_count).unwrap_or(0), 4096);

	let err = anonymous(&db, "insert place::live [{ tile_x: 0, tile_y: 0, x: 0, y: 0, color: 1 }]").unwrap_err();
	assert!(err.starts_with("POLICY_002"), "{err}");
	let err = anonymous(&db, "delete place::live filter { tile_x == 0 and tile_y == 0 and x == 0 and y == 0 }")
		.unwrap_err();
	assert!(err.starts_with("POLICY_002"), "{err}");
	assert_eq!(total(&db), 4096);
}

#[test]
fn a_commit_before_the_canvas_is_ready_is_refused() {
	// During boot the WS port is open before every tile exists; a commit then must fail loudly, never update zero
	// rows.
	let db = build();
	seed_tile(&db, 0, 0);
	let err = commit(&db, vec![pack(1, 1, 42)]).unwrap_err();
	assert_eq!(err, "ASSERT canvas not ready");
	assert_eq!(pixel(&db, 1, 1), vec![(0, 0, 0)]);

	mark_ready(&db);
	commit(&db, vec![pack(1, 1, 42)]).expect("commit once ready");
	assert_eq!(pixel(&db, 1, 1), vec![(0, 0, 42)]);
}
