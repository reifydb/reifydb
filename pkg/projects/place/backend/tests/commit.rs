// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{
	Database, IdentityId, Value, WithSubsystem,
	auth::service::AuthResponse,
	core::value::column::factory,
	routine::abi::{context::FunctionContext, error::RoutineError, registry::Routines},
	runtime::context::RuntimeContext,
	server,
	value::{
		fragment::Fragment,
		params::Params,
		value::{blob::Blob, frame::frame::Frame, value_type::ValueType},
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

fn login_as(db: &Database, caller: IdentityId, method: &str, credentials: &[(&str, &str)]) -> IdentityId {
	let credentials = credentials.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect();
	match db.auth_service().authenticate_as(caller, method, credentials).expect("login") {
		AuthResponse::Authenticated {
			identity,
			..
		} => identity,
		other => panic!("{method} login did not authenticate: {other:?}"),
	}
}

fn guest(db: &Database) -> IdentityId {
	login_as(db, IdentityId::anonymous(), "guest", &[])
}

fn commit_edits(db: &Database, tiles: Vec<Value>) -> Result<(), String> {
	commit_edits_as(db, guest(db), tiles)
}

fn commit_as(db: &Database, identity: IdentityId, pixels: Vec<(u64, u64, u64)>) -> Result<(), String> {
	commit_edits_as(db, identity, edits(&pixels))
}

fn commit_edits_as(db: &Database, identity: IdentityId, tiles: Vec<Value>) -> Result<(), String> {
	let mut map = HashMap::new();
	map.insert("tiles".to_string(), Value::List(tiles));
	let r = db.engine().command_as(identity, "CALL place::commit($tiles)", Params::from(map));
	match r.error {
		Some(e) => Err(format!("{} {}", e.code, e.message)),
		None => Ok(()),
	}
}

fn edit_rows(db: &Database) -> Vec<(IdentityId, u16, Vec<u8>)> {
	root_query(db, "from place::edit map { owner, tile, pixels }")
		.iter()
		.flat_map(|f| f.rows())
		.map(|r| {
			(
				r.get::<IdentityId>("owner").unwrap().unwrap(),
				r.get::<u16>("tile").unwrap().unwrap(),
				r.get::<Blob>("pixels").unwrap().unwrap().as_bytes().to_vec(),
			)
		})
		.collect()
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

#[test]
fn a_commit_spanning_several_batches_paints_every_tile() {
	// The prepared edit map must serve every batch of one update, otherwise tiles past the first batch go
	// unpainted.
	let db = build();
	let coords: Vec<(u8, u8)> = (0..300u16).map(|i| ((i % 128) as u8, (i / 128) as u8)).collect();
	for &(tx, ty) in &coords {
		seed_tile(&db, tx, ty);
	}
	mark_ready(&db);
	let pixels = coords
		.iter()
		.enumerate()
		.map(|(i, &(tx, ty))| pack(tx as u64 * 64 + (i % 64) as u64, ty as u64 * 64, 1 + (i % 31) as u64))
		.collect();
	commit(&db, pixels).expect("one commit over 300 tiles");
	let wrong: Vec<String> = coords
		.iter()
		.enumerate()
		.filter(|(i, (tx, ty))| {
			pixel(&db, *tx as u16 * 64 + (i % 64) as u16, *ty as u16 * 64) != 1 + (i % 31) as u8
		})
		.map(|(i, coord)| format!("{i} at {coord:?}"))
		.collect();
	assert!(wrong.is_empty(), "unpainted tiles: {wrong:?}");
}

#[test]
fn a_prepared_paint_still_refuses_ragged_arguments() {
	// Skipping the length check on the prepared path would index a short argument past its end.
	let registry = routines(Routines::builder()).configure();
	let paint = registry.get_scalar_function("place::paint").expect("place::paint is registered");
	let runtime = RuntimeContext::testing(0, 0);
	let mut ctx = FunctionContext {
		fragment: Fragment::internal("place::paint"),
		identity: IdentityId::root(),
		row_count: 3,
		runtime_context: &runtime,
	};
	let edit = Value::Blob(Blob::new(encode_edit(0, 0, &[(0, 1)])));
	let list = factory::any_typed("tiles", [Value::List(vec![edit])], ValueType::List(Box::new(ValueType::Blob)));
	let prepared = paint.prepare(&mut ctx, &[None, None, Some(&list)]).unwrap();
	assert!(prepared.is_some(), "paint must prepare from a constant tiles list");
	let blank = Blob::new(encode_tile(&[0u8; TILE_BYTES]));
	let args =
		vec![factory::blob("pixels", vec![blank.clone(), blank]), factory::uint2("tile", [0u16, 0, 0]), list];

	match paint.execute_prepared(&mut ctx, &args, prepared.as_ref()) {
		Err(RoutineError::FunctionExecutionFailed {
			..
		}) => {}
		other => panic!("ragged arguments must fail execution, got {other:?}"),
	}
}

#[test]
fn a_guest_commit_writes_one_edit_row_per_tile_owned_by_that_guest() {
	// Attribution rests on this row: without it, or with another owner, a pixel change cannot be traced to its
	// painter.
	let db = build();
	seed_tile(&db, 0, 0);
	seed_tile(&db, 1, 0);
	mark_ready(&db);
	let painter = guest(&db);
	let tiles = edits(&[pack(1, 1, 3), pack(65, 1, 4)]);
	let sent: Vec<Vec<u8>> = tiles
		.iter()
		.map(|t| match t {
			Value::Blob(b) => b.as_bytes().to_vec(),
			_ => unreachable!(),
		})
		.collect();

	commit_edits_as(&db, painter, tiles).expect("guest commit");

	let mut rows = edit_rows(&db);
	rows.sort_by_key(|(_, tile, _)| *tile);
	assert_eq!(
		rows,
		vec![(painter, tile_key(0, 0), sent[0].clone()), (painter, tile_key(1, 0), sent[1].clone())],
		"exactly one row per tile, owned by the caller, holding the edit as sent"
	);
}

#[test]
fn an_anonymous_commit_is_refused_and_writes_nothing() {
	// Anonymous paint would be a change with no owner, so it must be refused before any tile or edit row is
	// written.
	let db = build();
	seed_tile(&db, 0, 0);
	mark_ready(&db);

	let err = commit_as(&db, IdentityId::anonymous(), vec![pack(1, 1, 12)]).unwrap_err();
	assert!(err.starts_with("POLICY_"), "{err}");
	assert_eq!(pixel(&db, 1, 1), 0);
	assert!(edit_rows(&db).is_empty(), "a refused commit must leave no edit row");
}

#[test]
fn a_user_commit_is_accepted_and_owned_by_that_user() {
	// Agents paint as named users, so a user commit must pass both policies and carry the user as owner.
	let db = build();
	seed_tile(&db, 0, 0);
	mark_ready(&db);
	let agent = login_as(&db, IdentityId::anonymous(), "password", &[("identifier", "agent-a"), ("password", "x")]);

	commit_as(&db, agent, vec![pack(1, 1, 12)]).expect("user commit");

	assert_eq!(pixel(&db, 1, 1), 12);
	let owners: Vec<IdentityId> = edit_rows(&db).into_iter().map(|(owner, _, _)| owner).collect();
	assert_eq!(owners, vec![agent]);
}

#[test]
fn a_commit_refused_by_the_pixel_cap_writes_no_edit_row() {
	// Tile update and edit insert are one transaction; a log row for paint that never landed would be false
	// attribution.
	let db = build();
	seed_tile(&db, 0, 0);
	seed_tile(&db, 0, 1);
	mark_ready(&db);
	let pixels = (0..4097u64).map(|i| pack(i % 64, i / 64, 7)).collect();

	commit(&db, pixels).unwrap_err();

	assert!(edit_rows(&db).is_empty(), "a refused commit must leave no edit row");
}

#[test]
fn a_promoted_guest_keeps_ownership_of_its_edits() {
	// Signup must promote the guest in place, or every pixel painted before signup is credited to a dead guest.
	let db = build();
	seed_tile(&db, 0, 0);
	mark_ready(&db);
	let painter = guest(&db);
	commit_as(&db, painter, vec![pack(1, 1, 12)]).expect("guest commit");

	let user = login_as(&db, painter, "password", &[("identifier", "alice"), ("password", "x")]);

	assert_eq!(user, painter, "the password method must promote the guest, not create a new identity");
	commit_as(&db, user, vec![pack(2, 1, 13)]).expect("commit after signup");
	let owners: Vec<IdentityId> = edit_rows(&db).into_iter().map(|(owner, _, _)| owner).collect();
	assert_eq!(owners, vec![painter, painter]);
}

#[test]
fn tile_key_answers_the_key_of_each_tile_edit() {
	// The edit row's tile column comes from this; a swapped tx/ty would file every edit under the wrong tile.
	let db = build();
	let mut map = HashMap::new();
	map.insert("a".to_string(), Value::Blob(Blob::new(encode_edit(3, 5, &[(0, 1)]))));
	map.insert("b".to_string(), Value::Blob(Blob::new(encode_edit(127, 0, &[]))));
	let r = db.engine().query_as(
		IdentityId::root(),
		"map { a: place::tile_key($a), b: place::tile_key($b) }",
		Params::from(map),
	);
	if let Some(e) = r.error {
		panic!("tile key query failed: {e:?}");
	}
	let row = r.frames.iter().flat_map(|f| f.rows()).next().expect("one row");
	assert_eq!(row.get::<u16>("a").unwrap().unwrap(), tile_key(3, 5));
	assert_eq!(row.get::<u16>("b").unwrap().unwrap(), tile_key(127, 0));
}

#[test]
fn an_anonymous_call_is_refused_by_the_call_policy_itself() {
	// The tile policy also blocks anonymous, so the call policy needs its own proof or it can open unnoticed.
	let db = build();
	seed_tile(&db, 0, 0);
	mark_ready(&db);

	let err = commit_as(&db, IdentityId::anonymous(), vec![pack(1, 1, 12)]).unwrap_err();

	assert!(err.contains("place_call_commit"), "the call policy must be the one that refuses: {err}");
	assert_eq!(pixel(&db, 1, 1), 0);
}

#[test]
fn edit_pixels_answers_the_pixel_count_of_each_tile_edit() {
	// The agents list sums this per painter; an off count misreports how much each one painted.
	let db = build();
	let mut map = HashMap::new();
	map.insert("one".to_string(), Value::Blob(Blob::new(encode_edit(3, 5, &[(0, 1)]))));
	map.insert("three".to_string(), Value::Blob(Blob::new(encode_edit(3, 5, &[(0, 1), (1, 2), (2, 3)]))));
	map.insert("empty".to_string(), Value::Blob(Blob::new(encode_edit(3, 5, &[]))));
	let r = db.engine().query_as(
		IdentityId::root(),
		"map { one: place::edit_pixels($one), three: place::edit_pixels($three), empty: place::edit_pixels($empty) }",
		Params::from(map),
	);
	if let Some(e) = r.error {
		panic!("edit pixels query failed: {e:?}");
	}
	let row = r.frames.iter().flat_map(|f| f.rows()).next().expect("one row");
	assert_eq!(row.get::<u16>("one").unwrap().unwrap(), 1);
	assert_eq!(row.get::<u16>("three").unwrap().unwrap(), 3);
	assert_eq!(row.get::<u16>("empty").unwrap().unwrap(), 0);
}

#[test]
fn an_edit_row_carries_the_painter_name_and_its_pixel_count() {
	// The agents list reads names and pixel totals from these columns; without them it shows ids and edit counts.
	let db = build();
	seed_tile(&db, 0, 0);
	seed_tile(&db, 1, 0);
	mark_ready(&db);
	let agent = login_as(&db, IdentityId::anonymous(), "password", &[("identifier", "agent-a"), ("password", "x")]);

	commit_as(&db, agent, vec![pack(1, 1, 3), pack(2, 1, 4), pack(65, 1, 5)]).expect("commit");

	let mut rows: Vec<(String, u16, u16)> = root_query(&db, "from place::edit map { name, tile, painted }")
		.iter()
		.flat_map(|f| f.rows())
		.map(|r| {
			(
				r.get::<String>("name").unwrap().unwrap(),
				r.get::<u16>("tile").unwrap().unwrap(),
				r.get::<u16>("painted").unwrap().unwrap(),
			)
		})
		.collect();
	rows.sort_by_key(|(_, tile, _)| *tile);
	assert_eq!(rows, vec![("agent-a".to_string(), tile_key(0, 0), 2), ("agent-a".to_string(), tile_key(1, 0), 1)]);
}

#[test]
fn a_guest_edit_row_carries_the_guest_name_shape() {
	// The list folds guests by the guest: name prefix, so a guest row must carry exactly that shape.
	let db = build();
	seed_tile(&db, 0, 0);
	mark_ready(&db);

	commit(&db, vec![pack(1, 1, 3)]).expect("guest commit");

	let names: Vec<String> = root_query(&db, "from place::edit map { name }")
		.iter()
		.flat_map(|f| f.rows())
		.map(|r| r.get::<String>("name").unwrap().unwrap())
		.collect();
	assert_eq!(names.len(), 1);
	assert!(names[0].starts_with("guest:"), "{names:?}");
}
