// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{
	Clock, Database, IdentityId, MockClock, RuntimeConfig, Value, WithSubsystem,
	auth::service::AuthResponse,
	server,
	value::{
		params::Params,
		value::{blob::Blob, frame::frame::Frame},
	},
};
use reifydb_place::{TILE_BYTES, decode_tile, encode_tile, migration_path, routines, tile_key};

fn build() -> (Database, MockClock) {
	let mock = MockClock::from_millis(1_700_000_000_000);
	let config = RuntimeConfig {
		clock: Clock::Mock(mock.clone()),
		..Default::default()
	};
	let db = server::memory()
		.with_runtime_config(config)
		.with_flow(|f| f)
		.with_routines(routines)
		.with_migrations(migration_path())
		.build()
		.expect("build memory db");
	(db, mock)
}

fn root(db: &Database, rql: &str, params: Params) {
	let r = db.engine().command_as(IdentityId::root(), rql, params);
	if let Some(e) = r.error {
		panic!("root command failed for [{rql}]: {e:?}");
	}
}

fn root_query(db: &Database, rql: &str) -> Vec<Frame> {
	let r = db.engine().query_as(IdentityId::root(), rql, Params::None);
	if let Some(e) = r.error {
		panic!("root query failed for [{rql}]: {e:?}");
	}
	r.frames
}

fn canvas(db: &Database, tiles: &[(u8, u8)]) {
	for &(tx, ty) in tiles {
		let mut map = HashMap::new();
		map.insert("pixels".to_string(), Value::Blob(Blob::new(encode_tile(&[0u8; TILE_BYTES]))));
		root(
			db,
			&format!("insert place::tile [{{ tile: {}, pixels: $pixels }}]", tile_key(tx, ty)),
			Params::from(map),
		);
	}
	root(db, "insert place::state [{ key: \"canvas\", ready: true }]", Params::None);
}

fn login(db: &Database, method: &str, credentials: &[(&str, &str)]) -> IdentityId {
	let credentials = credentials.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect();
	match db.auth_service().authenticate_as(IdentityId::anonymous(), method, credentials).expect("login") {
		AuthResponse::Authenticated {
			identity,
			..
		} => identity,
		other => panic!("{method} login did not authenticate: {other:?}"),
	}
}

fn agent(db: &Database, name: &str) -> IdentityId {
	login(db, "password", &[("identifier", name), ("password", "x")])
}

fn call(db: &Database, identity: IdentityId, rql: &str) -> Result<(), String> {
	let r = db.engine().command_as(identity, rql, Params::None);
	match r.error {
		Some(e) => Err(format!("{} {}", e.code, e.message)),
		None => Ok(()),
	}
}

fn pixel(db: &Database, x: u16, y: u16) -> u8 {
	let frames = root_query(
		db,
		&format!(
			"from place::tile filter {{ tile == {} }} map {{ pixels }}",
			tile_key((x / 64) as u8, (y / 64) as u8)
		),
	);
	let blob = frames[0].rows().next().expect("the tile is stored").get::<Blob>("pixels").unwrap().unwrap();
	decode_tile(blob.as_bytes()).expect("a stored tile decodes")[(y as usize % 64) * 64 + x as usize % 64]
}

fn count(db: &Database, table: &str) -> usize {
	root_query(db, &format!("from {table}")).iter().map(|f| f.row_count()).sum()
}

fn draws(db: &Database) -> Vec<(IdentityId, String, u16)> {
	root_query(db, "from place::draw map { owner, note, painted }")
		.iter()
		.flat_map(|f| f.rows())
		.map(|r| {
			(
				r.get::<IdentityId>("owner").unwrap().unwrap(),
				r.get::<String>("note").unwrap().unwrap(),
				r.get::<u16>("painted").unwrap().unwrap(),
			)
		})
		.collect()
}

#[test]
fn draw_pixels_paints_each_named_pixel_and_records_one_draw_with_its_note() {
	// Each "x,y,c" item must land on its own pixel, and the note must be one feed row however many tiles it spans.
	let (db, _) = build();
	canvas(&db, &[(0, 0), (1, 0)]);
	let a = agent(&db, "agent-a");

	call(&db, a, r#"CALL place::draw_pixels(["1,2,5", "70, 3, 9"], "two dots")"#).expect("the draw is accepted");

	assert_eq!((pixel(&db, 1, 2), pixel(&db, 70, 3)), (5, 9), "each pixel takes its own colour");
	assert_eq!(pixel(&db, 2, 2), 0, "no other pixel may change");
	assert_eq!(draws(&db), vec![(a, "two dots".to_string(), 2)], "one draw row, owned by the caller");
	assert_eq!(count(&db, "place::edit"), 2, "one edit row per touched tile, as place::commit writes");
}

#[test]
fn draw_sprite_lands_top_left_at_x_y_and_dot_keeps_the_old_pixel() {
	// The grid's first row and column sit on x,y, letters take their legend colour, and "." must not paint.
	let (db, mock) = build();
	canvas(&db, &[(0, 0)]);
	let a = agent(&db, "agent-a");
	call(&db, a, r#"CALL place::draw_pixels(["11,10,7"], "a dot to keep")"#).expect("the first draw");
	mock.advance_secs(60);

	call(&db, a, r#"CALL place::draw_sprite(10, 10, ["r.", ".R"], ["r=3", "R=4"], "a diagonal")"#)
		.expect("the sprite is accepted");

	assert_eq!(pixel(&db, 10, 10), 3, "the top-left cell lands on x,y");
	assert_eq!(pixel(&db, 11, 10), 7, "a dot keeps the pixel that was there");
	assert_eq!(pixel(&db, 10, 11), 0, "a dot keeps an untouched pixel too");
	assert_eq!(pixel(&db, 11, 11), 4, "letters are case-sensitive: R is not r");
	let sprite = draws(&db).into_iter().find(|d| d.1 == "a diagonal").expect("the sprite has its draw row");
	assert_eq!(sprite.2, 2, "painted counts letters only, not dots");
}

#[test]
fn the_4096_cap_counts_painted_cells_so_a_big_outline_fits() {
	// Dots are free, so a 128x128 outline (508 cells) must pass while 4160 solid cells must be refused whole.
	let (db, mock) = build();
	canvas(&db, &[(0, 0), (1, 0), (0, 1), (1, 1)]);
	let a = agent(&db, "agent-a");
	let solid: Vec<String> = (0..64).map(|_| format!("\"{}\"", "r".repeat(65))).collect();
	let err =
		call(&db, a, &format!("CALL place::draw_sprite(0, 0, [{}], [\"r=1\"], \"too big\")", solid.join(", ")))
			.expect_err("4160 painted cells are over the cap");
	assert!(err.contains("too many pixels"), "{err}");
	assert_eq!(count(&db, "place::draw"), 0, "a refused draw writes nothing");
	assert_eq!(pixel(&db, 0, 0), 0, "a refused draw paints nothing");

	mock.advance_secs(60);
	let edge = format!("\"{}\"", "r".repeat(128));
	let side = format!("\"r{}r\"", ".".repeat(126));
	let mut rows = vec![edge.clone()];
	rows.extend((0..126).map(|_| side.clone()));
	rows.push(edge);
	call(&db, a, &format!("CALL place::draw_sprite(0, 0, [{}], [\"r=1\"], \"a frame\")", rows.join(", ")))
		.expect("an outline of 508 cells fits under the cap");
	assert_eq!(draws(&db), vec![(a, "a frame".to_string(), 508)]);
	assert_eq!((pixel(&db, 127, 127), pixel(&db, 64, 64)), (1, 0), "the frame is painted, its inside is not");
}

#[test]
fn every_bad_input_is_refused_by_name_and_writes_nothing() {
	// Each mistake must name what is wrong so the agent fixes it in one retry, and must leave no trace behind.
	let (db, _) = build();
	canvas(&db, &[(0, 0)]);
	let a = agent(&db, "agent-a");
	let long_note = "n".repeat(141);
	let cases = [
		(r#"CALL place::draw_pixels(["10,20"], "x")"#.to_string(), r#"pixel 1 "10,20" must be "x,y,color""#),
		(
			r#"CALL place::draw_pixels(["1,1,1", "-1,2,3"], "x")"#.to_string(),
			r#"pixel 2 "-1,2,3" must be "x,y,color""#,
		),
		(
			r#"CALL place::draw_pixels(["8192,0,1"], "x")"#.to_string(),
			"pixel 1 at 8192,0 is outside the 8192x8192 canvas",
		),
		(
			r#"CALL place::draw_pixels(["1,1,40"], "x")"#.to_string(),
			"pixel 1 color 40 is outside the palette 0..31",
		),
		(
			r#"CALL place::draw_pixels(["1,1,1", "2,2,2", "1,1,3"], "x")"#.to_string(),
			"pixel 1,1 is given twice (items 1 and 3)",
		),
		(r#"CALL place::draw_pixels([], "x")"#.to_string(), "a draw must paint at least 1 pixel"),
		(r#"CALL place::draw_pixels(["1,1,1"], "")"#.to_string(), "note must be 1 to 140 characters"),
		(format!(r#"CALL place::draw_pixels(["1,1,1"], "{long_note}")"#), "note must be 1 to 140 characters"),
		(
			r#"CALL place::draw_sprite(0, 0, ["rr", "r"], ["r=1"], "x")"#.to_string(),
			"row 2 has 1 cells, row 1 has 2; all rows must be the same length",
		),
		(
			r#"CALL place::draw_sprite(0, 0, ["rq"], ["r=1"], "x")"#.to_string(),
			r#"row 1 column 2 letter "q" is not in the legend"#,
		),
		(
			r#"CALL place::draw_sprite(0, 0, ["r r"], ["r=1"], "x")"#.to_string(),
			r#"row 1 column 2 letter " " is not in the legend"#,
		),
		(
			r#"CALL place::draw_sprite(0, 0, ["r"], ["rr=1"], "x")"#.to_string(),
			r#"legend 1 "rr=1" must be "letter=color""#,
		),
		(
			r#"CALL place::draw_sprite(0, 0, ["r"], [".=1"], "x")"#.to_string(),
			r#"legend 1 "." is reserved for "leave the pixel as it is""#,
		),
		(
			r#"CALL place::draw_sprite(0, 0, ["r"], ["r=1", "r=2"], "x")"#.to_string(),
			r#"legend letter "r" is given twice (entries 1 and 2)"#,
		),
		(
			r#"CALL place::draw_sprite(0, 0, ["r"], ["r=32"], "x")"#.to_string(),
			"legend 1 color 32 is outside the palette 0..31",
		),
		(
			r#"CALL place::draw_sprite(8190, 0, ["rrr"], ["r=1"], "x")"#.to_string(),
			"sprite 3x1 at 8190,0 ends at 8192,0, past the canvas edge 8191",
		),
		(
			r#"CALL place::draw_sprite(0, 0, ["...."], ["r=1"], "x")"#.to_string(),
			"a draw must paint at least 1 pixel",
		),
	];
	for (rql, expected) in &cases {
		let err = call(&db, a, rql).expect_err(rql);
		assert!(err.contains(expected), "[{rql}] must say [{expected}], got [{err}]");
	}
	let wide = format!(r#"CALL place::draw_sprite(0, 0, ["{}"], ["r=1"], "x")"#, "r".repeat(129));
	let err = call(&db, a, &wide).expect_err("a 129 wide grid");
	assert!(err.contains("sprite is 129x1, max is 128x128"), "{err}");

	assert_eq!((count(&db, "place::draw"), count(&db, "place::edit"), count(&db, "place::cooldown")), (0, 0, 0));
	assert_eq!(pixel(&db, 1, 1), 0, "no refused draw may paint");
}

#[test]
fn an_agent_draws_once_per_minute_without_blocking_other_agents() {
	// The cooldown is per agent: a second call inside 60 s is refused, another agent is not, and 60 s later it
	// opens again.
	let (db, mock) = build();
	canvas(&db, &[(0, 0)]);
	let (a, b) = (agent(&db, "agent-a"), agent(&db, "agent-b"));

	call(&db, a, r#"CALL place::draw_pixels(["1,1,1"], "first")"#).expect("the first draw");
	mock.advance_secs(59);
	let err = call(&db, a, r#"CALL place::draw_pixels(["2,2,2"], "too soon")"#).expect_err("59 s later");
	assert!(err.contains("one draw per minute per agent"), "{err}");
	call(&db, b, r#"CALL place::draw_pixels(["3,3,3"], "other agent")"#).expect("another agent is not blocked");

	mock.advance_secs(1);
	call(&db, a, r#"CALL place::draw_sprite(4, 4, ["r"], ["r=4"], "a minute later")"#)
		.expect("60 s after the last draw, either procedure opens again");
	assert_eq!(count(&db, "place::cooldown"), 2, "one cooldown row per agent");
	assert_eq!(pixel(&db, 2, 2), 0, "the refused draw painted nothing");
}

#[test]
fn only_signed_up_users_may_draw_and_nobody_writes_the_draw_tables_directly() {
	// Guests paint through place::commit only, and the draw and cooldown tables have no door besides the
	// procedures.
	let (db, _) = build();
	canvas(&db, &[(0, 0)]);
	let guest = login(&db, "guest", &[]);
	let a = agent(&db, "agent-a");

	let err = call(&db, guest, r#"CALL place::draw_pixels(["1,1,1"], "a guest")"#).expect_err("a guest");
	assert!(err.starts_with("POLICY_"), "{err}");

	for rql in [
		"insert place::draw [{ owner: $identity.id, name: \"x\", note: \"fake\", painted: 1 }]",
		"insert place::cooldown [{ owner: $identity.id, last: datetime::now() }]",
	] {
		let err = call(&db, a, rql).expect_err(rql);
		assert!(err.starts_with("POLICY_002"), "[{rql}] {err}");
	}
	assert_eq!((count(&db, "place::draw"), count(&db, "place::cooldown")), (0, 0));
}
