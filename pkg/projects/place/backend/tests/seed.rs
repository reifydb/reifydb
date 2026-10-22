// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Database, IdentityId, WithSubsystem, server, value::params::Params};
use reifydb_place::{migration_path, seed};

fn build() -> Database {
	server::memory().with_flow(|f| f).with_migrations(migration_path()).build().expect("build memory db")
}

fn count(db: &Database, rql: &str) -> i64 {
	let r = db.engine().query_as(IdentityId::root(), rql, Params::None);
	if let Some(e) = r.error {
		panic!("count failed for [{rql}]: {e:?}");
	}
	r.frames.first().and_then(|f| f.rows().next()).map(|row| row.get::<i64>("n").unwrap().unwrap()).unwrap_or(0)
}

#[test]
fn seed_fills_every_pixel_white_once_and_refuses_a_damaged_tile() {
	// One seeded db serves all checks: a full seed is the expensive step and every check reads the same state.
	let db = build();
	seed(&db).expect("first seed");

	// Every pixel of the canvas must exist, or a commit to it silently updates zero rows.
	assert_eq!(count(&db, "from place::live aggregate { n: math::count(x) }"), 4_194_304);

	// A tile must hold exactly its own 64x64 pixels, all white.
	let tile = "from place::live filter { tile_x == 1 and tile_y == 2 }";
	assert_eq!(count(&db, &format!("{tile} aggregate {{ n: math::count(x) }}")), 4096);
	assert_eq!(count(&db, &format!("{tile} filter {{ color != 0 }} aggregate {{ n: math::count(x) }}")), 0);
	assert_eq!(
		count(
			&db,
			&format!(
				"{tile} filter {{ x < 64 or x >= 128 or y < 128 or y >= 192 }} aggregate {{ n: math::count(x) }}"
			)
		),
		0,
		"tile (1,2) must hold only x in 64..128 and y in 128..192"
	);

	// Commits stay refused until the seed marks the canvas ready, so the flag must be set exactly once.
	let ready =
		"from place::state filter { key == \"canvas\" and ready == true } aggregate { n: math::count(key) }";
	assert_eq!(count(&db, ready), 1);

	// A restart must not duplicate pixels, or one update would change two rows.
	seed(&db).expect("second seed");
	assert_eq!(count(&db, "from place::live aggregate { n: math::count(x) }"), 4_194_304);
	assert_eq!(count(&db, ready), 1);

	// A tile that is neither empty nor full means a broken seed and must stop the boot, never be skipped.
	let r = db.engine().command_as(
		IdentityId::root(),
		"insert place::live [{ tile_x: 3, tile_y: 3, x: 200, y: 200, color: 0 }]",
		Params::None,
	);
	assert!(r.error.is_none(), "{:?}", r.error);
	assert_eq!(seed(&db).unwrap_err(), "tile (3,3) has 4097 rows, expected 0 or 4096");
}
