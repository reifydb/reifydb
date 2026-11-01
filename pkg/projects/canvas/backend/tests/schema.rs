// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Database, IdentityId, WithSubsystem, server, value::params::Params};
use reifydb_canvas::{migration_path, routines};

fn build() -> Database {
	server::memory()
		.with_flow(|f| f)
		.with_routines(routines)
		.with_migrations(migration_path())
		.build()
		.expect("build memory db")
}

#[test]
fn tile_is_partitioned_so_tile_filters_prune() {
	// #partition exists only on a partitioned table; without it every tile read and commit scans all 16,384 tiles.
	let db = build();
	let r = db.engine().query_as(IdentityId::root(), "from canvas::tile map { #partition }", Params::None);
	assert!(r.error.is_none(), "canvas::tile must be partitioned: {:?}", r.error);
}

#[test]
fn the_per_pixel_live_table_is_gone() {
	// A leftover canvas::live would keep its 4.2 M-row layout reachable and its policies granting anonymous updates.
	let db = build();
	let r = db.engine().query_as(IdentityId::root(), "from canvas::live", Params::None);
	assert!(r.error.is_some(), "canvas::live must be dropped by the tile migration");
}
