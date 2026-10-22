// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Database, IdentityId, WithSubsystem, server, value::params::Params};
use reifydb_place::migration_path;

fn build() -> Database {
	server::memory().with_flow(|f| f).with_migrations(migration_path()).build().expect("build memory db")
}

#[test]
fn live_is_partitioned_so_tile_filters_prune() {
	// #partition exists only on a partitioned table; without it every tile scan reads all 4.2 M rows.
	let db = build();
	let r = db.engine().query_as(IdentityId::root(), "from place::live map { #partition }", Params::None);
	assert!(r.error.is_none(), "place::live must be partitioned: {:?}", r.error);
}
