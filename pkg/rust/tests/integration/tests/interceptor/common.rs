// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb::{Database, Params, embedded};

pub fn fresh_db() -> Database {
	let db = embedded::memory().build().unwrap();
	db
}

pub fn admin(db: &Database, rql: &str) {
	db.admin_as_root(rql, Params::None).unwrap_or_else(|e| panic!("admin failed: {e:?}\nrql: {rql}"));
}
