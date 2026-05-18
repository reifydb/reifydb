// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb::{Database, Params, embedded};

pub fn fresh_db() -> Database {
	let mut db = embedded::memory().build().unwrap();
	db.start().unwrap();
	db
}

pub fn admin(db: &Database, rql: &str) {
	db.admin_as_root(rql, Params::None).unwrap_or_else(|e| panic!("admin failed: {e:?}\nrql: {rql}"));
}
