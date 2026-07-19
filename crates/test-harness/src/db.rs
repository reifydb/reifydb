// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Deref;

use reifydb::{Database, embedded};
use reifydb_engine::engine::StandardEngine;
use reifydb_value::value::frame::frame::Frame;

use crate::engine::AsEngine;

pub struct TestDb {
	db: Database,
}

impl TestDb {
	pub fn memory() -> Self {
		Self {
			db: embedded::memory().build().unwrap(),
		}
	}

	pub fn admin(&self, rql: &str) -> Vec<Frame> {
		self.db.admin_as_root(rql, ()).unwrap()
	}

	pub fn command(&self, rql: &str) -> Vec<Frame> {
		self.db.command_as_root(rql, ()).unwrap()
	}

	pub fn query(&self, rql: &str) -> Vec<Frame> {
		self.db.query_as_root(rql, ()).unwrap()
	}
}

impl From<Database> for TestDb {
	fn from(db: Database) -> Self {
		Self {
			db,
		}
	}
}

impl Deref for TestDb {
	type Target = Database;

	fn deref(&self) -> &Database {
		&self.db
	}
}

impl AsEngine for TestDb {
	fn standard_engine(&self) -> &StandardEngine {
		self.db.engine()
	}
}

impl AsEngine for Database {
	fn standard_engine(&self) -> &StandardEngine {
		self.engine()
	}
}
