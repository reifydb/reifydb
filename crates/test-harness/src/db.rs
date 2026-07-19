// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	env,
	fs::{remove_dir_all, remove_file},
	ops::{Deref, DerefMut},
	path::{Path, PathBuf},
	process,
	thread::sleep,
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use reifydb::{Database, Frame, Result, SqliteConfig, embedded};
use reifydb_engine::engine::StandardEngine;
use reifydb_sqlite::SqliteTempPathGuard;

use crate::engine::AsEngine;

pub struct TestDb {
	db: Database,
	_guard: Option<SqliteTempPathGuard>,
}

impl TestDb {
	pub fn memory() -> Self {
		Self::wrap(embedded::memory().build().unwrap(), None)
	}

	pub fn sqlite(config: SqliteConfig) -> Self {
		Self::wrap(embedded::sqlite(config).build().unwrap(), None)
	}

	pub fn sqlite_without_buffer(config: SqliteConfig) -> Self {
		Self::wrap(embedded::sqlite_without_buffer(config).build().unwrap(), None)
	}

	pub fn sqlite_at(path: impl AsRef<Path>) -> Self {
		Self::sqlite(SqliteConfig::new(path))
	}

	pub fn sqlite_memory() -> Self {
		let (config, guard) = SqliteConfig::in_memory();
		Self::wrap(embedded::sqlite(config).build().unwrap(), Some(guard))
	}

	pub fn sqlite_without_buffer_memory() -> Self {
		let (config, guard) = SqliteConfig::in_memory();
		Self::wrap(embedded::sqlite_without_buffer(config).build().unwrap(), Some(guard))
	}

	fn wrap(db: Database, guard: Option<SqliteTempPathGuard>) -> Self {
		Self {
			db,
			_guard: guard,
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

	pub fn try_admin(&self, rql: &str) -> Result<Vec<Frame>> {
		self.db.admin_as_root(rql, ())
	}

	pub fn try_command(&self, rql: &str) -> Result<Vec<Frame>> {
		self.db.command_as_root(rql, ())
	}

	pub fn try_query(&self, rql: &str) -> Result<Vec<Frame>> {
		self.db.query_as_root(rql, ())
	}

	pub fn row_count(&self, rql: &str) -> usize {
		self.query(rql).iter().map(|frame| frame.row_count()).sum()
	}

	pub fn await_row_count(&self, rql: &str, want: usize, timeout: Duration) -> usize {
		let deadline = Instant::now() + timeout;
		loop {
			let got = self.row_count(rql);
			if got >= want || Instant::now() >= deadline {
				return got;
			}
			sleep(Duration::from_millis(20));
		}
	}

	pub fn await_exact_row_count(&self, rql: &str, want: usize, timeout: Duration) -> usize {
		await_value(want, timeout, || self.row_count(rql))
	}

	pub fn stop(&mut self) {
		self.db.stop().unwrap()
	}
}

impl From<Database> for TestDb {
	fn from(db: Database) -> Self {
		Self::wrap(db, None)
	}
}

impl Deref for TestDb {
	type Target = Database;

	fn deref(&self) -> &Database {
		&self.db
	}
}

impl DerefMut for TestDb {
	fn deref_mut(&mut self) -> &mut Database {
		&mut self.db
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

pub fn await_value<T: PartialEq>(want: T, timeout: Duration, mut poll: impl FnMut() -> T) -> T {
	let deadline = Instant::now() + timeout;
	loop {
		let got = poll();
		if got == want || Instant::now() >= deadline {
			return got;
		}
		sleep(Duration::from_millis(20));
	}
}

pub fn poll_until<T>(mut poll: impl FnMut() -> Option<T>, timeout: Duration) -> Option<T> {
	let deadline = Instant::now() + timeout;
	loop {
		if let Some(value) = poll() {
			return Some(value);
		}
		if Instant::now() >= deadline {
			return None;
		}
		sleep(Duration::from_millis(20));
	}
}

pub struct TempDbPath {
	path: PathBuf,
}

impl TempDbPath {
	pub fn new(tag: &str) -> Self {
		let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
		let path = env::temp_dir().join(format!("reifydb_{tag}_{}_{}.reifydb", process::id(), nanos));
		let _ = remove_file(&path);
		Self {
			path,
		}
	}
}

impl AsRef<Path> for TempDbPath {
	fn as_ref(&self) -> &Path {
		&self.path
	}
}

impl Deref for TempDbPath {
	type Target = Path;

	fn deref(&self) -> &Path {
		&self.path
	}
}

impl Drop for TempDbPath {
	fn drop(&mut self) {
		let _ = remove_file(&self.path);
		for suffix in ["-shm", "-wal", "-journal"] {
			let mut companion = self.path.clone().into_os_string();
			companion.push(suffix);
			let _ = remove_file(PathBuf::from(companion));
		}
		let derived_dir = self.path.with_extension("");
		if derived_dir != self.path {
			let _ = remove_dir_all(&derived_dir);
		}
	}
}
