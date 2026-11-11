// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	env,
	fs::{remove_dir_all, remove_file},
	ops::{Deref, DerefMut},
	path::{Path, PathBuf},
	process,
	sync::Arc,
	thread::sleep,
};

use reifydb_core::{
	commit::queue::{CommitPull, CommitQueue, CommitQueueFloor},
	common::CommitVersion,
	interface::commit::{CommitEntry, CommitLog},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	RuntimeConfig,
	context::clock::{Clock, MockClock},
	fatal::FatalConfig,
};
use reifydb_sqlite::SqliteTempPathGuard;
use reifydb_sub_flow::builder::FlowConfigurator;
use reifydb_test_harness::engine::AsEngine;
use reifydb_transaction::wal::{WalCommitLog, WalConfigurator};
use reifydb_value::{
	byte_size::ByteSize,
	value::{datetime::DateTime, duration::Duration as ValueDuration},
};
use reifydb_wal::lsn::Lsn;

use crate::{Database, Frame, Result, SqliteConfig, WithSubsystem, embedded};

pub struct TestDb {
	db: Database,
	_guard: Option<SqliteTempPathGuard>,
	held: Option<(CommitQueueFloor, CommitVersion, Lsn)>,
}

impl TestDb {
	pub fn builder() -> TestDbBuilder {
		TestDbBuilder::new()
	}

	pub fn memory() -> Self {
		Self::builder().memory()
	}

	pub fn hold_commits(&mut self) {
		let queue =
			self.db.engine()
				.ioc()
				.resolve::<CommitQueue>()
				.expect("the builder registers the commit queue");
		let (start, start_lsn) = queue.released();
		self.held = Some((queue.floor("test-history"), start, start_lsn));
	}

	pub fn commit_history(&self) -> Vec<Arc<CommitEntry>> {
		let queue =
			self.db.engine()
				.ioc()
				.resolve::<CommitQueue>()
				.expect("the builder registers the commit queue");
		let released = queue.released_through();
		let Some((_, start, start_lsn)) = self.held.as_ref() else {
			let log = WalCommitLog::new(self.db.engine().multi().wal().clone(), "test-history");
			return log
				.read((CommitVersion(0), Lsn::new(0)), released, ByteSize::from_bytes(u64::MAX))
				.expect("a database without hold_commits reads its history from the commit log")
				.entries;
		};
		match queue.pull(*start, released, ByteSize::from_bytes(u64::MAX)) {
			CommitPull::Hit {
				entries,
				..
			} => entries,
			CommitPull::Behind => {
				WalCommitLog::new(self.db.engine().multi().wal().clone(), "test-history")
					.read((*start, *start_lsn), released, ByteSize::from_bytes(u64::MAX))
					.expect(
						"a held commit history past the queue cap reads the rest from the commit log",
					)
					.entries
			}
		}
	}

	pub fn mock_clock(&self) -> &MockClock {
		self.db.engine().clock().as_mock().expect("TestDb was not built with a mock clock")
	}

	pub fn sqlite(config: SqliteConfig) -> Self {
		Self::builder().sqlite(config)
	}

	pub fn sqlite_at(path: impl AsRef<Path>) -> Self {
		Self::builder().sqlite_at(path)
	}

	pub fn sqlite_memory() -> Self {
		Self::builder().sqlite_memory()
	}

	fn wrap(db: Database, guard: Option<SqliteTempPathGuard>) -> Self {
		Self {
			db,
			_guard: guard,
			held: None,
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

	#[cfg(feature = "column")]
	pub fn query_column(&self, rql: &str) -> Vec<Frame> {
		self.db.query_column_as_root(rql, ()).unwrap()
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

	#[cfg(feature = "column")]
	pub fn try_query_column(&self, rql: &str) -> Result<Vec<Frame>> {
		self.db.query_column_as_root(rql, ())
	}

	pub fn row_count(&self, rql: &str) -> usize {
		self.query(rql).iter().map(|frame| frame.row_count()).sum()
	}

	pub fn await_row_count(&self, rql: &str, want: usize, timeout: impl Into<ValueDuration>) -> usize {
		let deadline = Clock::Real.instant() + timeout.into();
		loop {
			let got = self.row_count(rql);
			if got >= want || Clock::Real.instant() >= deadline {
				return got;
			}
			sleep(ValueDuration::from_milliseconds_const(20).to_std());
		}
	}

	pub fn await_exact_row_count(&self, rql: &str, want: usize, timeout: impl Into<ValueDuration>) -> usize {
		await_value(want, timeout, || self.row_count(rql))
	}

	pub fn await_all_flows(&self, timeout: impl Into<ValueDuration>) -> bool {
		let watermarks = self.db.watermarks();
		let target = watermarks.tx().current().expect("current commit version");
		watermarks
			.reader()
			.wait_for_flow_caught_up(target, timeout.into())
			.unwrap_or_else(|e| panic!("awaiting deferred flows failed: {e}"))
	}

	pub fn stop(&mut self) {
		self.db.stop().unwrap()
	}
}

pub struct TestDbBuilder {
	mock_at: Option<DateTime>,
	wal_configurator: Option<Box<dyn FnOnce(WalConfigurator) -> WalConfigurator + Send + 'static>>,
	flow_configurator: Option<Box<dyn FnOnce(FlowConfigurator) -> FlowConfigurator + Send + 'static>>,
}

impl TestDbBuilder {
	fn new() -> Self {
		Self {
			mock_at: None,
			wal_configurator: None,
			flow_configurator: None,
		}
	}

	pub fn mock_time(mut self, at: DateTime) -> Self {
		self.mock_at = Some(at);
		self
	}

	pub fn with_wal<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(WalConfigurator) -> WalConfigurator + Send + 'static,
	{
		self.wal_configurator = Some(Box::new(configurator));
		self
	}

	pub fn with_flow<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(FlowConfigurator) -> FlowConfigurator + Send + 'static,
	{
		self.flow_configurator = Some(Box::new(configurator));
		self
	}

	fn runtime_config(&self) -> RuntimeConfig {
		let config = RuntimeConfig::default().fatal(FatalConfig::disarmed());
		match self.mock_at {
			Some(at) => config.clock(Clock::Mock(MockClock::new(
				u64::try_from(at.to_nanos()).expect("mock clock cannot start before the Unix epoch"),
			))),
			None => config,
		}
	}

	pub fn memory(self) -> TestDb {
		let mut builder = embedded::memory().with_runtime_config(self.runtime_config());
		if let Some(configurator) = self.wal_configurator {
			builder = builder.with_wal(configurator);
		}
		if let Some(configurator) = self.flow_configurator {
			builder = builder.with_flow(configurator);
		}
		TestDb::wrap(builder.build().unwrap(), None)
	}

	pub fn sqlite(self, config: SqliteConfig) -> TestDb {
		let mut builder = embedded::sqlite(config).with_runtime_config(self.runtime_config());
		if let Some(configurator) = self.wal_configurator {
			builder = builder.with_wal(configurator);
		}
		if let Some(configurator) = self.flow_configurator {
			builder = builder.with_flow(configurator);
		}
		TestDb::wrap(builder.build().unwrap(), None)
	}

	pub fn sqlite_at(self, path: impl AsRef<Path>) -> TestDb {
		self.sqlite(SqliteConfig::new(path))
	}

	pub fn sqlite_memory(self) -> TestDb {
		let (config, guard) = SqliteConfig::in_memory();
		let mut builder = embedded::sqlite(config).with_runtime_config(self.runtime_config());
		if let Some(configurator) = self.wal_configurator {
			builder = builder.with_wal(configurator);
		}
		if let Some(configurator) = self.flow_configurator {
			builder = builder.with_flow(configurator);
		}
		TestDb::wrap(builder.build().unwrap(), Some(guard))
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
	fn engine(&self) -> &StandardEngine {
		self.db.engine()
	}
}

impl AsEngine for Database {
	fn engine(&self) -> &StandardEngine {
		self.engine()
	}
}

pub fn await_value<T: PartialEq>(want: T, timeout: impl Into<ValueDuration>, mut poll: impl FnMut() -> T) -> T {
	let deadline = Clock::Real.instant() + timeout.into();
	loop {
		let got = poll();
		if got == want || Clock::Real.instant() >= deadline {
			return got;
		}
		sleep(ValueDuration::from_milliseconds_const(20).to_std());
	}
}

pub fn poll_until<T>(mut poll: impl FnMut() -> Option<T>, timeout: impl Into<ValueDuration>) -> Option<T> {
	let deadline = Clock::Real.instant() + timeout.into();
	loop {
		if let Some(value) = poll() {
			return Some(value);
		}
		if Clock::Real.instant() >= deadline {
			return None;
		}
		sleep(ValueDuration::from_milliseconds_const(20).to_std());
	}
}

pub struct TempDbPath {
	path: PathBuf,
}

impl TempDbPath {
	pub fn new(tag: &str) -> Self {
		let nanos = Clock::Real.now().to_nanos();
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
