// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod anchor;
mod census;
mod checkpoint;
mod flush;
pub mod metrics;
pub mod schema;
pub mod sql;
mod state;

#[cfg(test)]
mod tests;

use std::sync::{
	Arc,
	atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

use reifydb_runtime::{
	shutdown::Shutdown,
	sync::mutex::{Mutex, MutexGuard},
};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_sqlite::{
	SqliteConfig, SqliteTempPathGuard,
	connection::{connect, convert_flags, resolve_db_path},
	pragma,
};
use reifydb_value::value::duration::Duration;
use rusqlite::Connection;
use tracing::instrument;

use crate::{
	filter::{ARMED_CAPACITY_KEYS, OperatorKeyFilter},
	sqlite::{schema::ensure_schema, state::state_exists},
};

const BUSY_TIMEOUT: Duration = Duration::from_milliseconds_const(200);

#[derive(Clone)]
pub struct SqliteOperatorStorage {
	inner: Arc<StoreInner>,
}

struct StoreInner {
	conn: Mutex<Option<Connection>>,
	readers: ReadPool,
	cache_hits: AtomicU64,
	cache_misses: AtomicU64,
	state_written: AtomicBool,
	filter: OperatorKeyFilter,
}

struct ReadPool {
	conns: Vec<Mutex<Option<Connection>>>,
	next: AtomicUsize,
}

impl ReadPool {
	fn acquire(&self) -> MutexGuard<'_, Option<Connection>> {
		let n = self.conns.len();
		let start = self.next.fetch_add(1, Ordering::Relaxed) % n;
		for i in 0..n {
			if let Some(guard) = self.conns[(start + i) % n].try_lock() {
				return guard;
			}
		}
		self.conns[start].lock()
	}

	fn shutdown(&self) {
		for slot in &self.conns {
			drop(slot.lock().take());
		}
	}
}

impl SqliteOperatorStorage {
	pub fn in_memory() -> (Self, SqliteTempPathGuard) {
		let (config, guard) = SqliteConfig::in_memory();
		(Self::new(config), guard)
	}

	#[cfg(not(target_arch = "wasm32"))]
	#[instrument(name = "store::operator::persistent::sqlite::new", level = "debug", skip(config), fields(
		db_path = ?config.path,
		page_size = config.page_size.as_ref().map(|size| size.as_bytes()),
		read_pool_size = config.read_pool_size,
		journal_mode = config.journal_mode.as_ref().map(|mode| mode.as_str())
	))]
	pub fn new(config: SqliteConfig) -> Self {
		let path = resolve_db_path(config.path.clone(), "operator.db");
		let flags = convert_flags(&config.flags);

		let conn = connect(&path, flags).expect("operator state database could not be opened");
		pragma::apply(&conn, &config).expect("operator state pragmas could not be applied");
		conn.busy_timeout(BUSY_TIMEOUT.to_std()).expect("operator state busy timeout could not be set");

		let pool_size = config.read_pool_size.max(1) as usize;
		let mut readers = Vec::with_capacity(pool_size);
		for _ in 0..pool_size {
			let reader = connect(&path, flags).expect("operator state read connection could not be opened");
			pragma::apply_read_only(&reader, &config)
				.expect("operator state read pragmas could not be applied");
			reader.busy_timeout(BUSY_TIMEOUT.to_std())
				.expect("operator state read busy timeout could not be set");
			readers.push(reader);
		}

		Self::with_connections(conn, readers)
	}

	fn with_connections(conn: Connection, readers: Vec<Connection>) -> Self {
		ensure_schema(&conn);
		let state_written = state_exists(&conn);
		let filter = if state_written {
			OperatorKeyFilter::new()
		} else {
			OperatorKeyFilter::armed(ARMED_CAPACITY_KEYS)
		};
		Self {
			inner: Arc::new(StoreInner {
				conn: Mutex::new(Some(conn)),
				readers: ReadPool {
					conns: readers.into_iter().map(|reader| Mutex::new(Some(reader))).collect(),
					next: AtomicUsize::new(0),
				},
				cache_hits: AtomicU64::new(0),
				cache_misses: AtomicU64::new(0),
				state_written: AtomicBool::new(state_written),
				filter,
			}),
		}
	}

	fn read_conn(&self) -> MutexGuard<'_, Option<Connection>> {
		if self.inner.readers.conns.is_empty() {
			return self.inner.conn.lock();
		}
		self.inner.readers.acquire()
	}

	pub(super) fn state_written(&self) -> bool {
		self.inner.state_written.load(Ordering::Acquire)
	}

	pub(super) fn mark_state_written(&self) {
		self.inner.state_written.store(true, Ordering::Release);
	}

	pub fn filter(&self) -> &OperatorKeyFilter {
		&self.inner.filter
	}
}

impl Shutdown for SqliteOperatorStorage {
	fn shutdown(&self) {
		self.inner.readers.shutdown();
		if let Some(conn) = self.inner.conn.lock().take() {
			let _ = conn.close();
		}
	}
}
