// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod anchor;
mod census;
mod checkpoint;
pub mod filter;
mod flush;
pub mod metrics;
pub mod schema;
pub mod sql;
mod state;

#[cfg(test)]
mod tests;

use std::sync::{
	Arc,
	atomic::{AtomicBool, AtomicU64, Ordering},
};

use reifydb_runtime::{
	shutdown::Shutdown,
	sync::mutex::{Mutex, MutexGuard},
};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_store::{
	filter::KeyFilter,
	sqlite::{OpenMessages, open, pool::ReadPool},
};
use rusqlite::Connection;
use tracing::instrument;

use crate::tier::persistent::{
	filter::{ARMED_CAPACITY_ANCHORS, ARMED_CAPACITY_KEYS, OperatorAnchors, OperatorKeys},
	sqlite::{anchor::anchor_exists, schema::ensure_schema, state::state_exists},
};

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
	anchors_out_of_band: AtomicBool,
	filter: KeyFilter<OperatorKeys>,
	anchor_filter: KeyFilter<OperatorAnchors>,
}

const OPEN_MESSAGES: OpenMessages = OpenMessages {
	connect: "operator state database could not be opened",
	pragmas: "operator state pragmas could not be applied",
	busy_timeout: "operator state busy timeout could not be set",
	read_connect: "operator state read connection could not be opened",
	read_pragmas: "operator state read pragmas could not be applied",
	read_busy_timeout: "operator state read busy timeout could not be set",
};

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
		let (conn, readers) = open(&config, "operator.db", &OPEN_MESSAGES);
		Self::with_connections(conn, readers)
	}

	fn with_connections(conn: Connection, readers: ReadPool) -> Self {
		ensure_schema(&conn);
		let state_written = state_exists(&conn);
		let filter = if state_written {
			KeyFilter::<OperatorKeys>::new()
		} else {
			KeyFilter::<OperatorKeys>::armed(ARMED_CAPACITY_KEYS)
		};
		let anchors_preexisting = anchor_exists(&conn);
		let anchor_filter = if anchors_preexisting {
			KeyFilter::<OperatorAnchors>::new()
		} else {
			KeyFilter::<OperatorAnchors>::armed(ARMED_CAPACITY_ANCHORS)
		};
		Self {
			inner: Arc::new(StoreInner {
				conn: Mutex::new(Some(conn)),
				readers,
				cache_hits: AtomicU64::new(0),
				cache_misses: AtomicU64::new(0),
				state_written: AtomicBool::new(state_written),
				anchors_out_of_band: AtomicBool::new(anchors_preexisting),
				filter,
				anchor_filter,
			}),
		}
	}

	fn read_conn(&self) -> MutexGuard<'_, Option<Connection>> {
		if self.inner.readers.is_empty() {
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

	pub fn filter(&self) -> &KeyFilter<OperatorKeys> {
		&self.inner.filter
	}

	pub fn anchor_filter(&self) -> &KeyFilter<OperatorAnchors> {
		&self.inner.anchor_filter
	}

	pub fn anchors_out_of_band(&self) -> bool {
		self.inner.anchors_out_of_band.load(Ordering::Relaxed)
	}

	pub(crate) fn mark_anchors_out_of_band(&self) {
		self.inner.anchors_out_of_band.store(true, Ordering::Relaxed);
	}

	pub fn set_checkpoint_threshold(&self, frames: u32) {
		let guard = self.inner.conn.lock();
		let conn = guard.as_ref().expect("operator state wal_autocheckpoint ran without an open connection");
		conn.pragma_update(None, "wal_autocheckpoint", frames)
			.expect("operator state wal_autocheckpoint pragma could not be applied");
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
