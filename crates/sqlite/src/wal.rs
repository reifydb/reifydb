// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cell::{Cell, RefCell},
	collections::VecDeque,
	ffi::{c_char, c_int, c_void},
	ops::Deref,
	ptr,
	sync::{
		Arc,
		atomic::{AtomicU32, AtomicU64, Ordering},
	},
};

use rusqlite::{Connection, ffi};

use crate::error::{SqliteError, SqliteResult};

pub struct WalConnection {
	conn: Connection,
	frames: Box<AtomicU32>,
	threshold: Cell<u32>,
	markers: RefCell<VecDeque<(u32, u64)>>,
	progress: WalProgress,
}

impl WalConnection {
	pub fn new(conn: Connection, threshold: u32) -> SqliteResult<Self> {
		conn.pragma_update(None, "wal_autocheckpoint", 0).map_err(|source| SqliteError::Pragma {
			name: "wal_autocheckpoint".into(),
			source,
		})?;
		let frames = Box::new(AtomicU32::new(0));
		// SAFETY: the handle belongs to `conn`, which is open; the context points into the heap
		// allocation of `frames`, whose address is stable and which `Drop` keeps alive until the hook
		// is unregistered; the hook is registered on the only connection that can reach it, because
		// `WalConnection` never hands out `&mut Connection` or the connection by value.
		unsafe {
			ffi::sqlite3_wal_hook(
				conn.handle(),
				Some(record_frames),
				&*frames as *const AtomicU32 as *mut c_void,
			);
		}
		Ok(Self {
			conn,
			frames,
			threshold: Cell::new(threshold),
			markers: RefCell::new(VecDeque::new()),
			progress: WalProgress(Arc::new(AtomicU64::new(0))),
		})
	}

	pub fn committed(&self, marker: u64) {
		assert!(marker != u64::MAX, "wal marker u64::MAX is reserved and cannot be recorded");
		self.markers.borrow_mut().push_back((self.frames.load(Ordering::Acquire), marker));
	}

	pub fn checkpoint_if_due(&self) -> SqliteResult<()> {
		if self.frames.load(Ordering::Acquire) < self.threshold.get() {
			return Ok(());
		}
		self.checkpoint("PRAGMA wal_checkpoint(PASSIVE)")
	}

	pub fn shutdown(&self) -> SqliteResult<()> {
		self.checkpoint("PRAGMA wal_checkpoint(TRUNCATE)")?;
		self.conn.pragma_update(None, "cache_size", 0).map_err(|source| SqliteError::Pragma {
			name: "cache_size".into(),
			source,
		})
	}

	pub fn set_threshold(&self, frames: u32) {
		self.threshold.set(frames);
	}

	pub fn progress(&self) -> WalProgress {
		self.progress.clone()
	}

	fn checkpoint(&self, statement: &str) -> SqliteResult<()> {
		let (log, done) = self
			.conn
			.query_row(statement, [], |row| Ok((row.get::<_, i64>(1)?, row.get::<_, i64>(2)?)))
			.map_err(|source| SqliteError::Checkpoint {
				source,
			})?;
		self.advance(log, done);
		Ok(())
	}

	fn advance(&self, log: i64, done: i64) {
		if log < 0 {
			return;
		}
		let mut markers = self.markers.borrow_mut();
		let mut covered = None;
		while let Some(&(end, marker)) = markers.front() {
			if done != log && i64::from(end) > done {
				break;
			}
			covered = Some(marker);
			markers.pop_front();
		}
		if let Some(marker) = covered {
			self.progress.0.store(marker + 1, Ordering::Release);
		}
	}
}

impl Deref for WalConnection {
	type Target = Connection;

	fn deref(&self) -> &Connection {
		&self.conn
	}
}

impl Drop for WalConnection {
	fn drop(&mut self) {
		// SAFETY: `conn` is still open here because fields drop after this body; clearing the hook
		// before `frames` is freed means SQLite never calls back into a dangling context.
		unsafe {
			ffi::sqlite3_wal_hook(self.conn.handle(), None, ptr::null_mut());
		}
	}
}

#[derive(Clone)]
pub struct WalProgress(Arc<AtomicU64>);

impl WalProgress {
	pub fn checkpointed_through(&self) -> Option<u64> {
		match self.0.load(Ordering::Acquire) {
			0 => None,
			stored => Some(stored - 1),
		}
	}
}

// SAFETY: SQLite calls this only while the hook is registered, and `WalConnection::new` registers it
// with a context pointing at a live `AtomicU32` that outlives the registration.
unsafe extern "C" fn record_frames(
	context: *mut c_void,
	_db: *mut ffi::sqlite3,
	_name: *const c_char,
	frames: c_int,
) -> c_int {
	// SAFETY: see the function contract above; the pointer is non-null and aligned for `AtomicU32`.
	let frames_seen = unsafe { &*(context as *const AtomicU32) };
	frames_seen.store(frames.max(0) as u32, Ordering::Release);
	ffi::SQLITE_OK
}

#[cfg(test)]
mod tests {
	use std::{env::temp_dir, fs, path::PathBuf};

	use rusqlite::Connection;
	use uuid::Uuid;

	use super::WalConnection;

	fn scratch(name: &str) -> PathBuf {
		temp_dir().join(format!("reifydb_wal_{name}_{}.db", Uuid::new_v4()))
	}

	fn open(path: &PathBuf) -> Connection {
		let conn = Connection::open(path).unwrap();
		conn.pragma_update(None, "journal_mode", "wal").unwrap();
		conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
		conn
	}

	fn write(conn: &Connection, rows: usize) {
		conn.execute_batch("CREATE TABLE IF NOT EXISTS t (x BLOB)").unwrap();
		for _ in 0..rows {
			conn.execute("INSERT INTO t VALUES (randomblob(8192))", []).unwrap();
		}
	}

	fn wal_len(path: &PathBuf) -> u64 {
		let mut wal = path.clone().into_os_string();
		wal.push("-wal");
		fs::metadata(PathBuf::from(wal)).map(|meta| meta.len()).unwrap_or(0)
	}

	fn cleanup(path: PathBuf) {
		for suffix in ["", "-wal", "-shm"] {
			let mut file = path.clone().into_os_string();
			file.push(suffix);
			let _ = fs::remove_file(PathBuf::from(file));
		}
	}

	#[test]
	fn a_commit_is_not_reported_checkpointed_until_a_checkpoint_covers_it() {
		// A floor that moves on commit alone loses data on power loss under NORMAL.
		let path = scratch("commit");
		let wal = WalConnection::new(open(&path), 1).unwrap();
		write(&wal, 4);
		wal.committed(1);
		assert_eq!(wal.progress().checkpointed_through(), None, "a commit alone must not move progress");
		wal.checkpoint_if_due().unwrap();
		assert_eq!(wal.progress().checkpointed_through(), Some(1), "a full checkpoint must cover the commit");
		drop(wal);
		cleanup(path);
	}

	#[test]
	fn below_the_threshold_no_checkpoint_runs() {
		// Checkpointing early throws away the page merging the threshold exists to buy.
		let path = scratch("threshold");
		let wal = WalConnection::new(open(&path), u32::MAX).unwrap();
		write(&wal, 4);
		wal.committed(1);
		wal.checkpoint_if_due().unwrap();
		assert_eq!(wal.progress().checkpointed_through(), None, "no checkpoint may run below the threshold");
		wal.set_threshold(1);
		wal.checkpoint_if_due().unwrap();
		assert_eq!(wal.progress().checkpointed_through(), Some(1), "a lowered threshold must take effect");
		drop(wal);
		cleanup(path);
	}

	#[test]
	fn a_reader_pinning_old_frames_stops_progress_at_the_last_fully_backfilled_marker() {
		// A partial PASSIVE backfill must never report frames past the reader's snapshot as on disk.
		let path = scratch("partial");
		let wal = WalConnection::new(open(&path), 1).unwrap();
		write(&wal, 4);
		wal.committed(1);
		let reader = open(&path);
		reader.execute_batch("BEGIN").unwrap();
		let _: i64 = reader.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
		write(&wal, 4);
		wal.committed(2);
		wal.checkpoint_if_due().unwrap();
		assert_eq!(wal.progress().checkpointed_through(), Some(1), "a pinned reader must hold progress at 1");
		reader.execute_batch("COMMIT").unwrap();
		wal.checkpoint_if_due().unwrap();
		assert_eq!(
			wal.progress().checkpointed_through(),
			Some(2),
			"a released reader must let progress reach 2"
		);
		drop(reader);
		drop(wal);
		cleanup(path);
	}

	#[test]
	fn new_turns_off_sqlites_own_autocheckpoint() {
		// A surviving autocheckpoint runs inside COMMIT and backfills without moving progress.
		let path = scratch("autocheckpoint");
		let conn = open(&path);
		conn.pragma_update(None, "wal_autocheckpoint", 1000).unwrap();
		let wal = WalConnection::new(conn, u32::MAX).unwrap();
		let pages: i64 = wal.pragma_query_value(None, "wal_autocheckpoint", |r| r.get(0)).unwrap();
		assert_eq!(pages, 0, "the autocheckpoint pragma must read 0 once the hook owns checkpointing");
		drop(wal);
		cleanup(path);
	}

	#[test]
	fn shutdown_covers_every_marker_and_truncates_the_wal() {
		// Without this a clean stop leaves progress behind and every restart replays the tail.
		let path = scratch("shutdown");
		let wal = WalConnection::new(open(&path), u32::MAX).unwrap();
		write(&wal, 4);
		wal.committed(7);
		assert!(wal_len(&path) > 0, "the writes must sit in the wal before shutdown");
		wal.shutdown().unwrap();
		assert_eq!(wal.progress().checkpointed_through(), Some(7), "shutdown must cover the last marker");
		assert_eq!(wal_len(&path), 0, "shutdown must truncate the wal");
		drop(wal);
		cleanup(path);
	}
}
