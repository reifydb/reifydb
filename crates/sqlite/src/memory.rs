// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::os::raw::c_int;

use reifydb_value::{byte_size::ByteSize, count::Count};
use rusqlite::{
	Connection,
	ffi::{
		SQLITE_DBSTATUS_CACHE_HIT, SQLITE_DBSTATUS_CACHE_MISS, SQLITE_DBSTATUS_CACHE_USED, SQLITE_OK,
		sqlite3_db_status, sqlite3_memory_used,
	},
};

pub fn global_memory_used() -> ByteSize {
	// SAFETY: sqlite3_memory_used takes no arguments and dereferences no caller-supplied pointers; the

	let used = unsafe { sqlite3_memory_used() };
	ByteSize::from_bytes(used.max(0) as u64)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConnectionCacheSweep {
	pub used: ByteSize,
	pub hits: Count,
	pub misses: Count,
}

pub fn sweep_connection_cache(conn: &Connection) -> ConnectionCacheSweep {
	ConnectionCacheSweep {
		used: ByteSize::from_bytes(db_status(conn, SQLITE_DBSTATUS_CACHE_USED, false)),
		hits: Count::new(db_status(conn, SQLITE_DBSTATUS_CACHE_HIT, true)),
		misses: Count::new(db_status(conn, SQLITE_DBSTATUS_CACHE_MISS, true)),
	}
}

fn db_status(conn: &Connection, op: c_int, reset: bool) -> u64 {
	let mut current: c_int = 0;
	let mut highwater: c_int = 0;
	// SAFETY: the handle is a live sqlite3* for the duration of the borrow of `conn`, and

	let rc = unsafe { sqlite3_db_status(conn.handle(), op, &mut current, &mut highwater, reset as c_int) };
	if rc == SQLITE_OK {
		current.max(0) as u64
	} else {
		0
	}
}

#[cfg(test)]
mod tests {
	use rusqlite::Connection;

	use super::sweep_connection_cache;

	#[test]
	fn sweep_resets_hit_and_miss_counters_but_not_used() {
		// The sweep is take-and-reset so that partial sweeps across a pool stay
		// additive: every hit/miss is handed out exactly once. `used` is the
		// instantaneous page-cache size and must survive the sweep.
		let conn = Connection::open_in_memory().expect("open in-memory db");
		conn.execute_batch(
			"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT); \
			 INSERT INTO t VALUES (1, 'a'), (2, 'b');",
		)
		.expect("seed table");
		let count: i64 = conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0)).expect("count");
		assert_eq!(count, 2);

		let first = sweep_connection_cache(&conn);
		assert!(
			first.hits.as_u64() + first.misses.as_u64() > 0,
			"reading a table must touch the page cache, got {first:?}"
		);
		assert!(first.used.as_bytes() > 0, "pages held by the connection must report as used");

		let second = sweep_connection_cache(&conn);
		assert_eq!(second.hits.as_u64(), 0, "hits must have been taken by the first sweep");
		assert_eq!(second.misses.as_u64(), 0, "misses must have been taken by the first sweep");
		assert!(second.used.as_bytes() > 0, "used is instantaneous and must not be reset by sweeping");
	}
}
