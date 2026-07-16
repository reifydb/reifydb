// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::byte_size::ByteSize;
use rusqlite::ffi::sqlite3_memory_used;

pub fn global_memory_used() -> ByteSize {
	// SAFETY: sqlite3_memory_used takes no arguments and dereferences no caller-supplied pointers; the
	// bundled SQLite is compiled thread-safe, so reading its global allocation counter is sound from any
	// thread at any time, including before the first connection is opened (it then reports zero).
	let used = unsafe { sqlite3_memory_used() };
	ByteSize::from_bytes(used.max(0) as u64)
}
