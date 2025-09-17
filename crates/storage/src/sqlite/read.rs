// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	ops::Deref,
	path::{Path, PathBuf},
	sync::{Arc, Mutex, Weak},
};

use reifydb_type::{Error, Result};
use rusqlite::{Connection, OpenFlags};

use crate::diagnostic::connection_failed;

/// Type alias for a shared, thread-safe read connection
pub type ReadConnection = Arc<Mutex<Connection>>;

/// A reader connection that automatically returns itself to the pool when
/// dropped
pub struct Reader {
	conn: Option<ReadConnection>,
	pool: Weak<Mutex<Vec<ReadConnection>>>,
	pool_size: usize,
}

impl Reader {
	fn new(conn: ReadConnection, pool: Weak<Mutex<Vec<ReadConnection>>>, pool_size: usize) -> Self {
		Self {
			conn: Some(conn),
			pool,
			pool_size,
		}
	}

	/// Get access to the underlying connection
	pub fn conn(&self) -> &ReadConnection {
		self.conn.as_ref().expect("Reader connection already taken")
	}
}

impl Deref for Reader {
	type Target = ReadConnection;

	fn deref(&self) -> &Self::Target {
		self.conn()
	}
}

impl Drop for Reader {
	fn drop(&mut self) {
		if let Some(conn) = self.conn.take() {
			// Try to return the connection to the pool
			if let Some(pool) = self.pool.upgrade() {
				let mut pool = pool.lock().unwrap();
				if pool.len() < self.pool_size {
					pool.push(conn);
				} else {
					drop(conn);
				}
			} else {
				// If pool is gone (Readers struct dropped), let
				drop(conn);
			}
		}
	}
}

pub struct Readers {
	pool: Arc<Mutex<Vec<ReadConnection>>>,
	db_path: PathBuf,
	flags: OpenFlags,
	pool_size: usize,
}

impl Readers {
	pub fn new(db_path: &Path, flags: OpenFlags, pool_size: usize) -> Result<Self> {
		debug_assert!(pool_size > 0);

		let mut readers = Vec::new();

		for _ in 0..pool_size {
			let conn = Connection::open_with_flags(db_path, flags)
				.map_err(|e| Error(connection_failed(db_path.display().to_string(), e.to_string())))?;
			readers.push(Arc::new(Mutex::new(conn)));
		}

		Ok(Self {
			pool: Arc::new(Mutex::new(readers)),
			db_path: db_path.to_path_buf(),
			flags,
			pool_size,
		})
	}

	pub fn get_reader(&self) -> Reader {
		let mut pool = self.pool.lock().unwrap();

		// If we have an available connection, use it
		let conn = if let Some(conn) = pool.pop() {
			conn
		} else {
			// Create a new connection
			let conn = Connection::open_with_flags(&self.db_path, self.flags)
				.unwrap_or_else(|e| panic!("Failed to open reader connection: {}", e));
			Arc::new(Mutex::new(conn))
		};

		// Create a Reader with a weak reference to the pool
		Reader::new(conn, Arc::downgrade(&self.pool), self.pool_size)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_testing::tempdir::temp_dir;
	use rusqlite::OpenFlags;

	use super::super::read::*;

	#[test]
	fn test_reader_returns_to_pool() {
		temp_dir(|path| {
			let path = path.join("test.crates");

			let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;

			let readers = Readers::new(&path, flags, 2).expect("Failed to create readers");

			// Get a reader and check pool behavior
			{
				let reader1 = readers.get_reader();
				let reader2 = readers.get_reader();

				// Both readers should have connections
				assert!(reader1.conn().lock().is_ok());
				assert!(reader2.conn().lock().is_ok());

				// When these go out of scope, they should
				// return to pool
			}

			// Now get readers again - they should reuse pooled
			// connections
			{
				let reader3 = readers.get_reader();
				let reader4 = readers.get_reader();

				// These should work, reusing the returned
				// connections
				assert!(reader3.conn().lock().is_ok());
				assert!(reader4.conn().lock().is_ok());
			}

			// Test that connections are properly returned even
			// after multiple cycles
			for _ in 0..10 {
				let reader = readers.get_reader();
				assert!(reader.conn().lock().is_ok());
				// Reader drops here, returning connection to
				// pool
			}
			Ok(())
		})
		.expect("test to pass");
	}

	#[test]
	fn test_reader_pool_size_limit() {
		temp_dir(|path| {
			let path = path.join("test.crates");

			let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | { OpenFlags::SQLITE_OPEN_CREATE };

			let readers = Readers::new(&path, flags, 3).expect("Failed to create readers"); // Pool size of 3

			// Create readers that will be dropped
			let readers_vec: Vec<_> = (0..5).map(|_| readers.get_reader()).collect();

			// All should have valid connections
			for reader in &readers_vec {
				assert!(reader.conn().lock().is_ok());
			}

			// Drop all readers - only 3 should return to pool (pool
			// size limit)
			drop(readers_vec);

			// Get new readers - first 3 should be from pool
			let _r1 = readers.get_reader();
			let _r2 = readers.get_reader();
			let _r3 = readers.get_reader();
			let _r4 = readers.get_reader(); // This one creates new connection

			// All should work
			assert!(_r1.conn().lock().is_ok());
			assert!(_r2.conn().lock().is_ok());
			assert!(_r3.conn().lock().is_ok());
			assert!(_r4.conn().lock().is_ok());

			Ok(())
		})
		.expect("test to pass");
	}
}
