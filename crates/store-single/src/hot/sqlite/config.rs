// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::path::{Path, PathBuf};

use uuid::Uuid;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DbPath {
	File(PathBuf),
	Tmpfs(PathBuf),  // tmpfs-backed file for WAL support + cleanup
	Memory(PathBuf), // /dev/shm-backed file for RAM-only storage with WAL support + cleanup
}

/// Configuration for SQLite storage backend
#[derive(Debug, Clone)]
pub struct SqliteConfig {
	pub path: DbPath,
	pub flags: OpenFlags,
	pub journal_mode: JournalMode,
	pub synchronous_mode: SynchronousMode,
	pub temp_store: TempStore,
	pub cache_size: u32,
	pub wal_autocheckpoint: u32,
	pub page_size: u32, // Page size in bytes (must be power of 2, 512-65536)
	pub mmap_size: u64, // Memory-mapped I/O size in bytes
}

impl SqliteConfig {
	/// Create a new SqliteConfig with the specified database path
	pub fn new<P: AsRef<Path>>(path: P) -> Self {
		Self {
			path: DbPath::File(path.as_ref().to_path_buf()),
			flags: OpenFlags::default(),
			journal_mode: JournalMode::Wal,
			synchronous_mode: SynchronousMode::Normal,
			temp_store: TempStore::Memory,
			cache_size: 20000,
			wal_autocheckpoint: 1000,
			page_size: 4096, // SQLite default
			mmap_size: 0,    // Disabled by default
		}
	}

	/// Create an in-memory configuration for production use
	/// - RAM-only database with WAL mode for concurrent access
	/// - Uses /dev/shm for guaranteed RAM-backed storage
	/// - WAL journal mode for concurrent readers + single writer
	/// - NORMAL synchronous mode (safe for RAM storage)
	/// - MEMORY temp store
	/// - Automatic cleanup on drop
	pub fn in_memory() -> Self {
		Self {
			path: DbPath::Memory(PathBuf::from(format!("/dev/shm/reifydb_mem_{}.db", Uuid::new_v4()))),
			flags: OpenFlags::default(),
			journal_mode: JournalMode::Wal,
			synchronous_mode: SynchronousMode::Off,
			temp_store: TempStore::Memory,
			cache_size: 20000,
			wal_autocheckpoint: 10000,
			page_size: 16384,     // Larger page size for bulk operations
			mmap_size: 268435456, // 256MB mmap for RAM-backed storage
		}
	}

	/// Create a test configuration optimized for testing with in-memory database
	/// - RAM-only database with WAL mode for concurrent access
	/// - Uses /dev/shm for guaranteed RAM-backed storage
	/// - WAL journal mode for concurrent readers + single writer
	/// - FULL synchronous mode for test safety
	/// - MEMORY temp store for fastest temp operations
	/// - Automatic cleanup on drop
	pub fn test() -> Self {
		Self {
			path: DbPath::Memory(PathBuf::from(format!("/dev/shm/reifydb_test_{}.db", Uuid::new_v4()))),
			flags: OpenFlags::default(),
			journal_mode: JournalMode::Wal,
			synchronous_mode: SynchronousMode::Off,
			temp_store: TempStore::Memory,
			cache_size: 10000,
			wal_autocheckpoint: 10000,
			page_size: 4096, // Default for tests
			mmap_size: 0,    // Disabled for tests
		}
	}
}

impl Default for SqliteConfig {
	fn default() -> Self {
		Self::new("reify.reifydb")
	}
}

/// SQLite database open flags
#[derive(Debug, Clone)]
pub struct OpenFlags {
	pub read_write: bool,
	pub create: bool,
	pub full_mutex: bool,
	pub no_mutex: bool,
	pub shared_cache: bool,
	pub private_cache: bool,
	pub uri: bool,
}

impl Default for OpenFlags {
	fn default() -> Self {
		Self {
			read_write: true,
			create: true,
			full_mutex: true,
			no_mutex: false,
			shared_cache: false,
			private_cache: false,
			uri: false,
		}
	}
}

/// SQLite journal mode options
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalMode {
	/// Delete journal files after each transaction
	Delete,
	/// Truncate journal files to zero length instead of deleting
	Truncate,
	/// Persist journal files
	Persist,
	/// Use memory for journaling
	Memory,
	/// Write-Ahead Logging mode (recommended for concurrent access)
	Wal,
	/// No journaling (unsafe)
	Off,
}

impl JournalMode {
	pub(crate) fn as_str(&self) -> &'static str {
		match self {
			JournalMode::Delete => "DELETE",
			JournalMode::Truncate => "TRUNCATE",
			JournalMode::Persist => "PERSIST",
			JournalMode::Memory => "MEMORY",
			JournalMode::Wal => "WAL",
			JournalMode::Off => "OFF",
		}
	}
}

/// SQLite synchronous mode options
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SynchronousMode {
	/// No sync calls (fastest, but may corrupt on power loss)
	Off,
	/// Sync only at critical moments (good balance of safety and speed)
	Normal,
	/// Sync more frequently (safer but slower)
	Full,
	/// Sync even more frequently (safest but slowest)
	Extra,
}

impl SynchronousMode {
	pub(crate) fn as_str(&self) -> &'static str {
		match self {
			SynchronousMode::Off => "OFF",
			SynchronousMode::Normal => "NORMAL",
			SynchronousMode::Full => "FULL",
			SynchronousMode::Extra => "EXTRA",
		}
	}
}

/// SQLite temporary storage location
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TempStore {
	/// Use default storage (usually disk)
	Default,
	/// Store temporary data in files
	File,
	/// Store temporary data in memory (faster)
	Memory,
}

impl TempStore {
	pub(crate) fn as_str(&self) -> &'static str {
		match self {
			TempStore::Default => "DEFAULT",
			TempStore::File => "FILE",
			TempStore::Memory => "MEMORY",
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_testing::tempdir::temp_dir;

	use super::*;

	#[test]
	fn test_enum_string_conversion() {
		assert_eq!(JournalMode::Wal.as_str(), "WAL");
		assert_eq!(SynchronousMode::Normal.as_str(), "NORMAL");
		assert_eq!(TempStore::Memory.as_str(), "MEMORY");
	}

	#[test]
	fn test_all_journal_modes() {
		assert_eq!(JournalMode::Delete.as_str(), "DELETE");
		assert_eq!(JournalMode::Truncate.as_str(), "TRUNCATE");
		assert_eq!(JournalMode::Persist.as_str(), "PERSIST");
		assert_eq!(JournalMode::Memory.as_str(), "MEMORY");
		assert_eq!(JournalMode::Wal.as_str(), "WAL");
		assert_eq!(JournalMode::Off.as_str(), "OFF");
	}

	#[test]
	fn test_all_synchronous_modes() {
		assert_eq!(SynchronousMode::Off.as_str(), "OFF");
		assert_eq!(SynchronousMode::Normal.as_str(), "NORMAL");
		assert_eq!(SynchronousMode::Full.as_str(), "FULL");
		assert_eq!(SynchronousMode::Extra.as_str(), "EXTRA");
	}

	#[test]
	fn test_all_temp_store_modes() {
		assert_eq!(TempStore::Default.as_str(), "DEFAULT");
		assert_eq!(TempStore::File.as_str(), "FILE");
		assert_eq!(TempStore::Memory.as_str(), "MEMORY");
	}

	#[test]
	fn test_default_config() {
		let config = SqliteConfig::default();
		assert_eq!(config.path, DbPath::File(PathBuf::from("reify.reifydb")));
		assert_eq!(config.journal_mode, JournalMode::Wal);
		assert_eq!(config.synchronous_mode, SynchronousMode::Normal);
		assert_eq!(config.temp_store, TempStore::Memory);
	}

	#[test]
	fn test_path_handling() {
		temp_dir(|db_path| {
			// Test with file path
			let file_path = db_path.join("test.reifydb");
			let config = SqliteConfig::new(&file_path);
			assert_eq!(config.path, DbPath::File(file_path));

			// Test with directory path
			let config = SqliteConfig::new(db_path);
			assert_eq!(config.path, DbPath::File(db_path.to_path_buf()));
			Ok(())
		})
		.expect("test failed");
	}
}
