// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[cfg(not(target_os = "linux"))]
use std::env;
use std::path::{Path, PathBuf};

use uuid::Uuid;

#[cfg(not(target_arch = "wasm32"))]
pub mod connection;
#[cfg(not(target_arch = "wasm32"))]
pub mod error;
#[cfg(not(target_arch = "wasm32"))]
pub mod pragma;

/// Where the SQLite database file lives on disk.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DbPath {
	/// A regular file path.
	File(PathBuf),
	/// Tmpfs-backed file for WAL support + automatic cleanup.
	Tmpfs(PathBuf),
	/// RAM-backed file for storage with WAL support + automatic cleanup.
	Memory(PathBuf),
}

fn memory_dir() -> PathBuf {
	#[cfg(target_os = "linux")]
	{
		PathBuf::from("/dev/shm")
	}
	#[cfg(not(target_os = "linux"))]
	{
		env::temp_dir()
	}
}

/// Configuration for a SQLite storage backend.
#[derive(Debug, Clone)]
pub struct SqliteConfig {
	pub path: DbPath,
	pub flags: OpenFlags,
	pub journal_mode: JournalMode,
	pub synchronous_mode: SynchronousMode,
	pub temp_store: TempStore,
	pub cache_size: u32,
	pub wal_autocheckpoint: u32,
	pub page_size: u32,
	pub mmap_size: u64,
}

impl SqliteConfig {
	/// Create a new `SqliteConfig` with the specified database path.
	pub fn new<P: AsRef<Path>>(path: P) -> Self {
		Self {
			path: DbPath::File(path.as_ref().to_path_buf()),
			flags: OpenFlags::default(),
			journal_mode: JournalMode::Wal,
			synchronous_mode: SynchronousMode::Normal,
			temp_store: TempStore::Memory,
			cache_size: 20000,
			wal_autocheckpoint: 1000,
			page_size: 4096,
			mmap_size: 0,
		}
	}

	/// Safety-first configuration optimized for data integrity.
	/// - WAL journal mode for crash recovery
	/// - FULL synchronous mode for maximum durability
	/// - FILE temp store for persistence
	pub fn safe<P: AsRef<Path>>(path: P) -> Self {
		Self {
			path: DbPath::File(path.as_ref().to_path_buf()),
			flags: OpenFlags::default(),
			journal_mode: JournalMode::Wal,
			synchronous_mode: SynchronousMode::Full,
			temp_store: TempStore::File,
			cache_size: 20000,
			wal_autocheckpoint: 1000,
			page_size: 4096,
			mmap_size: 0,
		}
	}

	/// High-performance configuration optimized for speed.
	/// - MEMORY journal mode for fastest writes
	/// - OFF synchronous mode for minimal disk I/O
	/// - MEMORY temp store for fastest temp operations
	pub fn fast<P: AsRef<Path>>(path: P) -> Self {
		Self {
			path: DbPath::File(path.as_ref().to_path_buf()),
			flags: OpenFlags::default(),
			journal_mode: JournalMode::Memory,
			synchronous_mode: SynchronousMode::Off,
			temp_store: TempStore::Memory,
			cache_size: 10000,
			wal_autocheckpoint: 10000,
			page_size: 16384,
			mmap_size: 268435456,
		}
	}

	/// Tmpfs-backed configuration for temporary database storage.
	/// Uses /tmp which may or may not be tmpfs (system-dependent).
	pub fn tmpfs() -> Self {
		Self {
			path: DbPath::Tmpfs(PathBuf::from(format!("/tmp/reifydb_{}.db", Uuid::new_v4()))),
			flags: OpenFlags::default(),
			journal_mode: JournalMode::Wal,
			synchronous_mode: SynchronousMode::Off,
			temp_store: TempStore::Memory,
			cache_size: 20000,
			wal_autocheckpoint: 10000,
			page_size: 16384,
			mmap_size: 268435456,
		}
	}

	/// In-memory configuration for production use.
	/// Uses /dev/shm on Linux, temp dir on other platforms.
	pub fn in_memory() -> Self {
		Self {
			path: DbPath::Memory(memory_dir().join(format!("reifydb_{}.db", Uuid::new_v4()))),
			flags: OpenFlags::default(),
			journal_mode: JournalMode::Wal,
			synchronous_mode: SynchronousMode::Off,
			temp_store: TempStore::Memory,
			cache_size: 20000,
			wal_autocheckpoint: 10000,
			page_size: 16384,
			mmap_size: 268435456,
		}
	}

	/// Test configuration with an in-memory database.
	/// Uses /dev/shm on Linux, temp dir on other platforms.
	pub fn test() -> Self {
		Self {
			path: DbPath::Memory(memory_dir().join(format!("reifydb_{}.db", Uuid::new_v4()))),
			flags: OpenFlags::default(),
			journal_mode: JournalMode::Wal,
			synchronous_mode: SynchronousMode::Off,
			temp_store: TempStore::Memory,
			cache_size: 10000,
			wal_autocheckpoint: 10000,
			page_size: 4096,
			mmap_size: 0,
		}
	}

	pub fn path<P: AsRef<Path>>(mut self, path: P) -> Self {
		self.path = DbPath::File(path.as_ref().to_path_buf());
		self
	}

	pub fn flags(mut self, flags: OpenFlags) -> Self {
		self.flags = flags;
		self
	}

	pub fn journal_mode(mut self, mode: JournalMode) -> Self {
		self.journal_mode = mode;
		self
	}

	pub fn synchronous_mode(mut self, mode: SynchronousMode) -> Self {
		self.synchronous_mode = mode;
		self
	}

	pub fn temp_store(mut self, store: TempStore) -> Self {
		self.temp_store = store;
		self
	}

	pub fn cache_size(mut self, size_kb: u32) -> Self {
		self.cache_size = size_kb;
		self
	}

	pub fn wal_autocheckpoint(mut self, pages: u32) -> Self {
		self.wal_autocheckpoint = pages;
		self
	}

	/// Set the page size in bytes (must be a power of 2 between 512 and 65536).
	/// Must be set before the database is created; changing the page size
	/// on an existing database requires a VACUUM.
	pub fn page_size(mut self, size: u32) -> Self {
		self.page_size = size;
		self
	}

	/// Memory-mapped I/O size in bytes (0 = disabled).
	pub fn mmap_size(mut self, size: u64) -> Self {
		self.mmap_size = size;
		self
	}
}

impl Default for SqliteConfig {
	fn default() -> Self {
		Self::new("reifydb.db")
	}
}

/// SQLite database open flags.
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

impl OpenFlags {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn read_write(mut self, enabled: bool) -> Self {
		self.read_write = enabled;
		self
	}

	pub fn create(mut self, enabled: bool) -> Self {
		self.create = enabled;
		self
	}

	pub fn full_mutex(mut self, enabled: bool) -> Self {
		self.full_mutex = enabled;
		self.no_mutex = !enabled;
		self
	}

	pub fn no_mutex(mut self, enabled: bool) -> Self {
		self.no_mutex = enabled;
		self.full_mutex = !enabled;
		self
	}

	pub fn shared_cache(mut self, enabled: bool) -> Self {
		self.shared_cache = enabled;
		self.private_cache = !enabled;
		self
	}

	pub fn private_cache(mut self, enabled: bool) -> Self {
		self.private_cache = enabled;
		self.shared_cache = !enabled;
		self
	}

	pub fn uri(mut self, enabled: bool) -> Self {
		self.uri = enabled;
		self
	}
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

/// SQLite journal mode options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalMode {
	Delete,
	Truncate,
	Persist,
	Memory,
	Wal,
	Off,
}

impl JournalMode {
	pub fn as_str(&self) -> &'static str {
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

/// SQLite synchronous mode options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SynchronousMode {
	Off,
	Normal,
	Full,
	Extra,
}

impl SynchronousMode {
	pub fn as_str(&self) -> &'static str {
		match self {
			SynchronousMode::Off => "OFF",
			SynchronousMode::Normal => "NORMAL",
			SynchronousMode::Full => "FULL",
			SynchronousMode::Extra => "EXTRA",
		}
	}
}

/// SQLite temporary storage location.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TempStore {
	Default,
	File,
	Memory,
}

impl TempStore {
	pub fn as_str(&self) -> &'static str {
		match self {
			TempStore::Default => "DEFAULT",
			TempStore::File => "FILE",
			TempStore::Memory => "MEMORY",
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_testing::tempdir::temp_dir;

	use super::*;

	#[test]
	fn test_config_fluent_api() {
		let config = SqliteConfig::new("/tmp/test.reifydb")
			.journal_mode(JournalMode::Wal)
			.synchronous_mode(SynchronousMode::Normal)
			.temp_store(TempStore::Memory)
			.cache_size(30000)
			.flags(OpenFlags::new().read_write(true).create(true).full_mutex(true));

		assert_eq!(config.path, DbPath::File(PathBuf::from("/tmp/test.reifydb")));
		assert_eq!(config.journal_mode, JournalMode::Wal);
		assert_eq!(config.synchronous_mode, SynchronousMode::Normal);
		assert_eq!(config.temp_store, TempStore::Memory);
		assert_eq!(config.cache_size, 30000);
		assert!(config.flags.read_write);
		assert!(config.flags.create);
		assert!(config.flags.full_mutex);
	}

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
		assert_eq!(config.path, DbPath::File(PathBuf::from("reifydb.db")));
		assert_eq!(config.journal_mode, JournalMode::Wal);
		assert_eq!(config.synchronous_mode, SynchronousMode::Normal);
		assert_eq!(config.temp_store, TempStore::Memory);
	}

	#[test]
	fn test_safe_config() {
		temp_dir(|db_path| {
			let db_file = db_path.join("safe.reifydb");
			let config = SqliteConfig::safe(&db_file);

			assert_eq!(config.path, DbPath::File(db_file));
			assert_eq!(config.journal_mode, JournalMode::Wal);
			assert_eq!(config.synchronous_mode, SynchronousMode::Full);
			assert_eq!(config.temp_store, TempStore::File);
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_fast_config() {
		temp_dir(|db_path| {
			let db_file = db_path.join("fast.reifydb");
			let config = SqliteConfig::fast(&db_file);

			assert_eq!(config.path, DbPath::File(db_file));
			assert_eq!(config.journal_mode, JournalMode::Memory);
			assert_eq!(config.synchronous_mode, SynchronousMode::Off);
			assert_eq!(config.temp_store, TempStore::Memory);
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_tmpfs_config() {
		let config = SqliteConfig::tmpfs();

		match config.path {
			DbPath::Tmpfs(path) => {
				assert!(path.to_string_lossy().starts_with("/tmp/reifydb_"));
				assert!(path.to_string_lossy().ends_with(".db"));
			}
			_ => panic!("Expected DbPath::Tmpfs variant"),
		}

		assert_eq!(config.journal_mode, JournalMode::Wal);
		assert_eq!(config.synchronous_mode, SynchronousMode::Off);
		assert_eq!(config.temp_store, TempStore::Memory);
		assert_eq!(config.cache_size, 20000);
		assert_eq!(config.wal_autocheckpoint, 10000);
	}

	#[test]
	fn test_config_chaining() {
		temp_dir(|db_path| {
			let db_file = db_path.join("chain.reifydb");

			let config = SqliteConfig::new(&db_file)
				.journal_mode(JournalMode::Delete)
				.synchronous_mode(SynchronousMode::Extra)
				.temp_store(TempStore::File)
				.flags(OpenFlags::new().read_write(false).create(false).shared_cache(true));

			assert_eq!(config.journal_mode, JournalMode::Delete);
			assert_eq!(config.synchronous_mode, SynchronousMode::Extra);
			assert_eq!(config.temp_store, TempStore::File);
			assert!(!config.flags.read_write);
			assert!(!config.flags.create);
			assert!(config.flags.shared_cache);
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_open_flags_mutex_exclusivity() {
		let flags = OpenFlags::new().full_mutex(true);
		assert!(flags.full_mutex);
		assert!(!flags.no_mutex);

		let flags = OpenFlags::new().no_mutex(true);
		assert!(!flags.full_mutex);
		assert!(flags.no_mutex);
	}

	#[test]
	fn test_open_flags_cache_exclusivity() {
		let flags = OpenFlags::new().shared_cache(true);
		assert!(flags.shared_cache);
		assert!(!flags.private_cache);

		let flags = OpenFlags::new().private_cache(true);
		assert!(!flags.shared_cache);
		assert!(flags.private_cache);
	}

	#[test]
	fn test_open_flags_all_combinations() {
		let flags =
			OpenFlags::new().read_write(true).create(true).full_mutex(true).shared_cache(true).uri(true);

		assert!(flags.read_write);
		assert!(flags.create);
		assert!(flags.full_mutex);
		assert!(!flags.no_mutex);
		assert!(flags.shared_cache);
		assert!(!flags.private_cache);
		assert!(flags.uri);
	}

	#[test]
	fn test_path_handling() {
		temp_dir(|db_path| {
			let file_path = db_path.join("test.reifydb");
			let config = SqliteConfig::new(&file_path);
			assert_eq!(config.path, DbPath::File(file_path));

			let config = SqliteConfig::new(db_path);
			assert_eq!(config.path, DbPath::File(db_path.to_path_buf()));
			Ok(())
		})
		.expect("test failed");
	}
}
