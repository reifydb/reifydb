// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use std::path::{Path, PathBuf};

/// Configuration for SQLite storage backend
#[derive(Debug, Clone)]
pub struct SqliteConfig {
	pub(crate) path: PathBuf,
	pub(crate) flags: OpenFlags,
	pub(crate) journal_mode: JournalMode,
	pub(crate) synchronous_mode: SynchronousMode,
	pub(crate) temp_store: TempStore,
	pub(crate) max_pool_size: u32,
}

impl SqliteConfig {
	/// Create a new SqliteConfig with the specified database path
	pub fn new<P: AsRef<Path>>(path: P) -> Self {
		Self {
			path: path.as_ref().to_path_buf(),
			flags: OpenFlags::default(),
			journal_mode: JournalMode::Wal,
			synchronous_mode: SynchronousMode::Normal,
			temp_store: TempStore::Memory,
			max_pool_size: 4,
		}
	}

	/// Create a safety-first configuration optimized for data integrity
	/// - WAL journal mode for crash recovery
	/// - FULL synchronous mode for maximum durability
	/// - FILE temp store for persistence
	/// - Conservative pool size
	pub fn safe<P: AsRef<Path>>(path: P) -> Self {
		Self {
			path: path.as_ref().to_path_buf(),
			flags: OpenFlags::default(),
			journal_mode: JournalMode::Wal,
			synchronous_mode: SynchronousMode::Full,
			temp_store: TempStore::File,
			max_pool_size: 2,
		}
	}

	/// Create a high-performance configuration optimized for speed
	/// - MEMORY journal mode for fastest writes
	/// - OFF synchronous mode for minimal disk I/O
	/// - MEMORY temp store for fastest temp operations
	/// - Larger pool size for concurrency
	pub fn fast<P: AsRef<Path>>(path: P) -> Self {
		Self {
			path: path.as_ref().to_path_buf(),
			flags: OpenFlags::default(),
			journal_mode: JournalMode::Memory,
			synchronous_mode: SynchronousMode::Off,
			temp_store: TempStore::Memory,
			max_pool_size: 8,
		}
	}

	/// Create a test configuration optimized for testing
	/// - WAL journal mode for concurrent test execution
	/// - Normal synchronous mode for reliability
	/// - MEMORY temp store for fastest temp operations
	/// - Small pool size to minimize locking issues
	pub fn test<P: AsRef<Path>>(path: P) -> Self {
		Self {
			path: path.as_ref().to_path_buf(),
			flags: OpenFlags::default(),
			journal_mode: JournalMode::Wal,
			synchronous_mode: SynchronousMode::Normal,
			temp_store: TempStore::Memory,
			max_pool_size: 2,
		}
	}

	/// Set the database file path
	pub fn path<P: AsRef<Path>>(mut self, path: P) -> Self {
		self.path = path.as_ref().to_path_buf();
		self
	}

	/// Set the SQLite open flags
	pub fn flags(mut self, flags: OpenFlags) -> Self {
		self.flags = flags;
		self
	}

	/// Set the journal mode
	pub fn journal_mode(mut self, mode: JournalMode) -> Self {
		self.journal_mode = mode;
		self
	}

	/// Set the synchronous mode
	pub fn synchronous_mode(mut self, mode: SynchronousMode) -> Self {
		self.synchronous_mode = mode;
		self
	}

	/// Set the temp store location
	pub fn temp_store(mut self, store: TempStore) -> Self {
		self.temp_store = store;
		self
	}

	/// Set the maximum connection pool size
	pub fn max_pool_size(mut self, size: u32) -> Self {
		self.max_pool_size = size;
		self
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
	pub(crate) read_write: bool,
	pub(crate) create: bool,
	pub(crate) full_mutex: bool,
	pub(crate) no_mutex: bool,
	pub(crate) shared_cache: bool,
	pub(crate) private_cache: bool,
	pub(crate) uri: bool,
}

impl OpenFlags {
	/// Create a new OpenFlags configuration
	pub fn new() -> Self {
		Self::default()
	}

	/// Enable read-write access (default: true)
	pub fn read_write(mut self, enabled: bool) -> Self {
		self.read_write = enabled;
		self
	}

	/// Enable creation of database if it doesn't exist (default: true)
	pub fn create(mut self, enabled: bool) -> Self {
		self.create = enabled;
		self
	}

	/// Use full mutex locking (default: true)
	pub fn full_mutex(mut self, enabled: bool) -> Self {
		self.full_mutex = enabled;
		self.no_mutex = !enabled;
		self
	}

	/// Disable mutex locking (default: false)
	pub fn no_mutex(mut self, enabled: bool) -> Self {
		self.no_mutex = enabled;
		self.full_mutex = !enabled;
		self
	}

	/// Enable shared cache (default: false)
	pub fn shared_cache(mut self, enabled: bool) -> Self {
		self.shared_cache = enabled;
		self.private_cache = !enabled;
		self
	}

	/// Enable private cache (default: false)
	pub fn private_cache(mut self, enabled: bool) -> Self {
		self.private_cache = enabled;
		self.shared_cache = !enabled;
		self
	}

	/// Enable URI filename interpretation (default: false)
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
mod tests {
	use reifydb_testing::tempdir::temp_dir;

	use super::*;

	#[test]
	fn test_config_fluent_api() {
		let config = SqliteConfig::new("/tmp/test.reifydb")
			.journal_mode(JournalMode::Wal)
			.synchronous_mode(SynchronousMode::Normal)
			.temp_store(TempStore::Memory)
			.max_pool_size(8)
			.flags(OpenFlags::new()
				.read_write(true)
				.create(true)
				.full_mutex(true));

		assert_eq!(config.path, PathBuf::from("/tmp/test.reifydb"));
		assert_eq!(config.journal_mode, JournalMode::Wal);
		assert_eq!(config.synchronous_mode, SynchronousMode::Normal);
		assert_eq!(config.temp_store, TempStore::Memory);
		assert_eq!(config.max_pool_size, 8);
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
		assert_eq!(config.path, PathBuf::from("reify.reifydb"));
		assert_eq!(config.journal_mode, JournalMode::Wal);
		assert_eq!(config.synchronous_mode, SynchronousMode::Normal);
		assert_eq!(config.temp_store, TempStore::Memory);
		assert_eq!(config.max_pool_size, 4);
	}

	#[test]
	fn test_safe_config() {
		temp_dir(|db_path| {
			let db_file = db_path.join("safe.reifydb");
			let config = SqliteConfig::safe(&db_file);

			assert_eq!(config.path, db_file);
			assert_eq!(config.journal_mode, JournalMode::Wal);
			assert_eq!(
				config.synchronous_mode,
				SynchronousMode::Full
			);
			assert_eq!(config.temp_store, TempStore::File);
			assert_eq!(config.max_pool_size, 2);
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_fast_config() {
		temp_dir(|db_path| {
			let db_file = db_path.join("fast.reifydb");
			let config = SqliteConfig::fast(&db_file);

			assert_eq!(config.path, db_file);
			assert_eq!(config.journal_mode, JournalMode::Memory);
			assert_eq!(
				config.synchronous_mode,
				SynchronousMode::Off
			);
			assert_eq!(config.temp_store, TempStore::Memory);
			assert_eq!(config.max_pool_size, 8);
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_config_chaining() {
		temp_dir(|db_path| {
			let db_file = db_path.join("chain.reifydb");

			let config = SqliteConfig::new(&db_file)
				.journal_mode(JournalMode::Delete)
				.synchronous_mode(SynchronousMode::Extra)
				.temp_store(TempStore::File)
				.max_pool_size(16)
				.flags(OpenFlags::new()
					.read_write(false)
					.create(false)
					.shared_cache(true));

			assert_eq!(config.journal_mode, JournalMode::Delete);
			assert_eq!(
				config.synchronous_mode,
				SynchronousMode::Extra
			);
			assert_eq!(config.temp_store, TempStore::File);
			assert_eq!(config.max_pool_size, 16);
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
		let flags = OpenFlags::new()
			.read_write(true)
			.create(true)
			.full_mutex(true)
			.shared_cache(true)
			.uri(true);

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
			// Test with file path
			let file_path = db_path.join("test.reifydb");
			let config = SqliteConfig::new(&file_path);
			assert_eq!(config.path, file_path);

			// Test with directory path
			let config = SqliteConfig::new(db_path);
			assert_eq!(config.path, db_path);
			Ok(())
		})
		.expect("test failed");
	}
}
