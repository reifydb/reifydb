// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[cfg(not(target_os = "linux"))]
use std::env;
use std::path::{Path, PathBuf};

use uuid::Uuid;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DbPath {
	File(PathBuf),
	Tmpfs(PathBuf),
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

/// Configuration for CDC SQLite storage backend
#[derive(Debug, Clone)]
pub struct SqliteCdcConfig {
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

impl SqliteCdcConfig {
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

	pub fn tmpfs() -> Self {
		Self {
			path: DbPath::Tmpfs(PathBuf::from(format!("/tmp/reifydb_cdc_tmpfs_{}.db", Uuid::new_v4()))),
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

	pub fn in_memory() -> Self {
		Self {
			path: DbPath::Memory(memory_dir().join(format!("reifydb_cdc_mem_{}.db", Uuid::new_v4()))),
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

	pub fn test() -> Self {
		Self {
			path: DbPath::Memory(memory_dir().join(format!("reifydb_cdc_test_{}.db", Uuid::new_v4()))),
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

	pub fn page_size(mut self, size: u32) -> Self {
		self.page_size = size;
		self
	}

	pub fn mmap_size(mut self, size: u64) -> Self {
		self.mmap_size = size;
		self
	}
}

impl Default for SqliteCdcConfig {
	fn default() -> Self {
		Self::new("cdc.reifydb")
	}
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SynchronousMode {
	Off,
	Normal,
	Full,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TempStore {
	Default,
	File,
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
