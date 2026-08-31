// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(reifydb_target = "host")]
pub mod host;
pub mod memory;
#[cfg(feature = "testing")]
pub mod testing;

use std::{
	error::Error as StdError,
	fmt,
	fmt::Display,
	io,
	path::{Path, PathBuf},
};

#[cfg(reifydb_target = "host")]
use crate::io::fs::host::{HostFile, HostFileMut, HostFs};
use crate::io::fs::memory::{MemoryFile, MemoryFileMut, MemoryFs};
#[cfg(feature = "testing")]
use crate::io::fs::testing::{TestingFile, TestingFileMut, TestingFs};

pub type Result<T> = std::result::Result<T, FsError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FsError {
	NotFound(PathBuf),
	AlreadyExists(PathBuf),
	NoSpace(PathBuf),
	NotADirectory(PathBuf),
	Io {
		path: PathBuf,
		message: String,
	},
}

impl FsError {
	pub fn from_io(path: &Path, err: io::Error) -> Self {
		match err.kind() {
			io::ErrorKind::NotFound => FsError::NotFound(path.to_path_buf()),
			io::ErrorKind::AlreadyExists => FsError::AlreadyExists(path.to_path_buf()),
			io::ErrorKind::StorageFull => FsError::NoSpace(path.to_path_buf()),
			io::ErrorKind::NotADirectory => FsError::NotADirectory(path.to_path_buf()),
			_ => FsError::Io {
				path: path.to_path_buf(),
				message: err.to_string(),
			},
		}
	}

	pub fn path(&self) -> &Path {
		match self {
			FsError::NotFound(path) => path,
			FsError::AlreadyExists(path) => path,
			FsError::NoSpace(path) => path,
			FsError::NotADirectory(path) => path,
			FsError::Io {
				path,
				..
			} => path,
		}
	}
}

impl Display for FsError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			FsError::NotFound(path) => write!(f, "not found: {}", path.display()),
			FsError::AlreadyExists(path) => write!(f, "already exists: {}", path.display()),
			FsError::NoSpace(path) => write!(f, "out of space: {}", path.display()),
			FsError::NotADirectory(path) => write!(f, "not a directory: {}", path.display()),
			FsError::Io {
				path,
				message,
			} => write!(f, "io error on {}: {}", path.display(), message),
		}
	}
}

impl StdError for FsError {}

pub trait Filesystem {
	type File: Pread + Len;
	type FileMut: Pread + Pwrite + SyncData + Truncate + Len;
}

pub trait Mkdir: Filesystem {
	fn mkdir(&self, path: &Path) -> Result<()>;
}

pub trait Create: Filesystem {
	fn create(&self, path: &Path, len: u64) -> Result<Self::FileMut>;
}

pub trait Open: Filesystem {
	fn open(&self, path: &Path) -> Result<Self::File>;
}

pub trait OpenMut: Filesystem {
	fn open_mut(&self, path: &Path) -> Result<Self::FileMut>;
}

pub trait ReadDir: Filesystem {
	fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>>;
}

pub trait Rename: Filesystem {
	fn rename(&self, from: &Path, to: &Path) -> Result<()>;
}

pub trait Unlink: Filesystem {
	fn unlink(&self, path: &Path) -> Result<()>;
}

pub trait SyncDir: Filesystem {
	fn sync_dir(&self, path: &Path) -> Result<()>;
}

pub trait Pread {
	fn pread(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;
}

pub trait Pwrite {
	fn pwrite(&self, offset: u64, buf: &[u8]) -> Result<usize>;
}

pub trait SyncData {
	fn sync_data(&self) -> Result<()>;
}

pub trait Truncate {
	fn truncate(&self, len: u64) -> Result<()>;
}

pub trait Len {
	fn len(&self) -> Result<u64>;

	fn is_empty(&self) -> Result<bool> {
		self.len().map(|len| len == 0)
	}
}

#[derive(Clone)]
pub enum Fs {
	#[cfg(reifydb_target = "host")]
	Host(HostFs),
	Memory(MemoryFs),
	#[cfg(feature = "testing")]
	Testing(TestingFs),
}

pub enum File {
	#[cfg(reifydb_target = "host")]
	Host(HostFile),
	Memory(MemoryFile),
	#[cfg(feature = "testing")]
	Testing(TestingFile),
}

pub enum FileMut {
	#[cfg(reifydb_target = "host")]
	Host(HostFileMut),
	Memory(MemoryFileMut),
	#[cfg(feature = "testing")]
	Testing(TestingFileMut),
}

impl Filesystem for Fs {
	type File = File;
	type FileMut = FileMut;
}

impl Mkdir for Fs {
	fn mkdir(&self, path: &Path) -> Result<()> {
		match self {
			#[cfg(reifydb_target = "host")]
			Fs::Host(fs) => fs.mkdir(path),
			Fs::Memory(fs) => fs.mkdir(path),
			#[cfg(feature = "testing")]
			Fs::Testing(fs) => fs.mkdir(path),
		}
	}
}

impl Create for Fs {
	fn create(&self, path: &Path, len: u64) -> Result<FileMut> {
		match self {
			#[cfg(reifydb_target = "host")]
			Fs::Host(fs) => fs.create(path, len).map(FileMut::Host),
			Fs::Memory(fs) => fs.create(path, len).map(FileMut::Memory),
			#[cfg(feature = "testing")]
			Fs::Testing(fs) => fs.create(path, len).map(FileMut::Testing),
		}
	}
}

impl Open for Fs {
	fn open(&self, path: &Path) -> Result<File> {
		match self {
			#[cfg(reifydb_target = "host")]
			Fs::Host(fs) => fs.open(path).map(File::Host),
			Fs::Memory(fs) => fs.open(path).map(File::Memory),
			#[cfg(feature = "testing")]
			Fs::Testing(fs) => fs.open(path).map(File::Testing),
		}
	}
}

impl OpenMut for Fs {
	fn open_mut(&self, path: &Path) -> Result<FileMut> {
		match self {
			#[cfg(reifydb_target = "host")]
			Fs::Host(fs) => fs.open_mut(path).map(FileMut::Host),
			Fs::Memory(fs) => fs.open_mut(path).map(FileMut::Memory),
			#[cfg(feature = "testing")]
			Fs::Testing(fs) => fs.open_mut(path).map(FileMut::Testing),
		}
	}
}

impl ReadDir for Fs {
	fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
		match self {
			#[cfg(reifydb_target = "host")]
			Fs::Host(fs) => fs.read_dir(path),
			Fs::Memory(fs) => fs.read_dir(path),
			#[cfg(feature = "testing")]
			Fs::Testing(fs) => fs.read_dir(path),
		}
	}
}

impl Rename for Fs {
	fn rename(&self, from: &Path, to: &Path) -> Result<()> {
		match self {
			#[cfg(reifydb_target = "host")]
			Fs::Host(fs) => fs.rename(from, to),
			Fs::Memory(fs) => fs.rename(from, to),
			#[cfg(feature = "testing")]
			Fs::Testing(fs) => fs.rename(from, to),
		}
	}
}

impl Unlink for Fs {
	fn unlink(&self, path: &Path) -> Result<()> {
		match self {
			#[cfg(reifydb_target = "host")]
			Fs::Host(fs) => fs.unlink(path),
			Fs::Memory(fs) => fs.unlink(path),
			#[cfg(feature = "testing")]
			Fs::Testing(fs) => fs.unlink(path),
		}
	}
}

impl SyncDir for Fs {
	fn sync_dir(&self, path: &Path) -> Result<()> {
		match self {
			#[cfg(reifydb_target = "host")]
			Fs::Host(fs) => fs.sync_dir(path),
			Fs::Memory(fs) => fs.sync_dir(path),
			#[cfg(feature = "testing")]
			Fs::Testing(fs) => fs.sync_dir(path),
		}
	}
}

impl Pread for File {
	fn pread(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		match self {
			#[cfg(reifydb_target = "host")]
			File::Host(file) => file.pread(offset, buf),
			File::Memory(file) => file.pread(offset, buf),
			#[cfg(feature = "testing")]
			File::Testing(file) => file.pread(offset, buf),
		}
	}
}

impl Len for File {
	fn len(&self) -> Result<u64> {
		match self {
			#[cfg(reifydb_target = "host")]
			File::Host(file) => file.len(),
			File::Memory(file) => file.len(),
			#[cfg(feature = "testing")]
			File::Testing(file) => file.len(),
		}
	}
}

impl Pread for FileMut {
	fn pread(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		match self {
			#[cfg(reifydb_target = "host")]
			FileMut::Host(file) => file.pread(offset, buf),
			FileMut::Memory(file) => file.pread(offset, buf),
			#[cfg(feature = "testing")]
			FileMut::Testing(file) => file.pread(offset, buf),
		}
	}
}

impl Pwrite for FileMut {
	fn pwrite(&self, offset: u64, buf: &[u8]) -> Result<usize> {
		match self {
			#[cfg(reifydb_target = "host")]
			FileMut::Host(file) => file.pwrite(offset, buf),
			FileMut::Memory(file) => file.pwrite(offset, buf),
			#[cfg(feature = "testing")]
			FileMut::Testing(file) => file.pwrite(offset, buf),
		}
	}
}

impl SyncData for FileMut {
	fn sync_data(&self) -> Result<()> {
		match self {
			#[cfg(reifydb_target = "host")]
			FileMut::Host(file) => file.sync_data(),
			FileMut::Memory(file) => file.sync_data(),
			#[cfg(feature = "testing")]
			FileMut::Testing(file) => file.sync_data(),
		}
	}
}

impl Truncate for FileMut {
	fn truncate(&self, len: u64) -> Result<()> {
		match self {
			#[cfg(reifydb_target = "host")]
			FileMut::Host(file) => file.truncate(len),
			FileMut::Memory(file) => file.truncate(len),
			#[cfg(feature = "testing")]
			FileMut::Testing(file) => file.truncate(len),
		}
	}
}

impl Len for FileMut {
	fn len(&self) -> Result<u64> {
		match self {
			#[cfg(reifydb_target = "host")]
			FileMut::Host(file) => file.len(),
			FileMut::Memory(file) => file.len(),
			#[cfg(feature = "testing")]
			FileMut::Testing(file) => file.len(),
		}
	}
}
