// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fs,
	fs::{File, OpenOptions},
	os::unix::fs::FileExt,
	path::{Path, PathBuf},
};

use crate::io::fs::{
	Create, Filesystem, FsError, Len, Mkdir, Open, OpenMut, Pread, Pwrite, ReadDir, Rename, Result, SyncData,
	SyncDir, Truncate, Unlink,
};

#[derive(Debug, Clone, Default)]
pub struct HostFs;

impl HostFs {
	pub fn new() -> Self {
		Self
	}
}

#[derive(Debug)]
pub struct HostFile {
	path: PathBuf,
	file: File,
}

#[derive(Debug)]
pub struct HostFileMut {
	path: PathBuf,
	file: File,
}

impl Filesystem for HostFs {
	type File = HostFile;
	type FileMut = HostFileMut;
}

impl Mkdir for HostFs {
	fn mkdir(&self, path: &Path) -> Result<()> {
		fs::create_dir(path).map_err(|err| FsError::from_io(path, err))
	}
}

impl Create for HostFs {
	fn create(&self, path: &Path, len: u64) -> Result<HostFileMut> {
		let file = OpenOptions::new()
			.create_new(true)
			.read(true)
			.write(true)
			.open(path)
			.map_err(|err| FsError::from_io(path, err))?;
		file.set_len(len).map_err(|err| FsError::from_io(path, err))?;
		Ok(HostFileMut {
			path: path.to_path_buf(),
			file,
		})
	}
}

impl Open for HostFs {
	fn open(&self, path: &Path) -> Result<HostFile> {
		let file = OpenOptions::new().read(true).open(path).map_err(|err| FsError::from_io(path, err))?;
		Ok(HostFile {
			path: path.to_path_buf(),
			file,
		})
	}
}

impl OpenMut for HostFs {
	fn open_mut(&self, path: &Path) -> Result<HostFileMut> {
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.open(path)
			.map_err(|err| FsError::from_io(path, err))?;
		Ok(HostFileMut {
			path: path.to_path_buf(),
			file,
		})
	}
}

impl ReadDir for HostFs {
	fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
		let mut entries = Vec::new();
		for entry in fs::read_dir(path).map_err(|err| FsError::from_io(path, err))? {
			let entry = entry.map_err(|err| FsError::from_io(path, err))?;
			entries.push(entry.path());
		}
		entries.sort();
		Ok(entries)
	}
}

impl Rename for HostFs {
	fn rename(&self, from: &Path, to: &Path) -> Result<()> {
		fs::rename(from, to).map_err(|err| FsError::from_io(from, err))
	}
}

impl Unlink for HostFs {
	fn unlink(&self, path: &Path) -> Result<()> {
		fs::remove_file(path).map_err(|err| FsError::from_io(path, err))
	}
}

impl SyncDir for HostFs {
	fn sync_dir(&self, path: &Path) -> Result<()> {
		File::open(path).and_then(|dir| dir.sync_all()).map_err(|err| FsError::from_io(path, err))
	}
}

impl Pread for HostFile {
	fn pread(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		self.file.read_at(buf, offset).map_err(|err| FsError::from_io(&self.path, err))
	}
}

impl Len for HostFile {
	fn len(&self) -> Result<u64> {
		self.file.metadata().map(|meta| meta.len()).map_err(|err| FsError::from_io(&self.path, err))
	}
}

impl Pread for HostFileMut {
	fn pread(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		self.file.read_at(buf, offset).map_err(|err| FsError::from_io(&self.path, err))
	}
}

impl Pwrite for HostFileMut {
	fn pwrite(&self, offset: u64, buf: &[u8]) -> Result<usize> {
		self.file.write_at(buf, offset).map_err(|err| FsError::from_io(&self.path, err))
	}
}

impl SyncData for HostFileMut {
	fn sync_data(&self) -> Result<()> {
		self.file.sync_data().map_err(|err| FsError::from_io(&self.path, err))
	}
}

impl Truncate for HostFileMut {
	fn truncate(&self, len: u64) -> Result<()> {
		self.file.set_len(len).map_err(|err| FsError::from_io(&self.path, err))
	}
}

impl Len for HostFileMut {
	fn len(&self) -> Result<u64> {
		self.file.metadata().map(|meta| meta.len()).map_err(|err| FsError::from_io(&self.path, err))
	}
}

#[cfg(test)]
mod tests {
	use std::{
		env, process,
		sync::atomic::{AtomicU64, Ordering},
	};

	use super::*;

	static COUNTER: AtomicU64 = AtomicU64::new(0);

	struct Scratch {
		root: PathBuf,
	}

	impl Scratch {
		fn new() -> Self {
			let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
			let root = env::temp_dir().join(format!("reifydb-host-fs-{}-{}", process::id(), seq));
			fs::create_dir_all(&root).unwrap();
			Self {
				root,
			}
		}

		fn path(&self, name: &str) -> PathBuf {
			self.root.join(name)
		}
	}

	impl Drop for Scratch {
		fn drop(&mut self) {
			let _ = fs::remove_dir_all(&self.root);
		}
	}

	#[test]
	fn test_create_preallocates_length() {
		// preallocation is what lets later appends write in place without ever moving the file end
		let scratch = Scratch::new();
		let fs = HostFs::new();
		let file = fs.create(&scratch.path("seg"), 4096).unwrap();
		assert_eq!(file.len().unwrap(), 4096);
	}

	#[test]
	fn test_pwrite_pread_roundtrip_at_offsets() {
		// unaligned offsets must behave exactly like aligned ones, otherwise record packing corrupts data
		let scratch = Scratch::new();
		let fs = HostFs::new();
		let file = fs.create(&scratch.path("seg"), 4096).unwrap();

		for offset in [0u64, 512, 700] {
			let payload = [offset as u8; 64];
			assert_eq!(file.pwrite(offset, &payload).unwrap(), 64);
			let mut buf = [0u8; 64];
			assert_eq!(file.pread(offset, &mut buf).unwrap(), 64);
			assert_eq!(buf, payload);
		}

		assert_eq!(file.len().unwrap(), 4096);
	}

	#[test]
	fn test_pread_past_end_is_short_not_error() {
		// recovery scans read speculatively past the tail and must see a short count, never a failure
		let scratch = Scratch::new();
		let fs = HostFs::new();
		let file = fs.create(&scratch.path("seg"), 16).unwrap();

		let mut buf = [0u8; 32];
		assert_eq!(file.pread(8, &mut buf).unwrap(), 8);
		assert_eq!(file.pread(16, &mut buf).unwrap(), 0);
		assert_eq!(file.pread(4096, &mut buf).unwrap(), 0);
	}

	#[test]
	fn test_sync_data_succeeds() {
		// a durable commit is only durable if this call reaches the disk and reports success
		let scratch = Scratch::new();
		let fs = HostFs::new();
		let file = fs.create(&scratch.path("seg"), 64).unwrap();
		file.pwrite(0, b"durable").unwrap();
		file.sync_data().unwrap();
	}

	#[test]
	fn test_truncate_shortens_file() {
		// truncation is how a torn tail is discarded, so the reported length must follow immediately
		let scratch = Scratch::new();
		let fs = HostFs::new();
		let file = fs.create(&scratch.path("seg"), 4096).unwrap();
		file.truncate(1000).unwrap();
		assert_eq!(file.len().unwrap(), 1000);
	}

	#[test]
	fn test_rename_moves_file_and_old_path_is_gone() {
		// atomic rename is the install step, and a lingering source path would leave a duplicate segment
		let scratch = Scratch::new();
		let fs = HostFs::new();
		let from = scratch.path("tmp");
		let to = scratch.path("final");

		fs.create(&from, 32).unwrap();
		fs.rename(&from, &to).unwrap();

		assert_eq!(fs.open(&to).unwrap().len().unwrap(), 32);
		assert_eq!(fs.open(&from).unwrap_err(), FsError::NotFound(from.clone()));
	}

	#[test]
	fn test_unlink_removes_file() {
		// garbage collection relies on the path being unresolvable right after the call returns
		let scratch = Scratch::new();
		let fs = HostFs::new();
		let path = scratch.path("seg");

		fs.create(&path, 32).unwrap();
		fs.unlink(&path).unwrap();

		assert_eq!(fs.open(&path).unwrap_err(), FsError::NotFound(path.clone()));
	}

	#[test]
	fn test_read_dir_returns_sorted_entries() {
		// another suite diffs this listing byte for byte against a different backend, so order is part of the
		// contract
		let scratch = Scratch::new();
		let fs = HostFs::new();

		for name in ["c", "a", "b"] {
			fs.create(&scratch.path(name), 0).unwrap();
		}

		let entries = fs.read_dir(&scratch.root).unwrap();
		assert_eq!(entries, vec![scratch.path("a"), scratch.path("b"), scratch.path("c")]);
	}

	#[test]
	fn test_mkdir_creates_and_rejects_existing() {
		// directory creation is the exclusive claim on a store location, so a second claim must be refused
		let scratch = Scratch::new();
		let fs = HostFs::new();
		let path = scratch.path("store");

		fs.mkdir(&path).unwrap();
		assert!(path.is_dir());
		assert_eq!(fs.mkdir(&path).unwrap_err(), FsError::AlreadyExists(path.clone()));
	}

	#[test]
	fn test_open_missing_path_is_not_found() {
		// a missing segment must be distinguishable from a broken one, so the kind is mapped, not flattened
		let scratch = Scratch::new();
		let fs = HostFs::new();
		let path = scratch.path("absent");

		assert_eq!(fs.open(&path).unwrap_err(), FsError::NotFound(path.clone()));
		assert_eq!(fs.open_mut(&path).unwrap_err(), FsError::NotFound(path.clone()));
	}

	#[test]
	fn test_create_existing_path_is_already_exists() {
		// create must never silently reopen and re-truncate a segment that already holds committed data
		let scratch = Scratch::new();
		let fs = HostFs::new();
		let path = scratch.path("seg");

		fs.create(&path, 128).unwrap();
		assert_eq!(fs.create(&path, 128).unwrap_err(), FsError::AlreadyExists(path.clone()));
		assert_eq!(fs.open(&path).unwrap().len().unwrap(), 128);
	}

	#[test]
	fn test_open_reads_and_open_mut_writes_same_file() {
		// the split handle is the whole point: reads go through a read-only descriptor, writes through a
		// separate one
		let scratch = Scratch::new();
		let fs = HostFs::new();
		let path = scratch.path("seg");

		fs.create(&path, 256).unwrap();

		let writer = fs.open_mut(&path).unwrap();
		assert_eq!(writer.pwrite(64, b"payload").unwrap(), 7);
		writer.sync_data().unwrap();

		let reader = fs.open(&path).unwrap();
		let mut buf = [0u8; 7];
		assert_eq!(reader.pread(64, &mut buf).unwrap(), 7);
		assert_eq!(&buf, b"payload");
		assert_eq!(reader.len().unwrap(), 256);
	}
}
