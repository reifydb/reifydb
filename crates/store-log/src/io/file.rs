// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fs,
	fs::{File, OpenOptions},
	os::unix::fs::FileExt,
	path::{Path, PathBuf},
	sync::Arc,
};

use crate::{
	error::{LogError, Result},
	io::{Handle, LogIo},
};

#[derive(Debug, Default)]
pub struct FileIo;

impl FileIo {
	pub fn new() -> Self {
		Self
	}
}

impl LogIo for FileIo {
	fn mkdir(&self, path: &Path) -> Result<()> {
		fs::create_dir(path).map_err(|err| LogError::from_io(path, err))
	}

	fn create(&self, path: &Path, len: u64) -> Result<Arc<dyn Handle>> {
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create_new(true)
			.open(path)
			.map_err(|err| LogError::from_io(path, err))?;
		file.set_len(len).map_err(|err| LogError::from_io(path, err))?;
		Ok(Arc::new(FileHandle {
			path: path.to_path_buf(),
			file,
		}))
	}

	fn open(&self, path: &Path) -> Result<Arc<dyn Handle>> {
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.open(path)
			.map_err(|err| LogError::from_io(path, err))?;
		Ok(Arc::new(FileHandle {
			path: path.to_path_buf(),
			file,
		}))
	}

	fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
		let mut entries = Vec::new();
		for entry in fs::read_dir(path).map_err(|err| LogError::from_io(path, err))? {
			let entry = entry.map_err(|err| LogError::from_io(path, err))?;
			entries.push(entry.path());
		}
		entries.sort();
		Ok(entries)
	}

	fn rename(&self, from: &Path, to: &Path) -> Result<()> {
		fs::rename(from, to).map_err(|err| LogError::from_io(from, err))
	}

	fn unlink(&self, path: &Path) -> Result<()> {
		fs::remove_file(path).map_err(|err| LogError::from_io(path, err))
	}

	fn sync_dir(&self, path: &Path) -> Result<()> {
		File::open(path).and_then(|dir| dir.sync_all()).map_err(|err| LogError::from_io(path, err))
	}
}

#[derive(Debug)]
pub struct FileHandle {
	pub path: PathBuf,
	pub file: File,
}

impl Handle for FileHandle {
	fn pread(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		self.file.read_at(buf, offset).map_err(|err| LogError::from_io(&self.path, err))
	}

	fn pwrite(&self, offset: u64, buf: &[u8]) -> Result<usize> {
		self.file.write_at(buf, offset).map_err(|err| LogError::from_io(&self.path, err))
	}

	fn sync_data(&self) -> Result<()> {
		self.file.sync_data().map_err(|err| LogError::from_io(&self.path, err))
	}

	fn truncate(&self, len: u64) -> Result<()> {
		self.file.set_len(len).map_err(|err| LogError::from_io(&self.path, err))
	}

	fn len(&self) -> Result<u64> {
		self.file.metadata().map(|meta| meta.len()).map_err(|err| LogError::from_io(&self.path, err))
	}
}
