// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	path::{Path, PathBuf},
	sync::Arc,
};

use reifydb_runtime::sync::mutex::Mutex;

use crate::{
	error::{LogError, Result},
	io::{Handle, LogIo},
};

#[derive(Clone, Default)]
pub struct MemoryIo(pub Arc<MemoryIoInner>);

#[derive(Default)]
pub struct MemoryIoInner {
	pub state: Mutex<MemoryState>,
}

pub struct MemoryState {
	pub files: BTreeMap<PathBuf, Arc<MemoryFile>>,
	pub dirs: BTreeSet<PathBuf>,
}

impl Default for MemoryState {
	fn default() -> Self {
		Self {
			files: BTreeMap::new(),
			dirs: BTreeSet::from([PathBuf::from("/")]),
		}
	}
}

impl MemoryState {
	pub fn exists(&self, path: &Path) -> bool {
		self.files.contains_key(path) || self.dirs.contains(path)
	}

	pub fn parent_is_dir(&self, path: &Path) -> bool {
		match path.parent() {
			Some(parent) => self.dirs.contains(parent),
			None => false,
		}
	}
}

impl MemoryIo {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn paths(&self) -> Vec<PathBuf> {
		self.0.state.lock().files.keys().cloned().collect()
	}

	pub fn dirs(&self) -> Vec<PathBuf> {
		self.0.state.lock().dirs.iter().cloned().collect()
	}

	pub fn contents(&self, path: &Path) -> Option<Vec<u8>> {
		self.0.state.lock().files.get(path).map(|file| file.data.lock().clone())
	}
}

impl LogIo for MemoryIo {
	fn mkdir(&self, path: &Path) -> Result<()> {
		let mut state = self.0.state.lock();
		if !state.parent_is_dir(path) {
			return Err(LogError::NotFound(path.to_path_buf()));
		}
		if state.exists(path) {
			return Err(LogError::AlreadyExists(path.to_path_buf()));
		}
		state.dirs.insert(path.to_path_buf());
		Ok(())
	}

	fn create(&self, path: &Path, len: u64) -> Result<Arc<dyn Handle>> {
		let mut state = self.0.state.lock();
		if !state.parent_is_dir(path) {
			return Err(LogError::NotFound(path.to_path_buf()));
		}
		if state.exists(path) {
			return Err(LogError::AlreadyExists(path.to_path_buf()));
		}
		let file = Arc::new(MemoryFile {
			data: Mutex::new(vec![0u8; len as usize]),
		});
		state.files.insert(path.to_path_buf(), Arc::clone(&file));
		Ok(file)
	}

	fn open(&self, path: &Path) -> Result<Arc<dyn Handle>> {
		let state = self.0.state.lock();
		state.files
			.get(path)
			.map(|file| Arc::clone(file) as Arc<dyn Handle>)
			.ok_or_else(|| LogError::NotFound(path.to_path_buf()))
	}

	fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
		let state = self.0.state.lock();
		if !state.dirs.contains(path) {
			return Err(LogError::NotFound(path.to_path_buf()));
		}
		let mut entries: Vec<PathBuf> = state
			.files
			.keys()
			.chain(state.dirs.iter())
			.filter(|entry| entry.parent() == Some(path))
			.cloned()
			.collect();
		entries.sort();
		Ok(entries)
	}

	fn rename(&self, from: &Path, to: &Path) -> Result<()> {
		let mut state = self.0.state.lock();
		if !state.files.contains_key(from) {
			return Err(LogError::NotFound(from.to_path_buf()));
		}
		if !state.parent_is_dir(to) {
			return Err(LogError::NotFound(to.to_path_buf()));
		}
		let file = state.files.remove(from).unwrap();
		state.files.insert(to.to_path_buf(), file);
		Ok(())
	}

	fn unlink(&self, path: &Path) -> Result<()> {
		let mut state = self.0.state.lock();
		state.files.remove(path).map(|_| ()).ok_or_else(|| LogError::NotFound(path.to_path_buf()))
	}

	fn sync_dir(&self, path: &Path) -> Result<()> {
		let state = self.0.state.lock();
		if !state.dirs.contains(path) {
			return Err(LogError::NotFound(path.to_path_buf()));
		}
		Ok(())
	}
}

#[derive(Default)]
pub struct MemoryFile {
	pub data: Mutex<Vec<u8>>,
}

impl Handle for MemoryFile {
	fn pread(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		let data = self.data.lock();
		let offset = offset as usize;
		if offset >= data.len() {
			return Ok(0);
		}
		let read = buf.len().min(data.len() - offset);
		buf[..read].copy_from_slice(&data[offset..offset + read]);
		Ok(read)
	}

	fn pwrite(&self, offset: u64, buf: &[u8]) -> Result<usize> {
		let mut data = self.data.lock();
		let offset = offset as usize;
		let end = offset + buf.len();
		if end > data.len() {
			data.resize(end, 0);
		}
		data[offset..end].copy_from_slice(buf);
		Ok(buf.len())
	}

	fn sync_data(&self) -> Result<()> {
		Ok(())
	}

	fn truncate(&self, len: u64) -> Result<()> {
		self.data.lock().resize(len as usize, 0);
		Ok(())
	}

	fn len(&self) -> Result<u64> {
		Ok(self.data.lock().len() as u64)
	}
}
