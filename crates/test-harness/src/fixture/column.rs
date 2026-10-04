// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
		mpsc::{Receiver, Sender, channel},
	},
};

#[cfg(reifydb_target = "host")]
use reifydb_runtime::io::fs::host::HostFs;
use reifydb_runtime::{
	io::fs::{
		Fs, FsError,
		memory::{MemoryFs, SectorMask},
		testing::{FileId, ReadOutcome, SyncOutcome, TestingFs, TestingHooks, WriteOutcome},
	},
	sync::mutex::Mutex,
};
use reifydb_store_column::{
	device::BlockKey,
	store::{ColumnStore, FsColumnStore},
	testing::{ColumnHooks, RemoveOutcome, TestingColumnStore},
};
use reifydb_value::{Result, value::duration::Duration};

pub const MEMORY_ROOT: &str = "/column";

pub fn memory_store(fs: MemoryFs, fs_hooks: Arc<dyn TestingHooks>, hooks: Arc<dyn ColumnHooks>) -> Result<ColumnStore> {
	let store = FsColumnStore::new(Fs::Testing(TestingFs::new(fs, fs_hooks)), PathBuf::from(MEMORY_ROOT))?;
	Ok(ColumnStore::Testing(TestingColumnStore::over(store, hooks)))
}

#[cfg(reifydb_target = "host")]
pub fn host_store(dir: PathBuf, hooks: Arc<dyn ColumnHooks>) -> Result<ColumnStore> {
	let store = FsColumnStore::new(Fs::Host(HostFs::new()), dir)?;
	Ok(ColumnStore::Testing(TestingColumnStore::over(store, hooks)))
}

pub fn block_path(root: &Path, key: &BlockKey) -> PathBuf {
	root.join(key.dir.to_string()).join(format!("{:020}.borg", key.name))
}

fn first_sector() -> SectorMask {
	let mut mask = SectorMask::empty(1);
	mask.set(0);
	mask
}

pub struct TornWrites;

impl TestingHooks for TornWrites {
	fn on_pwrite(&self, _file: FileId, _offset: u64, _len: usize) -> WriteOutcome {
		WriteOutcome::Torn(first_sector())
	}
}

pub struct CorruptReads;

impl TestingHooks for CorruptReads {
	fn on_pread(&self, _file: FileId, _offset: u64, _len: usize) -> ReadOutcome {
		ReadOutcome::Corrupt(first_sector())
	}
}

pub struct LyingSync;

impl TestingHooks for LyingSync {
	fn on_sync(&self, _file: FileId) -> SyncOutcome {
		SyncOutcome::Lying
	}
}

pub struct SyncDirLog(Mutex<Vec<PathBuf>>);

impl Default for SyncDirLog {
	fn default() -> Self {
		Self(Mutex::new(Vec::new()))
	}
}

impl SyncDirLog {
	pub fn paths(&self) -> Vec<PathBuf> {
		self.0.lock().clone()
	}
}

impl TestingHooks for SyncDirLog {
	fn on_sync_dir(&self, path: &Path) -> SyncOutcome {
		self.0.lock().push(path.to_path_buf());
		SyncOutcome::Honest
	}
}

pub struct FailWrites;

impl TestingHooks for FailWrites {
	fn on_pwrite(&self, _file: FileId, _offset: u64, _len: usize) -> WriteOutcome {
		WriteOutcome::Err(FsError::NoSpace(PathBuf::from(MEMORY_ROOT)))
	}
}

pub struct Pause<T> {
	reached: Receiver<T>,
	release: Sender<()>,
}

impl<T> Pause<T> {
	pub fn reached(&self, timeout: Duration) -> Option<T> {
		self.reached.recv_timeout(timeout.to_std()).ok()
	}

	pub fn release(&self) {
		let _ = self.release.send(());
	}
}

struct Hold<T> {
	target: AtomicU64,
	channels: Mutex<Option<(Sender<T>, Receiver<()>)>>,
}

impl<T> Hold<T> {
	fn new() -> (Self, Pause<T>) {
		let (reached_tx, reached_rx) = channel();
		let (release_tx, release_rx) = channel();
		let hold = Self {
			target: AtomicU64::new(0),
			channels: Mutex::new(Some((reached_tx, release_rx))),
		};
		let pause = Pause {
			reached: reached_rx,
			release: release_tx,
		};
		(hold, pause)
	}

	fn target(&self, object: u64) {
		self.target.store(object, Ordering::SeqCst);
	}

	fn hold(&self, object: u64, at: T) {
		if self.target.load(Ordering::SeqCst) != object {
			return;
		}
		let taken = self.channels.lock().take();
		if let Some((reached, release)) = taken {
			let _ = reached.send(at);
			let _ = release.recv();
		}
	}
}

pub struct PauseOnRemove(Hold<u64>);

impl PauseOnRemove {
	pub fn new() -> (Self, Pause<u64>) {
		let (hold, pause) = Hold::new();
		(Self(hold), pause)
	}

	pub fn target(&self, object: u64) {
		self.0.target(object);
	}
}

impl ColumnHooks for PauseOnRemove {
	fn on_remove(&self, key: &BlockKey) -> RemoveOutcome {
		self.0.hold(key.dir, key.name);
		RemoveOutcome::Land
	}
}

pub struct PauseOnObjectSyncDir(Hold<PathBuf>);

impl PauseOnObjectSyncDir {
	pub fn new() -> (Self, Pause<PathBuf>) {
		let (hold, pause) = Hold::new();
		(Self(hold), pause)
	}

	pub fn target(&self, object: u64) {
		self.0.target(object);
	}
}

impl TestingHooks for PauseOnObjectSyncDir {
	fn on_sync_dir(&self, path: &Path) -> SyncOutcome {
		if path.parent() == Some(Path::new(MEMORY_ROOT))
			&& let Some(object) =
				path.file_name().and_then(|name| name.to_str()).and_then(|name| name.parse().ok())
		{
			self.0.hold(object, path.to_path_buf());
		}
		SyncOutcome::Honest
	}
}
