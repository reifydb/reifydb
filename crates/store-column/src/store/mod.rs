// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::PathBuf;

#[cfg(reifydb_target = "host")]
use reifydb_runtime::io::fs::host::HostFs;
use reifydb_runtime::io::fs::{
	Create, File, Filesystem, Fs, FsError, Mkdir, Open, OpenMut, SyncData, SyncDir, Truncate, Unlink,
	memory::MemoryFs,
};
use reifydb_value::Result;
use vortex_session::VortexSession;

#[cfg(feature = "testing")]
use crate::testing::TestingColumnStore;
use crate::{
	device::{BlockKey, Device, OpenBlock, RemoveBlock, WriteBlock},
	error::{self, fs},
	persist::{BlockHandle, serialize_block, write_all},
	session::new_session,
	snapshot::ColumnBlock,
};

#[derive(Clone)]
pub struct FsColumnStore<F: Filesystem> {
	fs: F,
	root: PathBuf,
	session: VortexSession,
}

impl<F: Filesystem + Mkdir + SyncDir> FsColumnStore<F> {
	pub fn new(fs: F, root: PathBuf) -> Result<Self> {
		match fs.mkdir(&root) {
			Ok(()) => {
				if let Some(parent) = root.parent() {
					fs.sync_dir(parent).map_err(error::fs("new"))?;
				}
			}
			Err(FsError::AlreadyExists(_)) => {}
			Err(err) => return Err(error::fs("new")(err)),
		}
		Ok(Self {
			fs,
			root,
			session: new_session(),
		})
	}
}

impl<F: Filesystem> FsColumnStore<F> {
	pub fn session(&self) -> &VortexSession {
		&self.session
	}

	fn dir(&self, key: &BlockKey) -> PathBuf {
		self.root.join(key.dir.to_string())
	}

	fn path(&self, key: &BlockKey) -> PathBuf {
		self.dir(key).join(format!("{:020}.borg", key.name))
	}
}

impl<F: Filesystem + Send + Sync + 'static> Device for FsColumnStore<F> {}

impl<F> WriteBlock for FsColumnStore<F>
where
	F: Filesystem + Create + Mkdir + OpenMut + SyncDir + Send + Sync + 'static,
{
	fn write(&self, key: &BlockKey, block: &ColumnBlock) -> Result<()> {
		let bytes = serialize_block(block, &self.session)?;
		let dir = self.dir(key);
		match self.fs.mkdir(&dir) {
			Ok(()) => self.fs.sync_dir(&self.root).map_err(fs("write"))?,
			Err(FsError::AlreadyExists(_)) => {}
			Err(err) => return Err(fs("write")(err)),
		}
		let path = self.path(key);
		let len = bytes.len() as u64;
		let (file, new) = match self.fs.create(&path, len) {
			Ok(file) => (file, true),
			Err(FsError::AlreadyExists(_)) => {
				let file = self.fs.open_mut(&path).map_err(fs("write"))?;
				file.truncate(len).map_err(fs("write"))?;
				(file, false)
			}
			Err(err) => return Err(fs("write")(err)),
		};
		write_all(&file, &path, 0, &bytes).map_err(fs("write"))?;
		file.sync_data().map_err(fs("write"))?;
		if new {
			self.fs.sync_dir(&dir).map_err(fs("write"))?;
		}
		Ok(())
	}
}

impl<F: Filesystem + Open + Send + Sync + 'static> OpenBlock for FsColumnStore<F> {
	type File = F::File;

	fn open(&self, key: &BlockKey) -> Result<Option<BlockHandle<F::File>>> {
		let file = match self.fs.open(&self.path(key)) {
			Ok(file) => file,
			Err(FsError::NotFound(_)) => return Ok(None),
			Err(err) => return Err(fs("open")(err)),
		};
		BlockHandle::open(file, self.session.clone()).map(Some)
	}
}

impl<F: Filesystem + Unlink + Send + Sync + 'static> RemoveBlock for FsColumnStore<F> {
	fn remove(&self, key: &BlockKey) -> Result<()> {
		self.fs.unlink(&self.path(key)).map_err(fs("remove"))
	}
}

#[derive(Clone)]
pub enum ColumnStore {
	Store(FsColumnStore<Fs>),
	#[cfg(feature = "testing")]
	Testing(TestingColumnStore<Fs>),
}

impl ColumnStore {
	#[cfg(reifydb_target = "host")]
	pub fn host(dir: PathBuf) -> Result<Self> {
		Ok(Self::Store(FsColumnStore::new(Fs::Host(HostFs::new()), dir)?))
	}

	pub fn memory() -> Result<Self> {
		Ok(Self::Store(FsColumnStore::new(Fs::Memory(MemoryFs::new()), PathBuf::from("/column"))?))
	}

	pub fn write(&self, key: &BlockKey, block: &ColumnBlock) -> Result<()> {
		match self {
			Self::Store(store) => store.write(key, block),
			#[cfg(feature = "testing")]
			Self::Testing(store) => store.write(key, block),
		}
	}

	pub fn open(&self, key: &BlockKey) -> Result<Option<BlockHandle<File>>> {
		match self {
			Self::Store(store) => store.open(key),
			#[cfg(feature = "testing")]
			Self::Testing(store) => store.open(key),
		}
	}

	pub fn remove(&self, key: &BlockKey) -> Result<()> {
		match self {
			Self::Store(store) => store.remove(key),
			#[cfg(feature = "testing")]
			Self::Testing(store) => store.remove(key),
		}
	}

	pub fn session(&self) -> &VortexSession {
		match self {
			Self::Store(store) => store.session(),
			#[cfg(feature = "testing")]
			Self::Testing(store) => store.session(),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::{path::Path, sync::Arc};

	use reifydb_core::value::column::factory;
	use reifydb_runtime::io::fs::{
		Len, Pwrite,
		memory::SectorMask,
		testing::{FileId, ReadOutcome, TestingFs, TestingHooks},
	};
	use reifydb_value::value::{Value, column_view::ColumnView, value_type::ValueType};

	use super::*;
	use crate::{compress::Compressor, convert::to_arrow};

	const KEY: BlockKey = BlockKey {
		dir: 1,
		name: 1,
	};

	fn block(values: &[i32]) -> ColumnBlock {
		let column = Compressor::new(new_session())
			.compress(ValueType::Int4, &factory::int4("a", values.to_vec()))
			.unwrap();
		ColumnBlock::new(Arc::new(vec![("a".to_string(), ValueType::Int4, false)]), vec![column])
	}

	fn values(block: &ColumnBlock) -> Vec<Value> {
		let session = new_session();
		let column = &block.columns[0];
		let mut out = Vec::new();
		for chunk in &column.chunks {
			let exported = to_arrow(&session, "a", &column.field_type, chunk.clone()).unwrap();
			let view = ColumnView::try_from(&exported).unwrap();
			for row in 0..exported.1.len() {
				out.push(view.get_value(row));
			}
		}
		out
	}

	struct CorruptFirstSector;

	impl TestingHooks for CorruptFirstSector {
		fn on_pread(&self, _file: FileId, _offset: u64, _len: usize) -> ReadOutcome {
			let mut mask = SectorMask::empty(1);
			mask.set(0);
			ReadOutcome::Corrupt(mask)
		}
	}

	#[test]
	fn open_fails_on_a_garbage_file() {
		let memory = MemoryFs::new();
		let store = FsColumnStore::new(Fs::Memory(memory.clone()), PathBuf::from("/column")).unwrap();
		memory.mkdir(Path::new("/column/1")).unwrap();
		memory.create(Path::new("/column/1/00000000000000000001.borg"), 5)
			.unwrap()
			.pwrite(0, &[0xff; 5])
			.unwrap();
		match store.open(&KEY) {
			Err(err) => assert_eq!(err.0.code, "COL_019"),
			Ok(None) => panic!("a garbage file must fail open, not read as missing"),
			Ok(Some(_)) => panic!("a garbage file must fail open"),
		}
	}

	#[test]
	fn open_fails_on_a_corrupt_read() {
		let fs = Fs::Testing(TestingFs::new(MemoryFs::new(), Arc::new(CorruptFirstSector)));
		let store = FsColumnStore::new(fs, PathBuf::from("/column")).unwrap();
		store.write(&KEY, &block(&[1, 2, 3])).unwrap();
		match store.open(&KEY) {
			Err(err) => assert_eq!(err.0.code, "COL_019"),
			Ok(None) => panic!("a corrupt read must fail open, not read as missing"),
			Ok(Some(_)) => panic!("a corrupt read must fail open"),
		}
	}

	#[test]
	fn open_on_a_missing_key_returns_none() {
		let store = ColumnStore::memory().unwrap();
		store.write(&KEY, &block(&[1])).unwrap();
		let missing_file = BlockKey {
			dir: 1,
			name: 2,
		};
		let missing_dir = BlockKey {
			dir: 2,
			name: 1,
		};
		assert!(store.open(&missing_file).unwrap().is_none(), "a missing file in a known folder must be none");
		assert!(store.open(&missing_dir).unwrap().is_none(), "a key whose folder does not exist must be none");
	}

	#[test]
	fn a_rewrite_of_the_same_key_reads_back_only_the_new_rows() {
		let memory = MemoryFs::new();
		let store = FsColumnStore::new(Fs::Memory(memory.clone()), PathBuf::from("/column")).unwrap();
		let first = block(&(0..1000).map(|i| (i * 7919) % 1_000_003).collect::<Vec<_>>());
		let second = block(&[9, 10]);
		let second_len = serialize_block(&second, store.session()).unwrap().len() as u64;
		assert!(
			serialize_block(&first, store.session()).unwrap().len() as u64 > second_len,
			"the first block must be the larger one, or truncation goes untested"
		);
		store.write(&KEY, &first).unwrap();
		store.write(&KEY, &second).unwrap();

		let read = store.open(&KEY).unwrap().expect("the rewritten block must exist").read(None).unwrap();

		assert_eq!(values(&read), vec![Value::Int4(9), Value::Int4(10)]);
		let len = memory.open(Path::new("/column/1/00000000000000000001.borg")).unwrap().len().unwrap();
		assert_eq!(len, second_len, "the file must shrink to the new block");
	}
}
