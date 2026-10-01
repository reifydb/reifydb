// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc as StdArc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_runtime::{
	io::fs::{Create, Filesystem, Mkdir, Open, OpenMut, SyncDir, Unlink, testing::Continue},
	sync::Arc,
};
use reifydb_value::{Result, error::Error};
use vortex_session::VortexSession;

use crate::{
	device::{BlockKey, Device, OpenBlock, RemoveBlock, WriteBlock},
	persist::BlockHandle,
	snapshot::ColumnBlock,
	store::FsColumnStore,
};

#[derive(Debug, PartialEq)]
pub enum WriteOutcome {
	Land,
	Err(Error),
}

#[derive(Debug, PartialEq)]
pub enum OpenOutcome {
	Honest,
	Err(Error),
}

#[derive(Debug, PartialEq)]
pub enum RemoveOutcome {
	Land,
	Err(Error),
}

pub trait ColumnHooks: Send + Sync {
	fn on_write(&self, _key: &BlockKey) -> WriteOutcome {
		WriteOutcome::Land
	}

	fn on_open(&self, _key: &BlockKey) -> OpenOutcome {
		OpenOutcome::Honest
	}

	fn on_remove(&self, _key: &BlockKey) -> RemoveOutcome {
		RemoveOutcome::Land
	}

	fn on_call(&self, _call: u64) -> Continue {
		Continue::Yes
	}
}

pub struct NoFaults;

impl ColumnHooks for NoFaults {}

struct Inner<F: Filesystem> {
	store: FsColumnStore<F>,
	hooks: StdArc<dyn ColumnHooks>,
	calls: AtomicU64,
}

pub struct TestingColumnStore<F: Filesystem>(Arc<Inner<F>>);

impl<F: Filesystem> Clone for TestingColumnStore<F> {
	fn clone(&self) -> Self {
		Self(Arc::clone(&self.0))
	}
}

impl<F: Filesystem> TestingColumnStore<F> {
	pub fn over(store: FsColumnStore<F>, hooks: StdArc<dyn ColumnHooks>) -> Self {
		Self(Arc::new(Inner {
			store,
			hooks,
			calls: AtomicU64::new(0),
		}))
	}

	pub fn session(&self) -> &VortexSession {
		self.0.store.session()
	}

	fn call(&self) {
		let call = self.0.calls.fetch_add(1, Ordering::SeqCst) + 1;
		if self.0.hooks.on_call(call) == Continue::Crash {
			panic!("simulated crash at column call {call}");
		}
	}
}

impl<F: Filesystem + Send + Sync + 'static> Device for TestingColumnStore<F> {}

impl<F> WriteBlock for TestingColumnStore<F>
where
	F: Filesystem + Create + Mkdir + OpenMut + SyncDir + Send + Sync + 'static,
{
	fn write(&self, key: &BlockKey, block: &ColumnBlock) -> Result<()> {
		self.call();
		match self.0.hooks.on_write(key) {
			WriteOutcome::Land => self.0.store.write(key, block),
			WriteOutcome::Err(error) => Err(error),
		}
	}
}

impl<F: Filesystem + Open + Send + Sync + 'static> OpenBlock for TestingColumnStore<F> {
	type File = F::File;

	fn open(&self, key: &BlockKey) -> Result<Option<BlockHandle<F::File>>> {
		self.call();
		match self.0.hooks.on_open(key) {
			OpenOutcome::Honest => self.0.store.open(key),
			OpenOutcome::Err(error) => Err(error),
		}
	}
}

impl<F: Filesystem + Unlink + Send + Sync + 'static> RemoveBlock for TestingColumnStore<F> {
	fn remove(&self, key: &BlockKey) -> Result<()> {
		self.call();
		match self.0.hooks.on_remove(key) {
			RemoveOutcome::Land => self.0.store.remove(key),
			RemoveOutcome::Err(error) => Err(error),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::{
		panic::{AssertUnwindSafe, catch_unwind},
		path::PathBuf,
	};

	use reifydb_core::value::column::factory;
	use reifydb_runtime::io::fs::{Fs, FsError, memory::MemoryFs};
	use reifydb_value::value::value_type::ValueType;

	use super::*;
	use crate::{compress::Compressor, error::ColumnError, session::new_session};

	const KEY: BlockKey = BlockKey {
		dir: 1,
		name: 1,
	};

	fn block(values: &[i32]) -> ColumnBlock {
		let column = Compressor::new(new_session())
			.compress(ValueType::Int4, &factory::int4("a", values.to_vec()))
			.unwrap();
		ColumnBlock::new(StdArc::new(vec![("a".to_string(), ValueType::Int4, false)]), vec![column])
	}

	fn injected() -> Error {
		ColumnError::Fs {
			operation: "remove",
			source: FsError::Io {
				path: PathBuf::from("/column/1/00000000000000000001.borg"),
				message: "injected".to_string(),
			},
		}
		.into()
	}

	struct FailRemove;

	impl ColumnHooks for FailRemove {
		fn on_remove(&self, _key: &BlockKey) -> RemoveOutcome {
			RemoveOutcome::Err(injected())
		}
	}

	struct CrashAt(u64);

	impl ColumnHooks for CrashAt {
		fn on_call(&self, call: u64) -> Continue {
			if call == self.0 {
				Continue::Crash
			} else {
				Continue::Yes
			}
		}
	}

	#[test]
	fn no_faults_passes_every_call_through() {
		let inner = FsColumnStore::new(Fs::Memory(MemoryFs::new()), PathBuf::from("/column")).unwrap();
		let store = TestingColumnStore::over(inner, StdArc::new(NoFaults));
		store.write(&KEY, &block(&[1, 2, 3])).unwrap();
		let read = store.open(&KEY).unwrap().expect("a written block must open").read(None).unwrap();
		assert_eq!(read.len(), 3, "the block must read back with every row");
		store.remove(&KEY).unwrap();
		assert!(store.open(&KEY).unwrap().is_none(), "a removed block must be gone");
	}

	#[test]
	fn a_failing_remove_returns_its_error_and_keeps_the_file() {
		let inner = FsColumnStore::new(Fs::Memory(MemoryFs::new()), PathBuf::from("/column")).unwrap();
		let store = TestingColumnStore::over(inner, StdArc::new(FailRemove));
		store.write(&KEY, &block(&[1, 2, 3])).unwrap();
		assert_eq!(store.remove(&KEY).err(), Some(injected()), "remove must return the hook's error");
		assert!(store.open(&KEY).unwrap().is_some(), "a failed remove must leave the file in place");
	}

	#[test]
	fn a_crash_hook_panics_at_the_named_call() {
		let inner = FsColumnStore::new(Fs::Memory(MemoryFs::new()), PathBuf::from("/column")).unwrap();
		let store = TestingColumnStore::over(inner, StdArc::new(CrashAt(3)));
		store.write(&KEY, &block(&[1, 2, 3])).unwrap();
		assert!(store.open(&KEY).unwrap().is_some(), "the second call must still pass");
		let payload =
			catch_unwind(AssertUnwindSafe(|| store.remove(&KEY))).expect_err("the third call must crash");
		assert_eq!(
			payload.downcast_ref::<String>().map(String::as_str),
			Some("simulated crash at column call 3"),
			"the crash must name the call it fired at"
		);
		assert!(store.open(&KEY).unwrap().is_some(), "a crash before the remove must leave the file in place");
	}
}
