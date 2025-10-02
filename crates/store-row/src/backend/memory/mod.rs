// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	ops::Deref,
	sync::{Arc, mpsc},
};

use mpsc::Sender;
pub use range::Range;
pub use range_rev::RangeRev;
pub use scan::MultiVersionIter;
pub use scan_rev::IterRev;

mod cdc;
mod commit;
mod contains;
mod get;
mod range;
mod range_rev;
mod scan;
mod scan_rev;
mod write;

use crossbeam_skiplist::SkipMap;
use reifydb_core::{
	CommitVersion, EncodedKey,
	interface::{Cdc, MultiVersionStore, SingleVersionInsert, SingleVersionRemove, SingleVersionStore},
	util::MultiVersionContainer,
	value::encoded::EncodedValues,
};
use write::{WriteCommand, Writer};

pub type MultiVersionRowContainer = MultiVersionContainer<EncodedValues>;

#[derive(Clone)]
pub struct Memory(Arc<MemoryInner>);

pub struct MemoryInner {
	multi: Arc<SkipMap<EncodedKey, MultiVersionRowContainer>>,
	single: Arc<SkipMap<EncodedKey, EncodedValues>>,
	cdcs: Arc<SkipMap<CommitVersion, Cdc>>,
	writer: Sender<WriteCommand>,
}

impl Deref for Memory {
	type Target = MemoryInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Drop for MemoryInner {
	fn drop(&mut self) {
		let _ = self.writer.send(WriteCommand::Shutdown);
	}
}

impl Default for Memory {
	fn default() -> Self {
		Self::new()
	}
}

impl Memory {
	pub fn new() -> Self {
		let multi = Arc::new(SkipMap::new());
		let single = Arc::new(SkipMap::new());
		let cdcs = Arc::new(SkipMap::new());

		let writer = Writer::spawn(multi.clone(), single.clone(), cdcs.clone())
			.expect("Failed to spawn memory writer thread");

		Self(Arc::new(MemoryInner {
			multi,
			single,
			cdcs,
			writer,
		}))
	}
}

impl MultiVersionStore for Memory {}
impl SingleVersionStore for Memory {}
impl SingleVersionInsert for Memory {}
impl SingleVersionRemove for Memory {}

// MemoryRowBackend wrapper for encoded store specific behavior
#[derive(Clone)]
pub struct MemoryRowBackend {
	inner: Memory,
	_size_limit: usize,
}

impl MemoryRowBackend {
	pub fn new(size_limit: usize) -> Self {
		Self {
			inner: Memory::new(),
			_size_limit: size_limit,
		}
	}

	pub fn get(
		&self,
		key: &EncodedKey,
		version: CommitVersion,
	) -> crate::Result<Option<reifydb_core::interface::MultiVersionValues>> {
		use reifydb_core::interface::MultiVersionGet;
		self.inner.get(key, version)
	}

	pub fn put(&self, _row: reifydb_core::interface::MultiVersionValues) -> crate::Result<()> {
		todo!("Implement put for MemoryRowBackend")
	}

	pub fn delete(&self, _key: &EncodedKey, _version: CommitVersion) -> crate::Result<()> {
		todo!("Implement delete for MemoryRowBackend")
	}

	pub fn range(
		&self,
		range: reifydb_core::EncodedKeyRange,
		version: CommitVersion,
	) -> crate::Result<Vec<reifydb_core::interface::MultiVersionValues>> {
		use reifydb_core::interface::MultiVersionRange;
		Ok(self.inner.range(range, version)?.collect())
	}

	pub fn count(&self) -> usize {
		todo!("Implement count for MemoryRowBackend")
	}

	pub fn name(&self) -> &str {
		"memory"
	}

	pub fn is_available(&self) -> bool {
		true
	}
}
