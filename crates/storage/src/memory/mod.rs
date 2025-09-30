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
	interface::{Cdc, MultiVersionStorage, SingleVersionInsert, SingleVersionRemove, SingleVersionStorage},
	util::MultiVersionContainer,
	value::row::EncodedRow,
};
use write::{WriteCommand, Writer};

pub type MultiVersionRowContainer = MultiVersionContainer<EncodedRow>;

#[derive(Clone)]
pub struct Memory(Arc<MemoryInner>);

pub struct MemoryInner {
	multi: Arc<SkipMap<EncodedKey, MultiVersionRowContainer>>,
	single: Arc<SkipMap<EncodedKey, EncodedRow>>,
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

impl MultiVersionStorage for Memory {}
impl SingleVersionStorage for Memory {}
impl SingleVersionInsert for Memory {}
impl SingleVersionRemove for Memory {}
