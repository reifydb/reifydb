// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	collections::{BTreeMap, HashMap},
	ops::Deref,
	sync::{Arc, mpsc},
};

use mpsc::Sender;
use parking_lot::RwLock;
use reifydb_core::{
	CommitVersion, EncodedKey,
	interface::{FlowNodeId, SourceId},
	value::encoded::EncodedValues,
};

use crate::cdc::InternalCdc;

mod cdc;
mod chain;
mod multi;
mod single;
mod write;

pub use cdc::{CdcRangeIter, CdcScanIter};
pub use chain::VersionChain;
pub use multi::{MultiVersionRangeIter, MultiVersionRangeRevIter};
pub use single::{SingleVersionRangeIter, SingleVersionRangeRevIter};
use write::{WriteCommand, Writer};

use crate::backend::{
	multi::BackendMultiVersion,
	single::{BackendSingleVersion, BackendSingleVersionRemove, BackendSingleVersionSet},
};

#[derive(Clone)]
pub struct MemoryBackend(Arc<MemoryBackendInner>);

pub struct MemoryBackendInner {
	sources: Arc<RwLock<HashMap<SourceId, BTreeMap<EncodedKey, VersionChain>>>>,
	operators: Arc<RwLock<HashMap<FlowNodeId, BTreeMap<EncodedKey, VersionChain>>>>,
	multi: Arc<RwLock<BTreeMap<EncodedKey, VersionChain>>>,
	single: Arc<RwLock<BTreeMap<EncodedKey, Option<EncodedValues>>>>,
	cdc: Arc<RwLock<BTreeMap<CommitVersion, InternalCdc>>>,
	writer: Sender<WriteCommand>,
}

impl Deref for MemoryBackend {
	type Target = MemoryBackendInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Drop for MemoryBackendInner {
	fn drop(&mut self) {
		let _ = self.writer.send(WriteCommand::Shutdown);
	}
}

impl Default for MemoryBackend {
	fn default() -> Self {
		Self::new()
	}
}

impl MemoryBackend {
	pub fn new() -> Self {
		let sources = Arc::new(RwLock::new(HashMap::new()));
		let operators = Arc::new(RwLock::new(HashMap::new()));
		let multi = Arc::new(RwLock::new(BTreeMap::new()));
		let single = Arc::new(RwLock::new(BTreeMap::new()));
		let cdc = Arc::new(RwLock::new(BTreeMap::new()));

		let writer =
			Writer::spawn(sources.clone(), operators.clone(), multi.clone(), single.clone(), cdc.clone())
				.expect("Failed to spawn memory writer thread");

		Self(Arc::new(MemoryBackendInner {
			sources,
			operators,
			multi,
			single,
			cdc,
			writer,
		}))
	}
}

impl BackendMultiVersion for MemoryBackend {}
impl BackendSingleVersion for MemoryBackend {}
impl BackendSingleVersionSet for MemoryBackend {}
impl BackendSingleVersionRemove for MemoryBackend {}
