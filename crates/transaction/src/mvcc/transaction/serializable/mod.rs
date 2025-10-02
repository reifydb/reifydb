// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{ops::Deref, sync::Arc};

pub use command::*;
pub use query::*;
use reifydb_core::{CommitVersion, EncodedKey, EncodedKeyRange, event::EventBus, interface::SingleVersionTransaction};
use reifydb_store_transaction::{MultiVersionStore, memory::MemoryBackend};

use crate::mvcc::transaction::version::StdVersionProvider;

#[allow(clippy::module_inception)]
mod command;
pub(crate) mod query;

use crate::{
	mvcc::transaction::{Committed, TransactionManager},
	svl::SingleVersionLock,
};

pub struct Serializable<MVS: MultiVersionStore, SVT: SingleVersionTransaction>(Arc<Inner<MVS, SVT>>);

pub struct Inner<MVS: MultiVersionStore, SVT: SingleVersionTransaction> {
	pub(crate) tm: TransactionManager<StdVersionProvider<SVT>>,
	pub(crate) multi: MVS,
	pub(crate) event_bus: EventBus,
}

impl<MVS: MultiVersionStore, SVT: SingleVersionTransaction> Deref for Serializable<MVS, SVT> {
	type Target = Inner<MVS, SVT>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl<MVS: MultiVersionStore, SVT: SingleVersionTransaction> Clone for Serializable<MVS, SVT> {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl<MVS: MultiVersionStore, SVT: SingleVersionTransaction> Inner<MVS, SVT> {
	fn new(multi: MVS, single: SVT, event_bus: EventBus) -> Self {
		let tm = TransactionManager::new(StdVersionProvider::new(single).unwrap()).unwrap();

		Self {
			tm,
			multi,
			event_bus,
		}
	}

	fn version(&self) -> crate::Result<CommitVersion> {
		self.tm.version()
	}
}

impl Serializable<MemoryBackend, SingleVersionLock<MemoryBackend>> {
	pub fn testing() -> Self {
		let memory = MemoryBackend::new();
		let event_bus = EventBus::new();
		Self::new(MemoryBackend::default(), SingleVersionLock::new(memory, event_bus.clone()), event_bus)
	}
}

impl<MVS: MultiVersionStore, SVT: SingleVersionTransaction> Serializable<MVS, SVT> {
	pub fn new(multi: MVS, single: SVT, event_bus: EventBus) -> Self {
		Self(Arc::new(Inner::new(multi, single, event_bus)))
	}
}

impl<MVS: MultiVersionStore, SVT: SingleVersionTransaction> Serializable<MVS, SVT> {
	pub fn version(&self) -> crate::Result<CommitVersion> {
		self.0.version()
	}
	pub fn begin_query(&self) -> crate::Result<QueryTransaction<MVS, SVT>> {
		QueryTransaction::new(self.clone(), None)
	}
}

impl<MVS: MultiVersionStore, SVT: SingleVersionTransaction> Serializable<MVS, SVT> {
	pub fn begin_command(&self) -> crate::Result<CommandTransaction<MVS, SVT>> {
		CommandTransaction::new(self.clone())
	}
}

pub enum Transaction<MVS: MultiVersionStore, SVT: SingleVersionTransaction> {
	Query(QueryTransaction<MVS, SVT>),
	Command(CommandTransaction<MVS, SVT>),
}

impl<MVS: MultiVersionStore, SVT: SingleVersionTransaction> Serializable<MVS, SVT> {
	pub fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<Committed>, reifydb_type::Error> {
		Ok(self.multi.get(key, version)?.map(|sv| sv.into()))
	}

	pub fn contains_key(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool, reifydb_type::Error> {
		self.multi.contains(key, version)
	}

	pub fn scan(&self, version: CommitVersion) -> Result<MVS::ScanIter<'_>, reifydb_type::Error> {
		self.multi.scan(version)
	}

	pub fn scan_rev(&self, version: CommitVersion) -> Result<MVS::ScanIterRev<'_>, reifydb_type::Error> {
		self.multi.scan_rev(version)
	}

	pub fn range(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
	) -> Result<MVS::RangeIter<'_>, reifydb_type::Error> {
		self.multi.range(range, version)
	}

	pub fn range_rev(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
	) -> Result<MVS::RangeIterRev<'_>, reifydb_type::Error> {
		self.multi.range_rev(range, version)
	}
}
