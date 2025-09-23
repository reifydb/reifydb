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
use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange,
	event::EventBus,
	interface::{MultiVersionStorage, SingleVersionTransaction},
};
use reifydb_storage::memory::Memory;

use crate::mvcc::transaction::version::StdVersionProvider;

#[allow(clippy::module_inception)]
mod command;
pub(crate) mod query;

use crate::{
	mvcc::transaction::{Committed, TransactionManager},
	svl::SingleVersionLock,
};

pub struct Serializable<MVS: MultiVersionStorage, SMVT: SingleVersionTransaction>(Arc<Inner<MVS, SMVT>>);

pub struct Inner<MVS: MultiVersionStorage, SMVT: SingleVersionTransaction> {
	pub(crate) tm: TransactionManager<StdVersionProvider<SMVT>>,
	pub(crate) multi: MVS,
	pub(crate) event_bus: EventBus,
}

impl<MVS: MultiVersionStorage, SMVT: SingleVersionTransaction> Deref for Serializable<MVS, SMVT> {
	type Target = Inner<MVS, SMVT>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl<MVS: MultiVersionStorage, SMVT: SingleVersionTransaction> Clone for Serializable<MVS, SMVT> {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl<MVS: MultiVersionStorage, SMVT: SingleVersionTransaction> Inner<MVS, SMVT> {
	fn new(multi: MVS, single: SMVT, event_bus: EventBus) -> Self {
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

impl Serializable<Memory, SingleVersionLock<Memory>> {
	pub fn testing() -> Self {
		let memory = Memory::new();
		let event_bus = EventBus::new();
		Self::new(Memory::default(), SingleVersionLock::new(memory, event_bus.clone()), event_bus)
	}
}

impl<MVS: MultiVersionStorage, SMVT: SingleVersionTransaction> Serializable<MVS, SMVT> {
	pub fn new(multi: MVS, single: SMVT, event_bus: EventBus) -> Self {
		Self(Arc::new(Inner::new(multi, single, event_bus)))
	}
}

impl<MVS: MultiVersionStorage, SMVT: SingleVersionTransaction> Serializable<MVS, SMVT> {
	pub fn version(&self) -> crate::Result<CommitVersion> {
		self.0.version()
	}
	pub fn begin_query(&self) -> crate::Result<QueryTransaction<MVS, SMVT>> {
		QueryTransaction::new(self.clone(), None)
	}
}

impl<MVS: MultiVersionStorage, SMVT: SingleVersionTransaction> Serializable<MVS, SMVT> {
	pub fn begin_command(&self) -> crate::Result<CommandTransaction<MVS, SMVT>> {
		CommandTransaction::new(self.clone())
	}
}

pub enum Transaction<MVS: MultiVersionStorage, SMVT: SingleVersionTransaction> {
	Query(QueryTransaction<MVS, SMVT>),
	Command(CommandTransaction<MVS, SMVT>),
}

impl<MVS: MultiVersionStorage, SMVT: SingleVersionTransaction> Serializable<MVS, SMVT> {
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
