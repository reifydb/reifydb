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
	EncodedKey, EncodedKeyRange, Version,
	event::EventBus,
	interface::{UnversionedTransaction, VersionedStorage},
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

pub struct Serializable<VS: VersionedStorage, UT: UnversionedTransaction>(
	Arc<Inner<VS, UT>>,
);

pub struct Inner<VS: VersionedStorage, UT: UnversionedTransaction> {
	pub(crate) tm: TransactionManager<StdVersionProvider<UT>>,
	pub(crate) versioned: VS,
	pub(crate) event_bus: EventBus,
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> Deref
	for Serializable<VS, UT>
{
	type Target = Inner<VS, UT>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> Clone
	for Serializable<VS, UT>
{
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> Inner<VS, UT> {
	fn new(versioned: VS, unversioned: UT, event_bus: EventBus) -> Self {
		let tm = TransactionManager::new(
			StdVersionProvider::new(unversioned).unwrap(),
		)
		.unwrap();

		Self {
			tm,
			versioned,
			event_bus,
		}
	}

	fn version(&self) -> crate::Result<Version> {
		self.tm.version()
	}
}

impl Serializable<Memory, SingleVersionLock<Memory>> {
	pub fn testing() -> Self {
		let memory = Memory::new();
		let event_bus = EventBus::new();
		Self::new(
			Memory::default(),
			SingleVersionLock::new(memory, event_bus.clone()),
			event_bus,
		)
	}
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> Serializable<VS, UT> {
	pub fn new(
		versioned: VS,
		unversioned: UT,
		event_bus: EventBus,
	) -> Self {
		Self(Arc::new(Inner::new(versioned, unversioned, event_bus)))
	}
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> Serializable<VS, UT> {
	pub fn version(&self) -> crate::Result<Version> {
		self.0.version()
	}
	pub fn begin_query(&self) -> crate::Result<QueryTransaction<VS, UT>> {
		QueryTransaction::new(self.clone(), None)
	}
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> Serializable<VS, UT> {
	pub fn begin_command(
		&self,
	) -> crate::Result<CommandTransaction<VS, UT>> {
		CommandTransaction::new(self.clone())
	}
}

pub enum Transaction<VS: VersionedStorage, UT: UnversionedTransaction> {
	Rx(QueryTransaction<VS, UT>),
	Tx(CommandTransaction<VS, UT>),
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> Serializable<VS, UT> {
	pub fn get(
		&self,
		key: &EncodedKey,
		version: Version,
	) -> Result<Option<Committed>, reifydb_type::Error> {
		Ok(self.versioned.get(key, version)?.map(|sv| sv.into()))
	}

	pub fn contains_key(
		&self,
		key: &EncodedKey,
		version: Version,
	) -> Result<bool, reifydb_type::Error> {
		self.versioned.contains(key, version)
	}

	pub fn scan(
		&self,
		version: Version,
	) -> Result<VS::ScanIter<'_>, reifydb_type::Error> {
		self.versioned.scan(version)
	}

	pub fn scan_rev(
		&self,
		version: Version,
	) -> Result<VS::ScanIterRev<'_>, reifydb_type::Error> {
		self.versioned.scan_rev(version)
	}

	pub fn range(
		&self,
		range: EncodedKeyRange,
		version: Version,
	) -> Result<VS::RangeIter<'_>, reifydb_type::Error> {
		self.versioned.range(range, version)
	}

	pub fn range_rev(
		&self,
		range: EncodedKeyRange,
		version: Version,
	) -> Result<VS::RangeIterRev<'_>, reifydb_type::Error> {
		self.versioned.range_rev(range, version)
	}
}
