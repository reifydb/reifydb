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

use TransactionSingleVersion::SingleVersionLock;
pub use command::*;
pub use query::*;
use reifydb_core::{CommitVersion, EncodedKey, EncodedKeyRange, event::EventBus};
use reifydb_store_transaction::{
	MultiVersionContains, MultiVersionGet, MultiVersionRange, MultiVersionRangeRev, TransactionStore,
};

use crate::multi::transaction::version::StandardVersionProvider;

#[allow(clippy::module_inception)]
mod command;
pub(crate) mod query;

use crate::{
	multi::transaction::{Committed, TransactionManager},
	single::{TransactionSingleVersion, TransactionSvl},
};

pub struct TransactionSerializable(Arc<Inner>);

pub struct Inner {
	pub(crate) tm: TransactionManager<StandardVersionProvider>,
	pub(crate) store: TransactionStore,
	pub(crate) event_bus: EventBus,
}

impl Deref for TransactionSerializable {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Clone for TransactionSerializable {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl Inner {
	fn new(store: TransactionStore, single: TransactionSingleVersion, event_bus: EventBus) -> Self {
		let tm = TransactionManager::new(StandardVersionProvider::new(single).unwrap()).unwrap();

		Self {
			tm,
			store,
			event_bus,
		}
	}

	fn version(&self) -> crate::Result<CommitVersion> {
		self.tm.version()
	}
}

impl TransactionSerializable {
	pub fn testing() -> Self {
		let store = TransactionStore::testing_memory();
		let event_bus = EventBus::new();
		Self::new(store.clone(), SingleVersionLock(TransactionSvl::new(store, event_bus.clone())), event_bus)
	}
}

impl TransactionSerializable {
	pub fn new(store: TransactionStore, single: TransactionSingleVersion, event_bus: EventBus) -> Self {
		Self(Arc::new(Inner::new(store, single, event_bus)))
	}
}

impl TransactionSerializable {
	pub fn version(&self) -> crate::Result<CommitVersion> {
		self.0.version()
	}
	pub fn begin_query(&self) -> crate::Result<QueryTransaction> {
		QueryTransaction::new(self.clone(), None)
	}
}

impl TransactionSerializable {
	pub fn begin_command(&self) -> crate::Result<CommandTransaction> {
		CommandTransaction::new(self.clone())
	}
}

pub enum Transaction {
	Query(QueryTransaction),
	Command(CommandTransaction),
}

impl TransactionSerializable {
	pub fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<Committed>, reifydb_type::Error> {
		Ok(self.store.get(key, version)?.map(|sv| sv.into()))
	}

	pub fn contains_key(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool, reifydb_type::Error> {
		self.store.contains(key, version)
	}

	pub fn range_batched(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: u64,
	) -> reifydb_type::Result<<TransactionStore as MultiVersionRange>::RangeIter<'_>> {
		self.store.range_batched(range, version, batch_size)
	}

	pub fn range(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
	) -> reifydb_type::Result<<TransactionStore as MultiVersionRange>::RangeIter<'_>> {
		self.range_batched(range, version, 1024)
	}

	pub fn range_rev_batched(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: u64,
	) -> reifydb_type::Result<<TransactionStore as MultiVersionRangeRev>::RangeIterRev<'_>> {
		self.store.range_rev_batched(range, version, batch_size)
	}

	pub fn range_rev(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
	) -> reifydb_type::Result<<TransactionStore as MultiVersionRangeRev>::RangeIterRev<'_>> {
		self.range_rev_batched(range, version, 1024)
	}
}
