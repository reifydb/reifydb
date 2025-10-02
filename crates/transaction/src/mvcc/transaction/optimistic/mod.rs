// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{ops::Deref, sync::Arc, time::Duration};

pub use command::CommandTransaction;
pub use query::QueryTransaction;
use reifydb_core::{CommitVersion, EncodedKey, EncodedKeyRange, event::EventBus, interface::SingleVersionTransaction};
use reifydb_store_transaction::{
	BackendConfig, MultiVersionStore, StandardTransactionStore, TransactionStoreConfig,
	backend::{Backend, cdc::BackendCdc, multi::BackendMulti, single::BackendSingle},
	memory::MemoryBackend,
};

use crate::{
	mvcc::{
		transaction::{TransactionManager, version::StdVersionProvider},
		types::Committed,
	},
	svl::SingleVersionLock,
};

mod command;
mod query;

pub struct Optimistic<MVS: MultiVersionStore, SVT: SingleVersionTransaction>(Arc<Inner<MVS, SVT>>);

impl<MVS: MultiVersionStore, SVT: SingleVersionTransaction> Deref for Optimistic<MVS, SVT> {
	type Target = Inner<MVS, SVT>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl<MVS: MultiVersionStore, SVT: SingleVersionTransaction> Clone for Optimistic<MVS, SVT> {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

pub struct Inner<MVS: MultiVersionStore, SVT: SingleVersionTransaction> {
	pub(crate) tm: TransactionManager<StdVersionProvider<SVT>>,
	pub(crate) multi: MVS,
	pub(crate) event_bus: EventBus,
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

impl<MVS: MultiVersionStore, SVT: SingleVersionTransaction> Optimistic<MVS, SVT> {
	pub fn new(multi: MVS, single: SVT, event_bus: EventBus) -> Self {
		Self(Arc::new(Inner::new(multi, single, event_bus)))
	}
}

impl Optimistic<StandardTransactionStore, SingleVersionLock<StandardTransactionStore>> {
	pub fn testing() -> Self {
		let memory = MemoryBackend::new();
		let store = StandardTransactionStore::new(TransactionStoreConfig {
			hot: Some(BackendConfig {
				backend: Backend {
					multi: BackendMulti::Memory(memory.clone()),
					single: BackendSingle::Memory(memory.clone()),
					cdc: BackendCdc::Memory(memory),
				},
				retention_period: Duration::from_millis(100),
			}),
			warm: None,
			cold: None,
			retention: Default::default(),
			merge_config: Default::default(),
		})
		.unwrap();

		let event_bus = EventBus::new();
		Self::new(store.clone(), SingleVersionLock::new(store, event_bus.clone()), event_bus)
	}
}

impl<MVS: MultiVersionStore, SVT: SingleVersionTransaction> Optimistic<MVS, SVT> {
	pub fn version(&self) -> crate::Result<CommitVersion> {
		self.0.version()
	}
	pub fn begin_query(&self) -> crate::Result<QueryTransaction<MVS, SVT>> {
		QueryTransaction::new(self.clone(), None)
	}
}

impl<MVS: MultiVersionStore, SVT: SingleVersionTransaction> Optimistic<MVS, SVT> {
	pub fn begin_command(&self) -> crate::Result<CommandTransaction<MVS, SVT>> {
		CommandTransaction::new(self.clone())
	}
}

pub enum Transaction<MVS: MultiVersionStore, SVT: SingleVersionTransaction> {
	Query(QueryTransaction<MVS, SVT>),
	Command(CommandTransaction<MVS, SVT>),
}

impl<MVS: MultiVersionStore, SVT: SingleVersionTransaction> Optimistic<MVS, SVT> {
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
