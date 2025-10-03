// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

use reifydb_core::{
	event::EventBus,
	interface::{CdcTransaction, SingleVersionTransaction},
};
use reifydb_engine::StandardCdcTransaction;
use reifydb_store_transaction::{
	BackendConfig, MultiVersionStore, StandardTransactionStore, TransactionStoreConfig,
	backend::{
		Backend,
		cdc::BackendCdc,
		memory::MemoryBackend,
		multi::BackendMulti,
		single::BackendSingle,
		sqlite::{SqliteBackend, SqliteConfig},
	},
};
use reifydb_transaction::{
	mvcc::transaction::{optimistic::OptimisticTransaction, serializable::SerializableTransaction},
	svl::SingleVersionLock,
};

pub mod embedded;

#[cfg(feature = "sub_server")]
pub mod server;

/// Convenience function to create in-memory storage
pub fn memory() -> (
	StandardTransactionStore,
	SingleVersionLock<StandardTransactionStore>,
	StandardCdcTransaction<StandardTransactionStore>,
	EventBus,
) {
	let eventbus = EventBus::new();
	let memory = MemoryBackend::default();
	let store = StandardTransactionStore::new(TransactionStoreConfig {
		hot: Some(BackendConfig {
			backend: Backend {
				multi: BackendMulti::Memory(memory.clone()),
				single: BackendSingle::Memory(memory.clone()),
				cdc: BackendCdc::Memory(memory.clone()),
			},
			retention_period: Duration::from_millis(200),
		}),
		warm: None,
		cold: None,
		retention: Default::default(),
		merge_config: Default::default(),
	})
	.unwrap();

	(
		store.clone(),
		SingleVersionLock::new(store.clone(), eventbus.clone()),
		StandardCdcTransaction::new(store),
		eventbus,
	)
}

/// Convenience function to create SQLite storage
pub fn sqlite(
	config: SqliteConfig,
) -> (
	StandardTransactionStore,
	SingleVersionLock<StandardTransactionStore>,
	StandardCdcTransaction<StandardTransactionStore>,
	EventBus,
) {
	let eventbus = EventBus::new();
	let sqlite = SqliteBackend::new(config);

	let store = StandardTransactionStore::new(TransactionStoreConfig {
		hot: Some(BackendConfig {
			backend: Backend {
				multi: BackendMulti::Sqlite(sqlite.clone()),
				single: BackendSingle::Sqlite(sqlite.clone()),
				cdc: BackendCdc::Sqlite(sqlite.clone()),
			},
			retention_period: Duration::from_millis(200),
		}),
		warm: None,
		cold: None,
		retention: Default::default(),
		merge_config: Default::default(),
	})
	.unwrap();

	(
		store.clone(),
		SingleVersionLock::new(store.clone(), eventbus.clone()),
		StandardCdcTransaction::new(store),
		eventbus,
	)
}

/// Convenience function to create an optimistic transaction layer
pub fn optimistic<MVS, SVT, C>(input: (MVS, SVT, C, EventBus)) -> (OptimisticTransaction<MVS, SVT>, SVT, C, EventBus)
where
	MVS: MultiVersionStore,
	SVT: SingleVersionTransaction,
	C: CdcTransaction,
{
	(OptimisticTransaction::new(input.0, input.1.clone(), input.3.clone()), input.1, input.2, input.3)
}

/// Convenience function to create a serializable transaction layer
pub fn serializable<MVS, SVT, C>(
	input: (MVS, SVT, C, EventBus),
) -> (SerializableTransaction<MVS, SVT>, SVT, C, EventBus)
where
	MVS: MultiVersionStore,
	SVT: SingleVersionTransaction,
	C: CdcTransaction,
{
	(SerializableTransaction::new(input.0, input.1.clone(), input.3.clone()), input.1, input.2, input.3)
}
