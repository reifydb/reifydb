// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

use reifydb_core::event::EventBus;
use reifydb_engine::TransactionCdc;
use reifydb_store_transaction::{
	BackendConfig, TransactionStore, TransactionStoreConfig,
	backend::{
		Backend,
		cdc::BackendCdc,
		memory::MemoryBackend,
		multi::BackendMulti,
		single::BackendSingle,
		sqlite::{SqliteBackend, SqliteConfig},
	},
};
use reifydb_transaction::{multi::TransactionMultiVersion, single::TransactionSingleVersion};

pub mod embedded;

#[cfg(feature = "sub_server")]
pub mod server;

/// Convenience function to create in-memory storage
pub fn memory() -> (TransactionStore, TransactionSingleVersion, TransactionCdc, EventBus) {
	let eventbus = EventBus::new();
	let memory = MemoryBackend::default();
	let store = TransactionStore::standard(TransactionStoreConfig {
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
	});

	(
		store.clone(),
		TransactionSingleVersion::svl(store.clone(), eventbus.clone()),
		TransactionCdc::new(store),
		eventbus,
	)
}

/// Convenience function to create SQLite storage
pub fn sqlite(config: SqliteConfig) -> (TransactionStore, TransactionSingleVersion, TransactionCdc, EventBus) {
	let eventbus = EventBus::new();
	let sqlite = SqliteBackend::new(config);

	let store = TransactionStore::standard(TransactionStoreConfig {
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
	});

	(
		store.clone(),
		TransactionSingleVersion::svl(store.clone(), eventbus.clone()),
		TransactionCdc::new(store),
		eventbus,
	)
}

/// Convenience function to create an optimistic transaction layer
pub fn optimistic(
	input: (TransactionStore, TransactionSingleVersion, TransactionCdc, EventBus),
) -> (TransactionMultiVersion, TransactionSingleVersion, TransactionCdc, EventBus) {
	(TransactionMultiVersion::optimistic(input.0, input.1.clone(), input.3.clone()), input.1, input.2, input.3)
}

/// Convenience function to create a serializable transaction layer
pub fn serializable(
	input: (TransactionStore, TransactionSingleVersion, TransactionCdc, EventBus),
) -> (TransactionMultiVersion, TransactionSingleVersion, TransactionCdc, EventBus) {
	(TransactionMultiVersion::serializable(input.0, input.1.clone(), input.3.clone()), input.1, input.2, input.3)
}
