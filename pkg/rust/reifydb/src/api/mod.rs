// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

use reifydb_core::event::EventBus;
use reifydb_store_transaction::{
	BackendConfig, TransactionStore, TransactionStoreConfig, backend::BackendStorage, sqlite::SqliteConfig,
};
use reifydb_transaction::{cdc::TransactionCdc, multi::TransactionMultiVersion, single::TransactionSingleVersion};

pub mod embedded;
pub mod server;

/// Convenience function to create in-memory storage
pub fn memory() -> (TransactionStore, TransactionSingleVersion, TransactionCdc, EventBus) {
	let eventbus = EventBus::new();
	let store = TransactionStore::standard(TransactionStoreConfig {
		hot: Some(BackendConfig {
			storage: BackendStorage::memory(),
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

	let store = TransactionStore::standard(TransactionStoreConfig {
		hot: Some(BackendConfig {
			storage: BackendStorage::sqlite(config),
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

/// Convenience function to create a transaction layer
pub fn transaction(
	input: (TransactionStore, TransactionSingleVersion, TransactionCdc, EventBus),
) -> (TransactionMultiVersion, TransactionSingleVersion, TransactionCdc, EventBus) {
	let multi = TransactionMultiVersion::new(input.0, input.1.clone(), input.3.clone());
	(multi, input.1, input.2, input.3)
}

/// Backwards-compat alias for transaction()
pub fn serializable(
	input: (TransactionStore, TransactionSingleVersion, TransactionCdc, EventBus),
) -> (TransactionMultiVersion, TransactionSingleVersion, TransactionCdc, EventBus) {
	transaction(input)
}
