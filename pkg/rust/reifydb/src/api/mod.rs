// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

use reifydb_core::event::EventBus;
use reifydb_store_transaction::{
	BackendConfig, TransactionStore, TransactionStoreConfig, backend::BackendStorage, sqlite::SqliteConfig,
};
use reifydb_transaction::{cdc::TransactionCdc, multi::TransactionMultiVersion, single::TransactionSingle};

pub mod embedded;
pub mod server;

/// Convenience function to create in-memory storage
pub async fn memory() -> (TransactionStore, TransactionSingle, TransactionCdc, EventBus) {
	let eventbus = EventBus::new();
	let storage = BackendStorage::memory().await;
	let store = TransactionStore::standard(TransactionStoreConfig {
		hot: Some(BackendConfig {
			storage,
			retention_period: Duration::from_millis(200),
		}),
		warm: None,
		cold: None,
		retention: Default::default(),
		merge_config: Default::default(),
		stats: Default::default(),
	});

	(store.clone(), TransactionSingle::svl(store.clone(), eventbus.clone()), TransactionCdc::new(store), eventbus)
}

/// Convenience function to create SQLite storage
pub async fn sqlite(config: SqliteConfig) -> (TransactionStore, TransactionSingle, TransactionCdc, EventBus) {
	let eventbus = EventBus::new();
	let storage = BackendStorage::sqlite(config).await;
	let store = TransactionStore::standard(TransactionStoreConfig {
		hot: Some(BackendConfig {
			storage,
			retention_period: Duration::from_millis(200),
		}),
		warm: None,
		cold: None,
		retention: Default::default(),
		merge_config: Default::default(),
		stats: Default::default(),
	});

	(store.clone(), TransactionSingle::svl(store.clone(), eventbus.clone()), TransactionCdc::new(store), eventbus)
}

/// Convenience function to create a transaction layer
pub async fn transaction(
	input: (TransactionStore, TransactionSingle, TransactionCdc, EventBus),
) -> crate::Result<(TransactionMultiVersion, TransactionSingle, TransactionCdc, EventBus)> {
	let multi = TransactionMultiVersion::new(input.0, input.1.clone(), input.3.clone()).await?;
	Ok((multi, input.1, input.2, input.3))
}

/// Backwards-compat alias for transaction()
pub async fn serializable(
	input: (TransactionStore, TransactionSingle, TransactionCdc, EventBus),
) -> crate::Result<(TransactionMultiVersion, TransactionSingle, TransactionCdc, EventBus)> {
	transaction(input).await
}
