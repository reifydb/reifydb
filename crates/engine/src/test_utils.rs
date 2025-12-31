// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::{
	CatalogStore,
	store::{
		namespace::NamespaceToCreate,
		table::{TableColumnToCreate, TableToCreate},
	},
};
use reifydb_core::{event::EventBus, interceptor::Interceptors};
use reifydb_store_transaction::TransactionStore;
pub use reifydb_transaction::multi::TransactionMulti;
use reifydb_transaction::{
	cdc::TransactionCdc,
	single::{TransactionSingle, TransactionSvl},
};
use reifydb_type::{Type, TypeConstraint};

use crate::StandardCommandTransaction;

pub async fn create_test_command_transaction() -> StandardCommandTransaction {
	let store = TransactionStore::testing_memory().await;

	let event_bus = EventBus::new();
	let single_svl = TransactionSvl::new(store.clone(), event_bus.clone());
	let single = TransactionSingle::SingleVersionLock(single_svl.clone());
	let cdc = TransactionCdc::new(store.clone());
	let multi = TransactionMulti::new(store, single.clone(), event_bus.clone()).await.unwrap();

	StandardCommandTransaction::new(multi, single, cdc, event_bus, Interceptors::new()).await.unwrap()
}

pub async fn create_test_command_transaction_with_internal_schema() -> StandardCommandTransaction {
	let store = TransactionStore::testing_memory().await;

	let event_bus = EventBus::new();
	let single_svl = TransactionSvl::new(store.clone(), event_bus.clone());
	let single = TransactionSingle::SingleVersionLock(single_svl.clone());
	let cdc = TransactionCdc::new(store.clone());
	let multi = TransactionMulti::new(store.clone(), single.clone(), event_bus.clone()).await.unwrap();
	let mut result =
		StandardCommandTransaction::new(multi, single, cdc, event_bus, Interceptors::new()).await.unwrap();

	let namespace = CatalogStore::create_namespace(
		&mut result,
		NamespaceToCreate {
			namespace_fragment: None,
			name: "reifydb".to_string(),
		},
	)
	.await
	.unwrap();

	CatalogStore::create_table(
		&mut result,
		TableToCreate {
			fragment: None,
			namespace: namespace.id,
			table: "flows".to_string(),
			columns: vec![
				TableColumnToCreate {
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Int8),
					policies: vec![],
					auto_increment: true,
					fragment: None,
					dictionary_id: None,
				},
				TableColumnToCreate {
					name: "data".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Blob),
					policies: vec![],
					auto_increment: false,
					fragment: None,
					dictionary_id: None,
				},
			],
			retention_policy: None,
		},
	)
	.await
	.unwrap();

	result
}
