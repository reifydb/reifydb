// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{
	CatalogStore, MaterializedCatalog,
	store::{
		namespace::NamespaceToCreate,
		table::{TableColumnToCreate, TableToCreate},
	},
};
use reifydb_core::{event::EventBus, interceptor::Interceptors};
use reifydb_store_transaction::TransactionStore;
pub use reifydb_transaction::multi::{TransactionMultiVersion, transaction::serializable::TransactionSerializable};
use reifydb_transaction::{
	cdc::TransactionCdc,
	single::{TransactionSingleVersion, TransactionSvl},
};
use reifydb_type::{Type, TypeConstraint};

use crate::StandardCommandTransaction;

pub fn create_test_command_transaction() -> StandardCommandTransaction {
	let store = TransactionStore::testing_memory();

	let event_bus = EventBus::new();
	let single_svl = TransactionSvl::new(store.clone(), event_bus.clone());
	let single = TransactionSingleVersion::SingleVersionLock(single_svl.clone());
	let cdc = TransactionCdc::new(store.clone());
	let multi_serializable = TransactionSerializable::new(store, single.clone(), event_bus.clone());
	let multi = TransactionMultiVersion::Serializable(multi_serializable);

	StandardCommandTransaction::new(multi, single, cdc, event_bus, MaterializedCatalog::new(), Interceptors::new())
		.unwrap()
}

pub fn create_test_command_transaction_with_internal_schema() -> StandardCommandTransaction {
	let store = TransactionStore::testing_memory();

	let event_bus = EventBus::new();
	let single_svl = TransactionSvl::new(store.clone(), event_bus.clone());
	let single = TransactionSingleVersion::SingleVersionLock(single_svl.clone());
	let cdc = TransactionCdc::new(store.clone());
	let multi_serializable = TransactionSerializable::new(store.clone(), single.clone(), event_bus.clone());
	let multi = TransactionMultiVersion::Serializable(multi_serializable);
	let mut result = StandardCommandTransaction::new(
		multi,
		single,
		cdc,
		event_bus,
		MaterializedCatalog::new(),
		Interceptors::new(),
	)
	.unwrap();

	let namespace = CatalogStore::create_namespace(
		&mut result,
		NamespaceToCreate {
			namespace_fragment: None,
			name: "reifydb".to_string(),
		},
	)
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
	.unwrap();

	result
}
