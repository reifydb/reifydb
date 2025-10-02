// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{
	CatalogStore, MaterializedCatalog,
	namespace::NamespaceToCreate,
	table::{TableColumnToCreate, TableToCreate},
};
use reifydb_core::{event::EventBus, interceptor::Interceptors};
use reifydb_store_transaction::StandardTransactionStore;
use reifydb_transaction::{mvcc::transaction::serializable::Serializable, svl::SingleVersionLock};
use reifydb_type::{Type, TypeConstraint};

use crate::{EngineTransaction, StandardCommandTransaction, transaction::StandardCdcTransaction};

pub fn create_test_command_transaction() -> StandardCommandTransaction<
	EngineTransaction<
		Serializable<StandardTransactionStore, SingleVersionLock<StandardTransactionStore>>,
		SingleVersionLock<StandardTransactionStore>,
		StandardCdcTransaction<StandardTransactionStore>,
	>,
> {
	let store = StandardTransactionStore::testing_memory();

	let event_bus = EventBus::new();
	let single = SingleVersionLock::new(store.clone(), event_bus.clone());
	let cdc = StandardCdcTransaction::new(store.clone());
	let multi = Serializable::new(store, single.clone(), event_bus.clone());

	StandardCommandTransaction::new(multi, single, cdc, event_bus, MaterializedCatalog::new(), Interceptors::new())
		.unwrap()
}

pub fn create_test_command_transaction_with_internal_schema() -> StandardCommandTransaction<
	EngineTransaction<
		Serializable<StandardTransactionStore, SingleVersionLock<StandardTransactionStore>>,
		SingleVersionLock<StandardTransactionStore>,
		StandardCdcTransaction<StandardTransactionStore>,
	>,
> {
	let store = StandardTransactionStore::testing_memory();

	let event_bus = EventBus::new();
	let single = SingleVersionLock::new(store.clone(), event_bus.clone());
	let cdc = StandardCdcTransaction::new(store.clone());
	let multi = Serializable::new(store.clone(), single.clone(), event_bus.clone());
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
				},
				TableColumnToCreate {
					name: "data".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Blob),
					policies: vec![],
					auto_increment: false,
					fragment: None,
				},
			],
		},
	)
	.unwrap();

	result
}
