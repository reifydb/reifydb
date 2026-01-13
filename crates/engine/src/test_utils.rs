// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_catalog::{
	Catalog, CatalogStore, MaterializedCatalog,
	store::{
		namespace::NamespaceToCreate,
		table::{TableColumnToCreate, TableToCreate},
	},
};
use reifydb_cdc::{CdcEventListener, CdcStore, CdcWorker};
use reifydb_core::{SharedRuntime, SharedRuntimeConfig, event::EventBus, event::transaction::PostCommitEvent, ioc::IocContainer};
use reifydb_rqlv2::Compiler;
use reifydb_store_transaction::{StorageResolver, TransactionStore};

pub use reifydb_transaction::multi::TransactionMulti;
use reifydb_transaction::{
	interceptor::{Interceptors, StandardInterceptorFactory},
	single::{TransactionSingle, TransactionSvl},
};
use reifydb_type::{Type, TypeConstraint};

use crate::{StandardCommandTransaction, StandardEngine};

pub fn create_test_command_transaction() -> StandardCommandTransaction {
	let store = TransactionStore::testing_memory();

	let event_bus = EventBus::new();
	let single_svl = TransactionSvl::new(store.clone(), event_bus.clone());
	let single = TransactionSingle::SingleVersionLock(single_svl.clone());
	let multi = TransactionMulti::new(store, single.clone(), event_bus.clone()).unwrap();

	StandardCommandTransaction::new(multi, single, event_bus, Interceptors::new()).unwrap()
}

pub fn create_test_command_transaction_with_internal_schema() -> StandardCommandTransaction {
	let store = TransactionStore::testing_memory();

	let event_bus = EventBus::new();
	let single_svl = TransactionSvl::new(store.clone(), event_bus.clone());
	let single = TransactionSingle::SingleVersionLock(single_svl.clone());
	let multi = TransactionMulti::new(store.clone(), single.clone(), event_bus.clone()).unwrap();
	let mut result = StandardCommandTransaction::new(multi, single, event_bus, Interceptors::new()).unwrap();

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

/// Create a test StandardEngine with all required dependencies registered.
pub fn create_test_engine() -> StandardEngine {
	#[cfg(debug_assertions)]
	reifydb_core::util::mock_time_set(1000);

	let store = TransactionStore::testing_memory();
	let eventbus = EventBus::new();
	let single = TransactionSingle::svl(store.clone(), eventbus.clone());
	let multi = TransactionMulti::new(store.clone(), single.clone(), eventbus.clone()).unwrap();

	let mut ioc = IocContainer::new();

	let materialized_catalog = MaterializedCatalog::new();
	ioc = ioc.register(materialized_catalog.clone());

	let runtime = SharedRuntime::from_config(
		SharedRuntimeConfig::default().async_threads(2).compute_threads(2).compute_max_in_flight(32),
	);
	ioc = ioc.register(runtime.clone());

	let compiler = Compiler::new(materialized_catalog.clone());
	ioc = ioc.register(compiler);

	// Create CDC pipeline with stats tracking
	let cdc_store = CdcStore::memory();
	ioc = ioc.register(cdc_store.clone());

	let stats_worker = store.stats_worker().clone();
	let hot_storage = store.hot().cloned().expect("hot tier required for CDC");
	let resolver = Arc::new(StorageResolver::new(hot_storage));
	let cdc_worker = Arc::new(CdcWorker::spawn(cdc_store, resolver, Some(stats_worker)));
	eventbus.register::<PostCommitEvent, _>(CdcEventListener::new(cdc_worker.sender()));
	ioc.register_service::<Arc<CdcWorker>>(cdc_worker);

	StandardEngine::new(
		multi,
		single,
		eventbus,
		Box::new(StandardInterceptorFactory::default()),
		Catalog::new(materialized_catalog),
		None,
		ioc,
	)
}
