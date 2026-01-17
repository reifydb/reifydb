// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_catalog::schema::SchemaRegistry;
use reifydb_catalog::{
	catalog::Catalog,
	materialized::MaterializedCatalog,
	store::{
		namespace::create::NamespaceToCreate,
		table::create::{TableColumnToCreate, TableToCreate},
	},
	CatalogStore,
};
use reifydb_cdc::{
	produce::{listener::CdcEventListener, worker::CdcWorker},
	storage::CdcStore,
};
#[cfg(debug_assertions)]
use reifydb_core::util::clock::mock_time_set;
use reifydb_core::{
	event::{
		metric::{CdcStatsRecordedEvent, StorageStatsRecordedEvent},
		transaction::PostCommitEvent,
		EventBus,
	},
	runtime::{SharedRuntime, SharedRuntimeConfig},
	util::ioc::IocContainer,
};
use reifydb_metric::worker::{CdcStatsListener, MetricsWorker, MetricsWorkerConfig, StorageStatsListener};
use reifydb_rqlv2::compiler::Compiler;
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_transaction::{
	interceptor::{factory::StandardInterceptorFactory, interceptors::Interceptors},
	multi::transaction::TransactionMulti,
	single::{svl::TransactionSvl, TransactionSingle},
	standard::command::StandardCommandTransaction,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use crate::engine::StandardEngine;

pub fn create_test_command_transaction() -> StandardCommandTransaction {
	let multi_store = MultiStore::testing_memory();
	let single_store = SingleStore::testing_memory();

	let event_bus = EventBus::new();
	let single_svl = TransactionSvl::new(single_store, event_bus.clone());
	let single = TransactionSingle::SingleVersionLock(single_svl.clone());
	let multi = TransactionMulti::new(multi_store, single.clone(), event_bus.clone()).unwrap();

	StandardCommandTransaction::new(multi, single, event_bus, Interceptors::new()).unwrap()
}

pub fn create_test_command_transaction_with_internal_schema() -> StandardCommandTransaction {
	let multi_store = MultiStore::testing_memory();
	let single_store = SingleStore::testing_memory();

	let event_bus = EventBus::new();
	let single_svl = TransactionSvl::new(single_store, event_bus.clone());
	let single = TransactionSingle::SingleVersionLock(single_svl.clone());
	let multi = TransactionMulti::new(multi_store, single.clone(), event_bus.clone()).unwrap();
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
	mock_time_set(1000);

	let eventbus = EventBus::new();
	let multi_store = MultiStore::testing_memory_with_eventbus(eventbus.clone());
	let single_store = SingleStore::testing_memory_with_eventbus(eventbus.clone());
	let single = TransactionSingle::svl(single_store.clone(), eventbus.clone());
	let multi = TransactionMulti::new(multi_store.clone(), single.clone(), eventbus.clone()).unwrap();

	let mut ioc = IocContainer::new();

	let materialized_catalog = MaterializedCatalog::new();
	ioc = ioc.register(materialized_catalog.clone());

	let schema_registry = SchemaRegistry::new(single.clone());
	ioc = ioc.register(schema_registry.clone());

	let runtime = SharedRuntime::from_config(
		SharedRuntimeConfig::default().async_threads(2).compute_threads(2).compute_max_in_flight(32),
	);
	ioc = ioc.register(runtime.clone());

	let compiler = Compiler::new(materialized_catalog.clone());
	ioc = ioc.register(compiler);

	// Create dedicated SingleStore for metrics persistence
	let metrics_store = single_store.clone();

	// Register metrics store in IoC so engine can access it
	ioc = ioc.register(metrics_store.clone());

	// Create metrics worker and register event listeners
	let metrics_worker = Arc::new(MetricsWorker::new(
		MetricsWorkerConfig::default(),
		metrics_store,
		multi_store.clone(),
		eventbus.clone(),
	));
	eventbus.register::<StorageStatsRecordedEvent, _>(StorageStatsListener::new(metrics_worker.sender()));
	eventbus.register::<CdcStatsRecordedEvent, _>(CdcStatsListener::new(metrics_worker.sender()));
	ioc.register_service::<Arc<MetricsWorker>>(metrics_worker);

	// Create CDC pipeline
	let cdc_store = CdcStore::memory();
	ioc = ioc.register(cdc_store.clone());

	let cdc_worker = Arc::new(CdcWorker::spawn(cdc_store, multi_store.clone(), eventbus.clone()));
	eventbus.register::<PostCommitEvent, _>(CdcEventListener::new(cdc_worker.sender()));
	ioc.register_service::<Arc<CdcWorker>>(cdc_worker);

	StandardEngine::new(
		multi,
		single,
		eventbus,
		Box::new(StandardInterceptorFactory::default()),
		Catalog::new(materialized_catalog, schema_registry),
		None,
		ioc,
	)
}
