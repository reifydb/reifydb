// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_catalog::{
	catalog::{
		Catalog,
		namespace::NamespaceToCreate,
		table::{TableColumnToCreate, TableToCreate},
	},
	materialized::MaterializedCatalog,
	schema::SchemaRegistry,
};
use reifydb_cdc::{
	produce::{listener::CdcEventListener, worker::CdcWorker},
	storage::CdcStore,
};
use reifydb_core::{
	event::{
		EventBus,
		metric::{CdcStatsDroppedEvent, CdcStatsRecordedEvent, StorageStatsRecordedEvent},
		transaction::PostCommitEvent,
	},
	util::ioc::IocContainer,
};
use reifydb_metric::worker::{
	CdcStatsDroppedListener, CdcStatsListener, MetricsWorker, MetricsWorkerConfig, StorageStatsListener,
};
use reifydb_rqlv2::compiler::Compiler;
use reifydb_runtime::{SharedRuntime, SharedRuntimeConfig, actor::system::{ActorSystem, ActorSystemConfig}, clock::Clock};
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_transaction::{
	interceptor::{factory::StandardInterceptorFactory, interceptors::Interceptors},
	multi::transaction::TransactionMulti,
	single::{TransactionSingle, svl::TransactionSvl},
	standard::command::StandardCommandTransaction,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use crate::engine::StandardEngine;

pub fn create_test_command_transaction() -> StandardCommandTransaction {
	let multi_store = MultiStore::testing_memory();
	let single_store = SingleStore::testing_memory();

	let actor_system = ActorSystem::new(ActorSystemConfig::default());
	let event_bus = EventBus::new(&actor_system);
	let single_svl = TransactionSvl::new(single_store, event_bus.clone());
	let single = TransactionSingle::SingleVersionLock(single_svl.clone());
	let multi = TransactionMulti::new(multi_store, single.clone(), event_bus.clone(), actor_system, Clock::default()).unwrap();

	StandardCommandTransaction::new(multi, single, event_bus, Interceptors::new()).unwrap()
}

pub fn create_test_command_transaction_with_internal_schema() -> StandardCommandTransaction {
	let multi_store = MultiStore::testing_memory();
	let single_store = SingleStore::testing_memory();

	let actor_system = ActorSystem::new(ActorSystemConfig::default());
	let event_bus = EventBus::new(&actor_system);
	let single_svl = TransactionSvl::new(single_store, event_bus.clone());
	let single = TransactionSingle::SingleVersionLock(single_svl.clone());
	let multi = TransactionMulti::new(multi_store, single.clone(), event_bus.clone(), actor_system, Clock::default()).unwrap();
	let mut result =
		StandardCommandTransaction::new(multi, single.clone(), event_bus.clone(), Interceptors::new()).unwrap();

	let materialized_catalog = MaterializedCatalog::new();
	let schema_registry = SchemaRegistry::new(single);
	let catalog = Catalog::new(materialized_catalog, schema_registry);

	let namespace = catalog
		.create_namespace(
			&mut result,
			NamespaceToCreate {
				namespace_fragment: None,
				name: "reifydb".to_string(),
			},
		)
		.unwrap();

	catalog.create_table(
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
			primary_key_columns: None,
		},
	)
	.unwrap();

	result
}

/// Create a test StandardEngine with all required dependencies registered.
pub fn create_test_engine() -> StandardEngine {
	let actor_system = ActorSystem::new(ActorSystemConfig::default());
	let eventbus = EventBus::new(&actor_system);
	let multi_store = MultiStore::testing_memory_with_eventbus(eventbus.clone());
	let single_store = SingleStore::testing_memory_with_eventbus(eventbus.clone());
	let single = TransactionSingle::svl(single_store.clone(), eventbus.clone());
	let runtime = SharedRuntime::from_config(
		SharedRuntimeConfig::default()
			.async_threads(2)
			.compute_threads(2)
			.compute_max_in_flight(32)
			.mock_clock(1000),
	);
	let multi =
		TransactionMulti::new(multi_store.clone(), single.clone(), eventbus.clone(), actor_system, runtime.clock().clone()).unwrap();

	let mut ioc = IocContainer::new();

	let materialized_catalog = MaterializedCatalog::new();
	ioc = ioc.register(materialized_catalog.clone());

	let schema_registry = SchemaRegistry::new(single.clone());
	ioc = ioc.register(schema_registry.clone());

	ioc = ioc.register(runtime.clone());

	let compiler = Compiler::new(materialized_catalog.clone());
	ioc = ioc.register(compiler);

	let metrics_store = single_store.clone();

	ioc = ioc.register(metrics_store.clone());

	let metrics_worker = Arc::new(MetricsWorker::new(
		MetricsWorkerConfig::default(),
		metrics_store,
		multi_store.clone(),
		eventbus.clone(),
	));
	eventbus.register::<StorageStatsRecordedEvent, _>(StorageStatsListener::new(metrics_worker.sender()));
	eventbus.register::<CdcStatsRecordedEvent, _>(CdcStatsListener::new(metrics_worker.sender()));
	eventbus.register::<CdcStatsDroppedEvent, _>(CdcStatsDroppedListener::new(metrics_worker.sender()));
	ioc.register_service::<Arc<MetricsWorker>>(metrics_worker);

	let cdc_store = CdcStore::memory();
	ioc = ioc.register(cdc_store.clone());

	let ioc_for_cdc = ioc.clone();

	let engine = StandardEngine::new(
		multi,
		single,
		eventbus.clone(),
		Box::new(StandardInterceptorFactory::default()),
		Catalog::new(materialized_catalog, schema_registry),
		None,
		ioc,
	);

	let cdc_worker = Arc::new(CdcWorker::spawn(cdc_store, multi_store.clone(), eventbus.clone(), engine.clone()));
	eventbus.register::<PostCommitEvent, _>(CdcEventListener::new(cdc_worker.sender(), runtime.clock().clone()));
	ioc_for_cdc.register_service::<Arc<CdcWorker>>(cdc_worker);

	engine
}
