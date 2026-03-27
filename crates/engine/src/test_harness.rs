// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{ops::Deref, sync::Arc};

use reifydb_builtin::registry::default_functions;
use reifydb_catalog::{
	catalog::{
		Catalog,
		namespace::NamespaceToCreate,
		table::{TableColumnToCreate, TableToCreate},
	},
	materialized::MaterializedCatalog,
	procedure::registry::Procedures,
	schema::RowSchemaRegistry,
};
use reifydb_cdc::{
	produce::producer::{CdcProduceMsg, CdcProducerEventListener, spawn_cdc_producer},
	storage::CdcStore,
};
use reifydb_core::{
	config::SystemConfig,
	event::{
		EventBus,
		metric::{CdcStatsDroppedEvent, CdcStatsRecordedEvent, StorageStatsRecordedEvent},
		transaction::PostCommitEvent,
	},
	interface::catalog::id::NamespaceId,
	util::ioc::IocContainer,
};
use reifydb_metric::worker::{
	CdcStatsDroppedListener, CdcStatsListener, MetricsWorker, MetricsWorkerConfig, StorageStatsListener,
};
use reifydb_runtime::{
	SharedRuntime, SharedRuntimeConfig,
	actor::system::{ActorHandle, ActorSystem, ActorSystemConfig},
	context::{RuntimeContext, clock::Clock},
};
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_transaction::{
	interceptor::{factory::InterceptorFactory, interceptors::Interceptors},
	multi::transaction::{MultiTransaction, register_oracle_defaults},
	single::SingleTransaction,
	transaction::admin::AdminTransaction,
};
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{constraint::TypeConstraint, frame::frame::Frame, identity::IdentityId, r#type::Type},
};

use crate::{engine::StandardEngine, transform::registry::Transforms};

pub struct TestEngine {
	engine: StandardEngine,
}

impl TestEngine {
	/// Create a new TestEngine with all subsystems (CDC, metrics, etc.).
	pub fn new() -> Self {
		Self::builder().with_cdc().with_metrics().build()
	}

	/// Start configuring a test engine via the builder.
	pub fn builder() -> TestEngineBuilder {
		TestEngineBuilder::default()
	}

	/// Run an admin RQL statement as system identity. Panics on error.
	pub fn admin(&self, rql: &str) -> Vec<Frame> {
		self.engine
			.admin_as(IdentityId::system(), rql, Params::None)
			.unwrap_or_else(|e| panic!("admin failed: {e:?}\nrql: {rql}"))
	}

	/// Run a command RQL statement as system identity. Panics on error.
	pub fn command(&self, rql: &str) -> Vec<Frame> {
		self.engine
			.command_as(IdentityId::system(), rql, Params::None)
			.unwrap_or_else(|e| panic!("command failed: {e:?}\nrql: {rql}"))
	}

	/// Run a query RQL statement as system identity. Panics on error.
	pub fn query(&self, rql: &str) -> Vec<Frame> {
		self.engine
			.query_as(IdentityId::system(), rql, Params::None)
			.unwrap_or_else(|e| panic!("query failed: {e:?}\nrql: {rql}"))
	}

	/// Run an admin statement expecting an error. Panics if it succeeds.
	pub fn admin_err(&self, rql: &str) -> String {
		match self.engine.admin_as(IdentityId::system(), rql, Params::None) {
			Err(e) => format!("{e:?}"),
			Ok(_) => panic!("Expected error but admin succeeded\nrql: {rql}"),
		}
	}

	/// Run a command statement expecting an error. Panics if it succeeds.
	pub fn command_err(&self, rql: &str) -> String {
		match self.engine.command_as(IdentityId::system(), rql, Params::None) {
			Err(e) => format!("{e:?}"),
			Ok(_) => panic!("Expected error but command succeeded\nrql: {rql}"),
		}
	}

	/// Run a query statement expecting an error. Panics if it succeeds.
	pub fn query_err(&self, rql: &str) -> String {
		match self.engine.query_as(IdentityId::system(), rql, Params::None) {
			Err(e) => format!("{e:?}"),
			Ok(_) => panic!("Expected error but query succeeded\nrql: {rql}"),
		}
	}

	/// Count rows in the first frame.
	pub fn row_count(frames: &[Frame]) -> usize {
		frames.first().map(|f| f.row_count()).unwrap_or(0)
	}

	/// Return the system identity used by this harness.
	pub fn identity() -> IdentityId {
		IdentityId::system()
	}

	/// Access the underlying StandardEngine.
	pub fn inner(&self) -> &StandardEngine {
		&self.engine
	}
}

impl Deref for TestEngine {
	type Target = StandardEngine;

	fn deref(&self) -> &StandardEngine {
		&self.engine
	}
}

#[derive(Default)]
pub struct TestEngineBuilder {
	cdc: bool,
	metrics: bool,
}

impl TestEngineBuilder {
	pub fn with_cdc(mut self) -> Self {
		self.cdc = true;
		self
	}

	pub fn with_metrics(mut self) -> Self {
		self.metrics = true;
		self
	}

	pub fn build(self) -> TestEngine {
		let actor_system = ActorSystem::new(SharedRuntimeConfig::default().actor_system_config());
		let eventbus = EventBus::new(&actor_system);
		let multi_store = MultiStore::testing_memory_with_eventbus(eventbus.clone());
		let single_store = SingleStore::testing_memory_with_eventbus(eventbus.clone());
		let single = SingleTransaction::new(single_store.clone(), eventbus.clone());
		let runtime = SharedRuntime::from_config(
			SharedRuntimeConfig::default()
				.async_threads(2)
				.compute_threads(2)
				.compute_max_in_flight(32)
				.deterministic_testing(1000),
		);
		let system_config = SystemConfig::new();
		register_oracle_defaults(&system_config);
		let multi = MultiTransaction::new(
			multi_store.clone(),
			single.clone(),
			eventbus.clone(),
			actor_system,
			runtime.clock().clone(),
			system_config,
		)
		.unwrap();

		let mut ioc = IocContainer::new();

		let materialized_catalog = MaterializedCatalog::new(SystemConfig::new());
		ioc = ioc.register(materialized_catalog.clone());

		let row_schema_registry = RowSchemaRegistry::new(single.clone());
		ioc = ioc.register(row_schema_registry.clone());

		ioc = ioc.register(runtime.clone());
		ioc = ioc.register(single_store.clone());

		if self.metrics {
			let metrics_worker = Arc::new(MetricsWorker::new(
				MetricsWorkerConfig::default(),
				single_store.clone(),
				multi_store.clone(),
				eventbus.clone(),
			));
			eventbus.register::<StorageStatsRecordedEvent, _>(StorageStatsListener::new(
				metrics_worker.sender(),
			));
			eventbus.register::<CdcStatsRecordedEvent, _>(CdcStatsListener::new(metrics_worker.sender()));
			eventbus.register::<CdcStatsDroppedEvent, _>(CdcStatsDroppedListener::new(
				metrics_worker.sender(),
			));
			ioc.register_service::<Arc<MetricsWorker>>(metrics_worker);
		}

		let cdc_store = CdcStore::memory();
		ioc = ioc.register(cdc_store.clone());

		let ioc_for_cdc = ioc.clone();

		let engine = StandardEngine::new(
			multi,
			single,
			eventbus.clone(),
			InterceptorFactory::default(),
			Catalog::new(materialized_catalog, row_schema_registry),
			RuntimeContext::with_clock(runtime.clock().clone()),
			default_functions().build(),
			Procedures::empty(),
			Transforms::empty(),
			ioc,
			#[cfg(not(target_arch = "wasm32"))]
			None,
		);

		if self.cdc {
			let cdc_handle = spawn_cdc_producer(
				&runtime.actor_system(),
				cdc_store,
				multi_store.clone(),
				engine.clone(),
				eventbus.clone(),
			);
			eventbus.register::<PostCommitEvent, _>(CdcProducerEventListener::new(
				cdc_handle.actor_ref().clone(),
				runtime.clock().clone(),
			));
			ioc_for_cdc.register_service::<Arc<ActorHandle<CdcProduceMsg>>>(Arc::new(cdc_handle));
		}

		TestEngine {
			engine,
		}
	}
}

pub fn create_test_admin_transaction() -> AdminTransaction {
	let multi_store = MultiStore::testing_memory();
	let single_store = SingleStore::testing_memory();

	let actor_system = ActorSystem::new(SharedRuntimeConfig::default().actor_system_config());
	let event_bus = EventBus::new(&actor_system);
	let single = SingleTransaction::new(single_store, event_bus.clone());
	let system_config = SystemConfig::new();
	register_oracle_defaults(&system_config);
	let multi = MultiTransaction::new(
		multi_store,
		single.clone(),
		event_bus.clone(),
		actor_system,
		Clock::default(),
		system_config,
	)
	.unwrap();

	AdminTransaction::new(multi, single, event_bus, Interceptors::new(), IdentityId::system()).unwrap()
}

pub fn create_test_admin_transaction_with_internal_schema() -> AdminTransaction {
	let multi_store = MultiStore::testing_memory();
	let single_store = SingleStore::testing_memory();

	let actor_system = ActorSystem::new(ActorSystemConfig {
		pool_threads: 1,
		max_in_flight: 1,
	});
	let event_bus = EventBus::new(&actor_system);
	let single = SingleTransaction::new(single_store, event_bus.clone());
	let system_config = SystemConfig::new();
	register_oracle_defaults(&system_config);
	let multi = MultiTransaction::new(
		multi_store,
		single.clone(),
		event_bus.clone(),
		actor_system,
		Clock::default(),
		system_config,
	)
	.unwrap();
	let mut result = AdminTransaction::new(
		multi,
		single.clone(),
		event_bus.clone(),
		Interceptors::new(),
		IdentityId::system(),
	)
	.unwrap();

	let materialized_catalog = MaterializedCatalog::new(SystemConfig::new());
	let row_schema_registry = RowSchemaRegistry::new(single);
	let catalog = Catalog::new(materialized_catalog, row_schema_registry);

	let namespace = catalog
		.create_namespace(
			&mut result,
			NamespaceToCreate {
				namespace_fragment: None,
				name: "reifydb".to_string(),
				local_name: "reifydb".to_string(),
				parent_id: NamespaceId::ROOT,
				grpc: None,
				token: None,
			},
		)
		.unwrap();

	catalog.create_table(
		&mut result,
		TableToCreate {
			name: Fragment::internal("flows"),
			namespace: namespace.id(),
			columns: vec![
				TableColumnToCreate {
					name: Fragment::internal("id"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(Type::Int8),
					properties: vec![],
					auto_increment: true,
					dictionary_id: None,
				},
				TableColumnToCreate {
					name: Fragment::internal("data"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(Type::Blob),
					properties: vec![],
					auto_increment: false,
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
