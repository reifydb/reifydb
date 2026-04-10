// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{ops::Deref, sync::Arc};

use reifydb_catalog::{
	catalog::{
		Catalog,
		namespace::NamespaceToCreate,
		table::{TableColumnToCreate, TableToCreate},
	},
	materialized::MaterializedCatalog,
};
use reifydb_cdc::{
	produce::producer::{CdcProducerEventListener, spawn_cdc_producer},
	storage::CdcStore,
};
use reifydb_core::{
	actors::cdc::CdcProduceHandle,
	event::{
		EventBus,
		metric::{CdcStatsDroppedEvent, CdcStatsRecordedEvent, StorageStatsRecordedEvent},
		transaction::PostCommitEvent,
	},
	interface::catalog::id::NamespaceId,
	util::ioc::IocContainer,
};
use reifydb_extension::transform::registry::Transforms;
use reifydb_metric_old::worker::{
	CdcStatsDroppedListener, CdcStatsListener, MetricsWorker, MetricsWorkerConfig, StorageStatsListener,
};
use reifydb_routine::{function::default_functions, procedure::registry::Procedures};
use reifydb_runtime::{
	SharedRuntime, SharedRuntimeConfig,
	actor::system::ActorSystem,
	context::{
		RuntimeContext,
		clock::{Clock, MockClock},
		rng::Rng,
	},
	pool::{PoolConfig, Pools},
};
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_transaction::{
	interceptor::{factory::InterceptorFactory, interceptors::Interceptors},
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::admin::AdminTransaction,
};
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{constraint::TypeConstraint, frame::frame::Frame, identity::IdentityId, r#type::Type},
};

use crate::{engine::StandardEngine, vm::services::EngineConfig};

pub struct TestEngine {
	engine: StandardEngine,
}

impl Default for TestEngine {
	fn default() -> Self {
		Self::new()
	}
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
		let r = self.engine.admin_as(IdentityId::system(), rql, Params::None);
		if let Some(e) = r.error {
			panic!("admin failed: {e:?}\nrql: {rql}")
		}
		r.frames
	}

	/// Run a command RQL statement as system identity. Panics on error.
	pub fn command(&self, rql: &str) -> Vec<Frame> {
		let r = self.engine.command_as(IdentityId::system(), rql, Params::None);
		if let Some(e) = r.error {
			panic!("command failed: {e:?}\nrql: {rql}")
		}
		r.frames
	}

	/// Run a query RQL statement as system identity. Panics on error.
	pub fn query(&self, rql: &str) -> Vec<Frame> {
		let r = self.engine.query_as(IdentityId::system(), rql, Params::None);
		if let Some(e) = r.error {
			panic!("query failed: {e:?}\nrql: {rql}")
		}
		r.frames
	}

	/// Run an admin statement expecting an error. Panics if it succeeds.
	pub fn admin_err(&self, rql: &str) -> String {
		let r = self.engine.admin_as(IdentityId::system(), rql, Params::None);
		match r.error {
			Some(e) => format!("{e:?}"),
			None => panic!("Expected error but admin succeeded\nrql: {rql}"),
		}
	}

	/// Run a command statement expecting an error. Panics if it succeeds.
	pub fn command_err(&self, rql: &str) -> String {
		let r = self.engine.command_as(IdentityId::system(), rql, Params::None);
		match r.error {
			Some(e) => format!("{e:?}"),
			None => panic!("Expected error but command succeeded\nrql: {rql}"),
		}
	}

	/// Run a query statement expecting an error. Panics if it succeeds.
	pub fn query_err(&self, rql: &str) -> String {
		let r = self.engine.query_as(IdentityId::system(), rql, Params::None);
		match r.error {
			Some(e) => format!("{e:?}"),
			None => panic!("Expected error but query succeeded\nrql: {rql}"),
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
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		let eventbus = EventBus::new(&actor_system);
		let multi_store = MultiStore::testing_memory_with_eventbus(eventbus.clone());
		let single_store = SingleStore::testing_memory_with_eventbus(eventbus.clone());
		let single = SingleTransaction::new(single_store.clone(), eventbus.clone());
		let runtime = SharedRuntime::from_config(
			SharedRuntimeConfig::default()
				.async_threads(2)
				.system_threads(2)
				.query_threads(2)
				.deterministic_testing(1000),
		);
		let materialized_catalog = MaterializedCatalog::new();
		let multi = MultiTransaction::new(
			multi_store.clone(),
			single.clone(),
			eventbus.clone(),
			actor_system,
			runtime.clock().clone(),
			runtime.rng().clone(),
			Arc::new(materialized_catalog.clone()),
		)
		.unwrap();

		let mut ioc = IocContainer::new();

		ioc = ioc.register(materialized_catalog.clone());

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
			single.clone(),
			eventbus.clone(),
			InterceptorFactory::default(),
			Catalog::new(materialized_catalog),
			EngineConfig {
				runtime_context: RuntimeContext::new(runtime.clock().clone(), runtime.rng().clone()),
				functions: default_functions().configure(),
				procedures: Procedures::empty(),
				transforms: Transforms::empty(),
				ioc,
				#[cfg(not(reifydb_single_threaded))]
				remote_registry: None,
			},
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
			ioc_for_cdc.register_service::<Arc<CdcProduceHandle>>(Arc::new(cdc_handle));
		}

		TestEngine {
			engine,
		}
	}
}

pub fn create_test_admin_transaction() -> AdminTransaction {
	let multi_store = MultiStore::testing_memory();
	let single_store = SingleStore::testing_memory();

	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let event_bus = EventBus::new(&actor_system);
	let single = SingleTransaction::new(single_store, event_bus.clone());
	let multi = MultiTransaction::new(
		multi_store,
		single.clone(),
		event_bus.clone(),
		actor_system,
		Clock::Mock(MockClock::from_millis(1000)),
		Rng::seeded(42),
		Arc::new(MaterializedCatalog::new()),
	)
	.unwrap();

	AdminTransaction::new(
		multi,
		single,
		event_bus,
		Interceptors::new(),
		IdentityId::system(),
		Clock::Mock(MockClock::from_millis(1000)),
	)
	.unwrap()
}

pub fn create_test_admin_transaction_with_internal_shape() -> AdminTransaction {
	let multi_store = MultiStore::testing_memory();
	let single_store = SingleStore::testing_memory();

	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let event_bus = EventBus::new(&actor_system);
	let single = SingleTransaction::new(single_store, event_bus.clone());
	let multi = MultiTransaction::new(
		multi_store,
		single.clone(),
		event_bus.clone(),
		actor_system,
		Clock::Mock(MockClock::from_millis(1000)),
		Rng::seeded(42),
		Arc::new(MaterializedCatalog::new()),
	)
	.unwrap();
	let mut result = AdminTransaction::new(
		multi,
		single.clone(),
		event_bus.clone(),
		Interceptors::new(),
		IdentityId::system(),
		Clock::Mock(MockClock::from_millis(1000)),
	)
	.unwrap();

	let materialized_catalog = MaterializedCatalog::new();
	let catalog = Catalog::new(materialized_catalog);

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
			retention_strategy: None,
			primary_key_columns: None,
		},
	)
	.unwrap();

	result
}
