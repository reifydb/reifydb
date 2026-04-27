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
	produce::{
		producer::{CdcProducerEventListener, spawn_cdc_producer},
		watermark::CdcProducerWatermark,
	},
	storage::CdcStore,
};
use reifydb_core::{
	actors::cdc::CdcProduceHandle,
	event::{EventBus, transaction::PostCommitEvent},
	interface::catalog::id::NamespaceId,
	util::ioc::IocContainer,
};
use reifydb_extension::transform::registry::Transforms;
use reifydb_routine::{
	function::default_native_functions, procedure::default_native_procedures, routine::registry::Routines,
};
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
#[cfg(not(target_arch = "wasm32"))]
use reifydb_sqlite::SqliteConfig;
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
	mock_clock: MockClock,
}

impl Default for TestEngine {
	fn default() -> Self {
		Self::new()
	}
}

impl TestEngine {
	/// Create a new TestEngine with all subsystems (CDC, metrics, etc.).
	pub fn new() -> Self {
		Self::builder().with_cdc().build()
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

	/// The mock clock backing the test engine. Use `.advance_millis()` etc. to
	/// move time forward deterministically.
	pub fn mock_clock(&self) -> MockClock {
		self.mock_clock.clone()
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
	#[cfg(not(target_arch = "wasm32"))]
	sqlite_cdc: Option<SqliteConfig>,
}

impl TestEngineBuilder {
	pub fn with_cdc(mut self) -> Self {
		self.cdc = true;
		self
	}

	/// Use a SQLite-backed CDC store instead of the default in-memory backend.
	/// Implies `with_cdc()`.
	#[cfg(not(target_arch = "wasm32"))]
	pub fn with_sqlite_cdc(mut self, config: SqliteConfig) -> Self {
		self.cdc = true;
		self.sqlite_cdc = Some(config);
		self
	}

	pub fn build(self) -> TestEngine {
		let mock_clock = MockClock::from_millis(1000);
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Mock(mock_clock.clone()));
		let eventbus = EventBus::new(&actor_system);
		let multi_store = MultiStore::testing_memory_with_eventbus(eventbus.clone());
		let single_store = SingleStore::testing_memory_with_eventbus(eventbus.clone());
		let single = SingleTransaction::new(single_store.clone(), eventbus.clone());
		let runtime = make_test_runtime(&mock_clock);
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

		#[cfg(not(target_arch = "wasm32"))]
		let cdc_store = match self.sqlite_cdc {
			Some(config) => CdcStore::sqlite(config),
			None => CdcStore::memory(),
		};
		#[cfg(target_arch = "wasm32")]
		let cdc_store = CdcStore::memory();
		ioc = ioc.register(cdc_store.clone());

		let cdc_producer_watermark = CdcProducerWatermark::new();
		ioc = ioc.register(cdc_producer_watermark.clone());

		let ioc_for_cdc = ioc.clone();

		let engine = StandardEngine::new(
			multi,
			single.clone(),
			eventbus.clone(),
			InterceptorFactory::default(),
			Catalog::new(materialized_catalog),
			EngineConfig {
				runtime_context: RuntimeContext::new(runtime.clock().clone(), runtime.rng().clone()),
				routines: {
					let b = Routines::builder();
					let b = default_native_functions(b);
					default_native_procedures(b).configure()
				},
				transforms: Transforms::empty(),
				ioc,
				#[cfg(not(reifydb_single_threaded))]
				remote_registry: None,
			},
		);

		if self.cdc {
			register_cdc_producer(
				&runtime,
				cdc_store,
				multi_store,
				&engine,
				&eventbus,
				ioc_for_cdc,
				cdc_producer_watermark,
			);
		}

		TestEngine {
			engine,
			mock_clock,
		}
	}
}

#[inline]
fn make_test_runtime(mock_clock: &MockClock) -> SharedRuntime {
	let base = SharedRuntimeConfig::default().async_threads(2).system_threads(2).query_threads(2).seeded(1000);
	let config = SharedRuntimeConfig {
		clock: Clock::Mock(mock_clock.clone()),
		..base
	};
	SharedRuntime::from_config(config)
}

fn register_cdc_producer(
	runtime: &SharedRuntime,
	cdc_store: CdcStore,
	multi_store: MultiStore,
	engine: &StandardEngine,
	eventbus: &EventBus,
	ioc_for_cdc: IocContainer,
	watermark: CdcProducerWatermark,
) {
	let cdc_handle = spawn_cdc_producer(
		&runtime.actor_system(),
		cdc_store,
		multi_store,
		engine.clone(),
		eventbus.clone(),
		runtime.clock().clone(),
		watermark,
	);
	eventbus.register::<PostCommitEvent, _>(CdcProducerEventListener::new(
		cdc_handle.actor_ref().clone(),
		runtime.clock().clone(),
	));
	ioc_for_cdc.register_service::<Arc<CdcProduceHandle>>(Arc::new(cdc_handle));
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
			underlying: false,
		},
	)
	.unwrap();

	result
}
