// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, ops::Deref, sync::Arc};

use reifydb_catalog::{
	cache::CatalogCache,
	catalog::{
		Catalog,
		namespace::NamespaceToCreate,
		table::{TableColumnToCreate, TableToCreate},
	},
};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_cdc::storage::recent_cache::RecentCdcCache;
use reifydb_cdc::{
	consume::wake::CdcWakeRegistry,
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
	Runtime, RuntimeConfig,
	actor::system::{ActorSpawner, ActorSystem},
	context::{
		RuntimeContext,
		clock::{Clock, MockClock},
		rng::Rng,
	},
	pool::{PoolConfig, Pools},
};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_sqlite::SqliteConfig;
use reifydb_store_multi::{MultiStore, gc::epoch::listener::VersionEpochListener};
use reifydb_store_single::SingleStore;
use reifydb_transaction::{
	dictionary::DictionaryAllocatorRegistry,
	interceptor::{factory::InterceptorFactory, interceptors::Interceptors},
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::admin::AdminTransaction,
};
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	value::{constraint::TypeConstraint, frame::frame::Frame, identity::IdentityId, value_type::ValueType},
};

use crate::{engine::StandardEngine, vm::services::EngineConfig};

pub struct TestEngine {
	engine: StandardEngine,
	mock_clock: MockClock,
	_runtime: Runtime,
}

impl Default for TestEngine {
	fn default() -> Self {
		Self::new()
	}
}

impl TestEngine {
	pub fn new() -> Self {
		Self::builder().with_cdc().build()
	}

	pub fn builder() -> TestEngineBuilder {
		TestEngineBuilder::default()
	}

	pub fn admin(&self, rql: &str) -> Vec<Frame> {
		let r = self.engine.admin_as(IdentityId::system(), rql, Params::None);
		if let Some(e) = r.error {
			panic!("admin failed: {e:?}\nrql: {rql}")
		}
		r.frames
	}

	pub fn command(&self, rql: &str) -> Vec<Frame> {
		let r = self.engine.command_as(IdentityId::system(), rql, Params::None);
		if let Some(e) = r.error {
			panic!("command failed: {e:?}\nrql: {rql}")
		}
		r.frames
	}

	pub fn query(&self, rql: &str) -> Vec<Frame> {
		let r = self.engine.query_as(IdentityId::system(), rql, Params::None);
		if let Some(e) = r.error {
			panic!("query failed: {e:?}\nrql: {rql}")
		}
		r.frames
	}

	pub fn admin_err(&self, rql: &str) -> String {
		let r = self.engine.admin_as(IdentityId::system(), rql, Params::None);
		match r.error {
			Some(e) => format!("{e:?}"),
			None => panic!("Expected error but admin succeeded\nrql: {rql}"),
		}
	}

	pub fn command_err(&self, rql: &str) -> String {
		let r = self.engine.command_as(IdentityId::system(), rql, Params::None);
		match r.error {
			Some(e) => format!("{e:?}"),
			None => panic!("Expected error but command succeeded\nrql: {rql}"),
		}
	}

	pub fn query_err(&self, rql: &str) -> String {
		let r = self.engine.query_as(IdentityId::system(), rql, Params::None);
		match r.error {
			Some(e) => format!("{e:?}"),
			None => panic!("Expected error but query succeeded\nrql: {rql}"),
		}
	}

	pub fn row_count(frames: &[Frame]) -> usize {
		frames.first().map(|f| f.row_count()).unwrap_or(0)
	}

	pub fn identity() -> IdentityId {
		IdentityId::system()
	}

	pub fn inner(&self) -> &StandardEngine {
		&self.engine
	}

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

	#[cfg(not(target_arch = "wasm32"))]
	pub fn with_sqlite_cdc(mut self, config: SqliteConfig) -> Self {
		self.cdc = true;
		self.sqlite_cdc = Some(config);
		self
	}

	pub fn build(self) -> TestEngine {
		let mock_clock = MockClock::from_millis(1000);
		let runtime = make_test_runtime(&mock_clock);
		let spawner = runtime.spawner();
		let clock = runtime.clock().clone();
		let rng = runtime.rng().clone();

		let eventbus = EventBus::new(&spawner);
		let multi_store = MultiStore::testing_memory_with_eventbus(eventbus.clone());
		let single_store = SingleStore::testing_memory();
		let single = SingleTransaction::new(single_store.clone(), eventbus.clone());
		let catalog_cache = CatalogCache::new();
		let multi = MultiTransaction::new(
			multi_store.clone(),
			single.clone(),
			eventbus.clone(),
			spawner.clone(),
			clock.clone(),
			rng.clone(),
			Arc::new(catalog_cache.clone()),
		)
		.unwrap();

		let mut ioc = IocContainer::new();
		ioc = ioc.register(catalog_cache.clone());
		ioc = ioc.register(spawner.clone()).register(clock.clone()).register(rng.clone());
		ioc = ioc.register(single_store.clone());
		ioc = ioc.register(eventbus.clone());

		#[cfg(not(target_arch = "wasm32"))]
		let cdc_store = match self.sqlite_cdc {
			Some(config) => CdcStore::sqlite(config, RecentCdcCache::DEFAULT_CAPACITY),
			None => CdcStore::memory(),
		};
		#[cfg(target_arch = "wasm32")]
		let cdc_store = CdcStore::memory();
		ioc = ioc.register(cdc_store.clone());

		let cdc_producer_watermark = CdcProducerWatermark::new();
		ioc = ioc.register(cdc_producer_watermark.clone());

		let cdc_wake_registry = CdcWakeRegistry::new();
		ioc = ioc.register(cdc_wake_registry.clone());

		let ioc_for_cdc = ioc.clone();

		let engine = StandardEngine::new(
			multi,
			single.clone(),
			eventbus.clone(),
			InterceptorFactory::default(),
			Catalog::new(catalog_cache),
			EngineConfig {
				runtime_context: RuntimeContext::new(clock.clone(), rng.clone()),
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
				&spawner,
				clock.clone(),
				cdc_store,
				multi_store,
				&engine,
				&eventbus,
				ioc_for_cdc,
				cdc_producer_watermark,
				cdc_wake_registry,
			);
		}

		TestEngine {
			engine,
			mock_clock,
			_runtime: runtime,
		}
	}
}

#[inline]
fn make_test_runtime(mock_clock: &MockClock) -> Runtime {
	let config = RuntimeConfig::default().seeded(1000);
	let config = RuntimeConfig {
		clock: Clock::Mock(mock_clock.clone()),
		..config
	};
	let pools = PoolConfig {
		coordination_threads: 2,
		flow_threads: 2,
		task_threads: 2,
		compute_threads: 2,
		async_threads: 2,
	};
	Runtime::from_config(config, pools)
}

#[allow(clippy::too_many_arguments)]
fn register_cdc_producer(
	spawner: &ActorSpawner,
	clock: Clock,
	cdc_store: CdcStore,
	multi_store: MultiStore,
	engine: &StandardEngine,
	eventbus: &EventBus,
	ioc_for_cdc: IocContainer,
	watermark: CdcProducerWatermark,
	wake_registry: CdcWakeRegistry,
) {
	let cdc_handle = spawn_cdc_producer(
		spawner,
		cdc_store,
		multi_store,
		engine.clone(),
		eventbus.clone(),
		clock.clone(),
		watermark,
		wake_registry,
	);
	eventbus.register::<PostCommitEvent, _>(CdcProducerEventListener::new(
		cdc_handle.actor_ref().clone(),
		clock.clone(),
	));
	eventbus.register::<PostCommitEvent, _>(VersionEpochListener::new(engine.version_epoch().clone(), clock));
	ioc_for_cdc.register_service::<Arc<CdcProduceHandle>>(Arc::new(cdc_handle));
}

pub fn create_test_admin_transaction() -> AdminTransaction {
	let multi_store = MultiStore::testing_memory();
	let single_store = SingleStore::testing_memory();

	let pools = Pools::new(PoolConfig::sync_only());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let spawner = actor_system.spawner();
	mem::forget(actor_system);
	let event_bus = EventBus::new(&spawner);
	let single = SingleTransaction::new(single_store, event_bus.clone());
	let multi = MultiTransaction::new(
		multi_store,
		single.clone(),
		event_bus.clone(),
		spawner,
		Clock::Mock(MockClock::from_millis(1000)),
		Rng::seeded(42),
		Arc::new(CatalogCache::new()),
	)
	.unwrap();

	let mut txn = AdminTransaction::new(
		multi,
		single,
		event_bus,
		Interceptors::new(),
		IdentityId::system(),
		Clock::Mock(MockClock::from_millis(1000)),
	)
	.unwrap();
	txn.set_dictionary_allocators(DictionaryAllocatorRegistry::new());
	txn
}

pub fn create_test_admin_transaction_with_internal_shape() -> AdminTransaction {
	let multi_store = MultiStore::testing_memory();
	let single_store = SingleStore::testing_memory();

	let pools = Pools::new(PoolConfig::sync_only());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let spawner = actor_system.spawner();
	mem::forget(actor_system);
	let event_bus = EventBus::new(&spawner);
	let single = SingleTransaction::new(single_store, event_bus.clone());
	let multi = MultiTransaction::new(
		multi_store,
		single.clone(),
		event_bus.clone(),
		spawner,
		Clock::Mock(MockClock::from_millis(1000)),
		Rng::seeded(42),
		Arc::new(CatalogCache::new()),
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
	result.set_dictionary_allocators(DictionaryAllocatorRegistry::new());

	let catalog_cache = CatalogCache::new();
	let catalog = Catalog::new(catalog_cache);

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
					constraint: TypeConstraint::unconstrained(ValueType::Int8),
					properties: vec![],
					auto_increment: true,
					dictionary_id: None,
				},
				TableColumnToCreate {
					name: Fragment::internal("data"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(ValueType::Blob),
					properties: vec![],
					auto_increment: false,
					dictionary_id: None,
				},
			],
			retention_strategy: None,
			primary_key_columns: None,
			partition_by: vec![],
			underlying: false,
		},
	)
	.unwrap();

	result
}
