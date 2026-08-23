// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[allow(clippy::disallowed_types)]
use std::time::Duration as StdDuration;
use std::{ops::Deref, sync::Arc, thread::sleep};

use reifydb_catalog::{
	cache::CatalogCache,
	catalog::{
		Catalog,
		namespace::NamespaceToCreate,
		table::{TableColumnToCreate, TableToCreate},
	},
};
use reifydb_cdc::{
	consume::{backlog::FlowBacklog, wake::CdcWakeRegistry},
	produce::{
		producer::{CdcProducerEventListener, spawn_cdc_producer},
		watermark::CdcProducerWatermark,
	},
};
use reifydb_core::{
	actors::cdc::CdcProduceHandle,
	common::{CommitVersion, TimeSource},
	event::{EventBus, transaction::PostCommitEvent},
	interface::catalog::{config::ConfigKey, id::NamespaceId},
	util::ioc::IocContainer,
};
use reifydb_engine::{engine::StandardEngine, vm::services::EngineConfig};
use reifydb_extension::transform::registry::Transforms;
use reifydb_routine::{
	function::default_in_process_functions, monoid::default_in_process_monoids,
	procedure::default_in_process_procedures,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::{
	Runtime, RuntimeConfig,
	actor::system::{ActorSpawner, ActorSystem},
	context::{
		RuntimeContext,
		clock::{Clock, MockClock},
		rng::Rng,
	},
	pool::PoolConfig,
	version_epoch::VersionEpoch,
};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_store_cdc::{
	config::{CdcCommitConfig, CdcPersistentConfig, CdcStoreConfig},
	store::CdcStore,
	tier::read::CdcReadConfig,
};
use reifydb_store_multi::MultiStore;
use reifydb_store_operator::store::OperatorStore;
use reifydb_store_single::SingleStore;
use reifydb_transaction::{
	dictionary::{DictionaryAllocatorRegistry, store::SingleDictionaryStore},
	interceptor::{factory::InterceptorFactory, interceptors::Interceptors},
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::admin::AdminTransaction,
};
use reifydb_value::{
	byte_size::ByteSize,
	fragment::Fragment,
	params::Params,
	value::{Value, constraint::TypeConstraint, frame::frame::Frame, identity::IdentityId, value_type::ValueType},
};

pub struct TestEngine {
	engine: StandardEngine,
	mock_clock: MockClock,
	_runtime: Runtime,
	_operator_guard: SqliteTempPathGuard,
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

	pub fn set_config(&self, key: ConfigKey, value: Value) {
		let catalog = self.engine.catalog();
		let mut admin = self.engine.begin_admin(IdentityId::system()).unwrap();
		catalog.set_config(&mut admin, key, value).unwrap();
		admin.commit().unwrap();
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

	#[allow(clippy::disallowed_types)]
	pub fn await_cdc(&self) -> CommitVersion {
		let target = self.engine.current_version().expect("current version");
		let producer = self.engine.ioc().resolve::<CdcProducerWatermark>().expect("producer watermark");
		for _ in 0..400 {
			if producer.get() >= target && self.engine.done_until() >= target {
				return target;
			}
			sleep(StdDuration::from_millis(5));
		}
		panic!(
			"CDC never caught up to {target:?} within 2s: producer={:?}, done_until={:?}",
			producer.get(),
			self.engine.done_until()
		)
	}
}

impl Deref for TestEngine {
	type Target = StandardEngine;

	fn deref(&self) -> &StandardEngine {
		&self.engine
	}
}

pub trait AsEngine {
	fn engine(&self) -> &StandardEngine;
}

impl AsEngine for StandardEngine {
	fn engine(&self) -> &StandardEngine {
		self
	}
}

impl AsEngine for TestEngine {
	fn engine(&self) -> &StandardEngine {
		self.inner()
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
		let (operator_store, operator_guard) = OperatorStore::testing_memory_with_persistent_sqlite();
		let single = SingleTransaction::new(single_store.clone(), eventbus.clone());
		let catalog_cache = CatalogCache::new();
		let version_epoch = VersionEpoch::new();
		let multi = MultiTransaction::new(
			multi_store.clone(),
			single.clone(),
			eventbus.clone(),
			spawner.clone(),
			clock.clone(),
			version_epoch.clone(),
			rng.clone(),
			Arc::new(catalog_cache.clone()),
		)
		.unwrap();

		let mut ioc = IocContainer::new();
		ioc = ioc.register(catalog_cache.clone());
		ioc = ioc.register(spawner.clone()).register(clock.clone()).register(rng.clone());
		ioc = ioc.register(single_store.clone());
		ioc = ioc.register(operator_store.clone());
		ioc = ioc.register(eventbus.clone());

		#[cfg(not(target_arch = "wasm32"))]
		let cdc_persistent = match self.sqlite_cdc {
			Some(config) => CdcPersistentConfig::sqlite(config),
			None => CdcPersistentConfig::memory(),
		};
		#[cfg(target_arch = "wasm32")]
		let cdc_persistent = CdcPersistentConfig::memory();
		let cdc_store = CdcStore::new(CdcStoreConfig {
			commit: CdcCommitConfig::default(),
			persistent: cdc_persistent,
			read: Some(CdcReadConfig::default()),
			spawner: spawner.clone(),
			clock: clock.clone(),
		});
		ioc = ioc.register(cdc_store.clone());

		let cdc_producer_watermark = CdcProducerWatermark::new();
		ioc = ioc.register(cdc_producer_watermark.clone());

		let cdc_wake_registry = CdcWakeRegistry::new();
		ioc = ioc.register(cdc_wake_registry.clone());

		let flow_backlog = FlowBacklog::new(ByteSize::from_bytes(
			multi.config().get_config_uint8(ConfigKey::FlowBacklogMemoryLimit),
		));
		ioc = ioc.register(flow_backlog.clone());

		let ioc_for_cdc = ioc.clone();

		let engine = StandardEngine::new(
			multi,
			single.clone(),
			eventbus.clone(),
			InterceptorFactory::default(),
			Catalog::new(catalog_cache),
			EngineConfig {
				runtime_context: RuntimeContext::new(clock.clone(), rng.clone(), version_epoch.clone()),
				routines: {
					let b = Routines::builder();
					let b = default_in_process_functions(b);
					let b = default_in_process_procedures(b);
					default_in_process_monoids(b).configure()
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
				&eventbus,
				ioc_for_cdc,
				cdc_producer_watermark,
				cdc_wake_registry,
				flow_backlog,
			);
		}

		TestEngine {
			engine,
			mock_clock,
			_runtime: runtime,
			_operator_guard: operator_guard,
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
		maintenance_threads: 1,
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
	eventbus: &EventBus,
	ioc_for_cdc: IocContainer,
	watermark: CdcProducerWatermark,
	wake_registry: CdcWakeRegistry,
	backlog: FlowBacklog,
) {
	let cdc_handle = spawn_cdc_producer(
		spawner,
		cdc_store,
		multi_store,
		eventbus.clone(),
		watermark,
		wake_registry,
		backlog,
	);
	eventbus.register::<PostCommitEvent, _>(CdcProducerEventListener::new(
		cdc_handle.actor_ref().clone(),
		clock.clone(),
	));
	ioc_for_cdc.register_service::<Arc<CdcProduceHandle>>(Arc::new(cdc_handle));
}

pub fn create_test_admin_transaction() -> AdminTransaction {
	let multi_store = MultiStore::testing_memory();
	let single_store = SingleStore::testing_memory();

	let actor_system = ActorSystem::testing(Clock::Real);
	let spawner = actor_system.spawner();
	let event_bus = EventBus::new(&spawner);
	let single = SingleTransaction::new(single_store, event_bus.clone());
	let multi = MultiTransaction::new(
		multi_store,
		single.clone(),
		event_bus.clone(),
		spawner,
		Clock::Mock(MockClock::from_millis(1000)),
		VersionEpoch::new(),
		Rng::seeded(42),
		Arc::new(CatalogCache::new()),
	)
	.unwrap();

	let dictionary_allocators =
		DictionaryAllocatorRegistry::new(Arc::new(SingleDictionaryStore::new(single.clone())));

	let mut txn = AdminTransaction::new(
		multi,
		single,
		event_bus,
		Interceptors::new(),
		IdentityId::system(),
		Clock::Mock(MockClock::from_millis(1000)),
	)
	.unwrap();
	txn.set_dictionary_allocators(dictionary_allocators);
	txn
}

pub fn create_test_admin_transaction_with_internal_shape() -> AdminTransaction {
	let multi_store = MultiStore::testing_memory();
	let single_store = SingleStore::testing_memory();

	let actor_system = ActorSystem::testing(Clock::Real);
	let spawner = actor_system.spawner();
	let event_bus = EventBus::new(&spawner);
	let single = SingleTransaction::new(single_store, event_bus.clone());
	let multi = MultiTransaction::new(
		multi_store,
		single.clone(),
		event_bus.clone(),
		spawner,
		Clock::Mock(MockClock::from_millis(1000)),
		VersionEpoch::new(),
		Rng::seeded(42),
		Arc::new(CatalogCache::new()),
	)
	.unwrap();
	let dictionary_allocators =
		DictionaryAllocatorRegistry::new(Arc::new(SingleDictionaryStore::new(single.clone())));

	let mut result = AdminTransaction::new(
		multi,
		single.clone(),
		event_bus.clone(),
		Interceptors::new(),
		IdentityId::system(),
		Clock::Mock(MockClock::from_millis(1000)),
	)
	.unwrap();
	result.set_dictionary_allocators(dictionary_allocators);

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
			primary_key_columns: None,
			partition_by: vec![],
			time: TimeSource::Processing,
		},
	)
	.unwrap();

	result
}
