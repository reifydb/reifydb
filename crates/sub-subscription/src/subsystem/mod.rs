// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod inspect;
mod pushdown;
mod service;

pub(crate) mod hydration;
pub(crate) mod registration;

use std::{
	any::Any,
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_catalog::catalog::Catalog;
use reifydb_cdc::{
	consume::{
		consumer::CdcConsumer,
		poll::{PollConsumer, PollConsumerConfig},
		wake::CdcWakeRegistry,
		watermark::CdcConsumerWatermark,
	},
	storage::CdcStore,
};
use reifydb_core::{
	interface::{
		WithEventBus,
		catalog::{
			config::{ConfigKey, GetConfig},
			subscription::SubscriptionInspectorRef,
		},
		cdc::CdcConsumerId,
		subscription::SubscriptionWatermarkSampler,
		version::{ComponentType, HasVersion, SystemVersion},
	},
	util::ioc::IocContainer,
};
use reifydb_engine::{engine::StandardEngine, subscription::SubscriptionServiceRef};
use reifydb_runtime::{
	actor::{
		mailbox::ActorRef,
		system::{ActorHandle, ActorSpawner},
	},
	context::{RuntimeContext, clock::Clock},
	shutdown::Shutdown,
	sync::{mutex::Mutex, rwlock::RwLock},
};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem, SubsystemFactory};
use reifydb_sub_flow::{builder::CustomOperators, engine::FlowEngineInner, transaction::allocators::FlowAllocators};
use reifydb_transaction::interceptor::builder::InterceptorBuilder;
use reifydb_value::{Result, value::duration::Duration};

use self::{
	inspect::SubscriptionInspectorImpl,
	service::{SubscriptionServiceImpl, SubscriptionState},
};
use crate::{
	consumer::SubscriptionCdcConsumer,
	sink::DeliveryBuffer,
	store::SubscriptionStore,
	tracker::{SubscriptionPositionTracker, SubscriptionSourceTracker},
	watermark::compute_subscription_watermarks,
	worker::{SubscriptionWorkerActor, SubscriptionWorkerMessage},
};

pub struct SubscriptionSubsystem {
	consumer: Mutex<PollConsumer<StandardEngine, SubscriptionCdcConsumer>>,
	state: Arc<SubscriptionState>,
	running: AtomicBool,
	_worker_handles: Mutex<Vec<ActorHandle<SubscriptionWorkerMessage>>>,
}

impl SubscriptionSubsystem {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		engine: StandardEngine,
		cdc_store: CdcStore,
		store: Arc<SubscriptionStore>,
		_runtime_context: RuntimeContext,
		custom_operators: CustomOperators,
		consumer_watermark: CdcConsumerWatermark,
		source_tracker: SubscriptionSourceTracker,
		position_tracker: SubscriptionPositionTracker,
	) -> Result<Self> {
		let catalog = engine.catalog();
		let multi = engine.multi_owned();
		let spawner = engine.spawner();

		let delivery = Arc::new(DeliveryBuffer::new(store.clone()));

		let num_workers = Self::resolve_worker_count(&catalog, &spawner);
		let (workers, worker_handles) = Self::spawn_worker_pool(
			&engine,
			&catalog,
			&store,
			&delivery,
			&custom_operators,
			&spawner,
			num_workers,
		);

		let state = Arc::new(SubscriptionState {
			store: store.clone(),
			workers: workers.clone(),
			subscription_flows: RwLock::new(HashMap::new()),
			multi,
			position_tracker: position_tracker.clone(),
		});

		let cdc_consumer =
			SubscriptionCdcConsumer::new(workers, source_tracker, position_tracker, store.clone());

		let cdc_wake_registry =
			engine.ioc().resolve::<CdcWakeRegistry>().expect("CdcWakeRegistry must be registered");
		let config = PollConsumerConfig::new(
			CdcConsumerId::subscription_consumer(),
			"sub-subscription-poll",
			Duration::from_milliseconds(10).unwrap(),
			None,
		)
		.with_consumer_watermark(consumer_watermark)
		.with_wake_registry(cdc_wake_registry);

		let mut consumer = PollConsumer::new(config, engine, cdc_consumer, cdc_store, spawner);
		consumer.start()?;

		Ok(Self {
			consumer: Mutex::new(consumer),
			state,
			running: AtomicBool::new(true),
			_worker_handles: Mutex::new(worker_handles),
		})
	}

	pub fn service_handle(&self) -> SubscriptionServiceRef {
		Arc::new(SubscriptionServiceImpl {
			state: self.state.clone(),
		})
	}

	pub fn store(&self) -> &Arc<SubscriptionStore> {
		&self.state.store
	}

	#[inline]
	fn resolve_worker_count(catalog: &Catalog, spawner: &ActorSpawner) -> usize {
		let configured = catalog.get_config_uint2(ConfigKey::SubscriptionWorkerThreads) as usize;
		if configured == 0 {
			spawner.pools().system_thread_count()
		} else {
			configured
		}
		.max(2)
	}

	#[inline]
	fn spawn_worker_pool(
		engine: &StandardEngine,
		catalog: &Catalog,
		store: &Arc<SubscriptionStore>,
		delivery: &Arc<DeliveryBuffer>,
		custom_operators: &CustomOperators,
		spawner: &ActorSpawner,
		num_workers: usize,
	) -> (Vec<ActorRef<SubscriptionWorkerMessage>>, Vec<ActorHandle<SubscriptionWorkerMessage>>) {
		let clock = engine.clock().clone();
		let mut workers: Vec<ActorRef<SubscriptionWorkerMessage>> = Vec::with_capacity(num_workers);
		let mut worker_handles: Vec<ActorHandle<SubscriptionWorkerMessage>> = Vec::with_capacity(num_workers);
		for i in 0..num_workers {
			let cat = catalog.clone();
			let exec = engine.executor();
			let bus = engine.event_bus().clone();
			let rc = RuntimeContext::with_clock(clock.clone());
			let co = custom_operators.clone();
			let allocators = FlowAllocators::with_dictionary(engine.dictionary_allocators());
			let factory = move || FlowEngineInner::new(cat, exec, bus, rc, co, allocators);

			let worker = SubscriptionWorkerActor::new(
				factory,
				engine.clone(),
				catalog.clone(),
				store.clone(),
				delivery.clone(),
			);
			let handle = spawner.spawn_system(&format!("subscription-worker-{}", i), worker);
			workers.push(handle.actor_ref().clone());
			worker_handles.push(handle);
		}
		(workers, worker_handles)
	}
}

impl Shutdown for SubscriptionSubsystem {
	fn shutdown(&self) {
		if self.running.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
			return;
		}
		let _ = self.consumer.lock().stop();
	}
}

impl Subsystem for SubscriptionSubsystem {
	fn name(&self) -> &'static str {
		"sub-subscription"
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::Acquire)
	}

	fn health_status(&self) -> HealthStatus {
		if self.is_running() {
			HealthStatus::Healthy
		} else {
			HealthStatus::Unknown
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}
}

impl HasVersion for SubscriptionSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Ephemeral subscription subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

pub struct SubscriptionSubsystemFactory;

impl SubsystemFactory for SubscriptionSubsystemFactory {
	fn provide_interceptors(&self, builder: InterceptorBuilder, _ioc: &IocContainer) -> InterceptorBuilder {
		builder
	}

	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let cdc_store = ioc.resolve::<CdcStore>()?;
		let clock = ioc.resolve::<Clock>()?;

		let runtime_context = RuntimeContext::with_clock(clock);
		let store = Arc::new(SubscriptionStore::new(1024));
		let custom_operators = CustomOperators::new(HashMap::new());

		let consumer_watermark = CdcConsumerWatermark::from_handle(engine.multi().consumer_watermark_handle());
		ioc.register_service::<CdcConsumerWatermark>(consumer_watermark.clone());

		let source_tracker = SubscriptionSourceTracker::new();
		let position_tracker = SubscriptionPositionTracker::new();

		ioc.register_service::<SubscriptionWatermarkSampler>(SubscriptionWatermarkSampler::new({
			let source_tracker = source_tracker.clone();
			let position_tracker = position_tracker.clone();
			let store = store.clone();
			move || compute_subscription_watermarks(&source_tracker, &position_tracker, &store)
		}));

		let subsystem = SubscriptionSubsystem::new(
			engine,
			cdc_store,
			store.clone(),
			runtime_context,
			custom_operators,
			consumer_watermark,
			source_tracker,
			position_tracker,
		)?;

		let service = subsystem.service_handle();
		ioc.register_service::<SubscriptionServiceRef>(service);
		ioc.register_service::<Arc<SubscriptionStore>>(store.clone());

		let inspector: SubscriptionInspectorRef = Arc::new(SubscriptionInspectorImpl {
			store,
		});
		ioc.register_service::<SubscriptionInspectorRef>(inspector);

		Ok(Box::new(subsystem))
	}
}
