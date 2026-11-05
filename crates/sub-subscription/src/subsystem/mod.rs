// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod inspect;
mod service;

pub(crate) mod registration;

use std::{
	any::Any,
	collections::HashMap,
	sync::{
		Arc, OnceLock,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	commit::{
		loader::{CommitLoader, CommitLoaderHandle, CommitLoaderMetrics},
		queue::{CommitQueue, SubscriptionQueueFloor},
		watermark::ReaderWatermark,
	},
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			subscription::SubscriptionInspectorRef,
		},
		subscription::SubscriptionWatermarkSampler,
		version::{ComponentType, HasVersion, SystemVersion},
	},
	metrics::registry::MetricsRegistry,
	util::ioc::IocContainer,
};
use reifydb_engine::{engine::StandardEngine, subscription::SubscriptionServiceRef};
use reifydb_flow_async::{
	engine::FlowEngineInner,
	operator::{metrics::OperatorSampleRegistry, provider::EmptyOperatorProvider},
	transaction::substrate::FlowSubstrate,
};
use reifydb_runtime::{
	actor::{
		mailbox::ActorRef,
		system::{ActorHandle, ActorSpawner},
	},
	context::{RuntimeContext, clock::Clock},
	shutdown::{Shutdown, ShutdownErrors},
	sync::{mutex::Mutex, rwlock::RwLock},
};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem, SubsystemFactory};
use reifydb_transaction::{
	interceptor::builder::InterceptorBuilder,
	wal::{CommitWal, WalCommitLog},
};
use reifydb_value::Result;
use reifydb_wal::lsn::Lsn;

use self::{
	inspect::SubscriptionInspectorImpl,
	service::{SubscriptionServiceImpl, SubscriptionState},
};
use crate::{
	consumer::SubscriptionCommitLogConsumer,
	delivery::DeliveryBuffer,
	reader::{SubscriptionReader, SubscriptionReaderMessage},
	store::SubscriptionStore,
	tracker::{SubscribedObjects, SubscriptionPositionTracker, SubscriptionSourceTracker},
	watermark::compute_subscription_watermarks,
	worker::{SubscriptionWorkerActor, SubscriptionWorkerMessage, worker_name},
};

const SUBSCRIPTION_READER: &str = "subscription";

fn frontier_lsn(wal: &CommitWal) -> Lsn {
	let cursors = wal.cursors().unwrap_or_else(|err| {
		panic!("commit log cursors could not be read for the subscription reader: {err}")
	});
	cursors.appended.unwrap_or(Lsn::new(0))
}

pub struct SubscriptionSubsystem {
	reader: Mutex<Option<ActorHandle<SubscriptionReaderMessage>>>,
	state: Arc<SubscriptionState>,
	running: AtomicBool,
	_worker_handles: Mutex<Vec<ActorHandle<SubscriptionWorkerMessage>>>,
	_loader_handle: CommitLoaderHandle,
}

impl SubscriptionSubsystem {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		engine: StandardEngine,
		queue: CommitQueue,
		floor: SubscriptionQueueFloor,
		store: Arc<SubscriptionStore>,
		_runtime_context: RuntimeContext,
		consumer_watermark: ReaderWatermark,
		source_tracker: SubscriptionSourceTracker,
		position_tracker: SubscriptionPositionTracker,
		loader_metrics: CommitLoaderMetrics,
	) -> Result<Self> {
		let catalog = engine.catalog();
		let multi = engine.multi_owned();
		let spawner = engine.spawner();

		let delivery = Arc::new(DeliveryBuffer::new(store.clone()));

		let num_workers = Self::resolve_worker_count(&catalog, &spawner);
		let (workers, worker_handles) =
			Self::spawn_worker_pool(&engine, &catalog, &store, &delivery, &spawner, num_workers);

		let subscribed = SubscribedObjects::new();
		let state = Arc::new(SubscriptionState {
			store: store.clone(),
			workers: workers.clone(),
			subscription_flows: RwLock::new(HashMap::new()),
			multi,
			position_tracker: position_tracker.clone(),
			subscribed: subscribed.clone(),
			spawner: spawner.clone(),
		});

		let commit_log_consumer = SubscriptionCommitLogConsumer::new(
			engine.clone(),
			workers,
			source_tracker,
			position_tracker,
			store.clone(),
			delivery,
			subscribed,
		);

		let SubscriptionQueueFloor(floor) = floor;
		let start = engine.current_version()?;
		let wal = engine.multi().wal().clone();
		let start_lsn = frontier_lsn(&wal);
		let loader_handle = spawner.spawn_coordination(
			"sub-subscription-loader",
			CommitLoader::new(Arc::new(WalCommitLog::new(wal, SUBSCRIPTION_READER)), loader_metrics),
		);
		let reader_ref = Arc::new(OnceLock::<ActorRef<SubscriptionReaderMessage>>::new());
		let wake_ref = reader_ref.clone();
		floor.adopt(start, move || {
			if let Some(reader_ref) = wake_ref.get() {
				let _ = reader_ref.send(SubscriptionReaderMessage::Wake);
			}
		});
		let reader = SubscriptionReader::new(
			engine,
			queue,
			floor.clone(),
			commit_log_consumer,
			consumer_watermark,
			start,
			start_lsn,
			loader_handle.actor_ref().clone(),
		);
		let handle = spawner.spawn_coordination("sub-subscription-reader", reader);
		let _ = reader_ref.set(handle.actor_ref().clone());

		Ok(Self {
			reader: Mutex::new(Some(handle)),
			state,
			running: AtomicBool::new(true),
			_worker_handles: Mutex::new(worker_handles),
			_loader_handle: loader_handle,
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
		// A single worker keeps dispatch order deterministic when there is only one thread to
		// dispatch on, which is what the dst/wasm builds need.
		if cfg!(reifydb_single_threaded) {
			return 1;
		}
		let configured = catalog.get_config_uint2(ConfigKey::SubscriptionWorkerThreads) as usize;
		if configured == 0 {
			spawner.pools().coordination_thread_count()
		} else {
			configured
		}
		.max(2)
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn spawn_worker_pool(
		engine: &StandardEngine,
		catalog: &Catalog,
		store: &Arc<SubscriptionStore>,
		delivery: &Arc<DeliveryBuffer>,
		spawner: &ActorSpawner,
		num_workers: usize,
	) -> (Vec<ActorRef<SubscriptionWorkerMessage>>, Vec<ActorHandle<SubscriptionWorkerMessage>>) {
		let clock = engine.clock().clone();
		let mut workers: Vec<ActorRef<SubscriptionWorkerMessage>> = Vec::with_capacity(num_workers);
		let mut worker_handles: Vec<ActorHandle<SubscriptionWorkerMessage>> = Vec::with_capacity(num_workers);
		for i in 0..num_workers {
			let cat = catalog.clone();
			let routines = engine.executor().routines.clone();
			let rc = RuntimeContext::with_clock(clock.clone());
			let provider = Arc::new(EmptyOperatorProvider);
			let substrate = FlowSubstrate::new(engine.dictionary_allocators());
			let factory = move || {
				FlowEngineInner::new(
					cat,
					routines,
					rc,
					provider,
					substrate,
					OperatorSampleRegistry::new(),
				)
			};

			let worker = SubscriptionWorkerActor::new(
				factory,
				engine.clone(),
				catalog.clone(),
				store.clone(),
				delivery.clone(),
			);
			let handle = spawner.spawn_coordination(&worker_name(i), worker);
			workers.push(handle.actor_ref().clone());
			worker_handles.push(handle);
		}
		(workers, worker_handles)
	}
}

impl Shutdown for SubscriptionSubsystem {
	fn shutdown(&self) -> Result<()> {
		if self.running.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
			return Ok(());
		}
		let errors = ShutdownErrors::new("subscription");
		if let Some(handle) = self.reader.lock().take() {
			let _ = handle.actor_ref().send(SubscriptionReaderMessage::Shutdown);
			let _ = handle.join();
		}
		errors.finish()
	}
}

impl Subsystem for SubscriptionSubsystem {
	fn name(&self) -> &'static str {
		"subscription"
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
		let queue = ioc.resolve::<CommitQueue>()?;
		let floor = ioc.resolve::<SubscriptionQueueFloor>()?;
		let clock = ioc.resolve::<Clock>()?;

		let runtime_context = RuntimeContext::with_clock(clock);
		let store = Arc::new(SubscriptionStore::new(1024));

		let consumer_watermark = ReaderWatermark::new();
		ioc.register_service::<ReaderWatermark>(consumer_watermark.clone());

		let source_tracker = SubscriptionSourceTracker::new();
		let position_tracker = SubscriptionPositionTracker::new();

		ioc.register_service::<SubscriptionWatermarkSampler>(SubscriptionWatermarkSampler::new({
			let source_tracker = source_tracker.clone();
			let position_tracker = position_tracker.clone();
			let store = store.clone();
			move || compute_subscription_watermarks(&source_tracker, &position_tracker, &store)
		}));

		let loader_metrics = CommitLoaderMetrics::new("subscription_loader");
		ioc.resolve::<MetricsRegistry>()?.register_collector(Arc::new(loader_metrics.clone()));

		let subsystem = SubscriptionSubsystem::new(
			engine,
			queue,
			floor,
			store.clone(),
			runtime_context,
			consumer_watermark,
			source_tracker,
			position_tracker,
			loader_metrics,
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

#[cfg(test)]
mod tests {
	use reifydb_test_harness::engine::TestEngine;

	use super::*;

	#[test]
	fn the_reader_starts_at_the_last_appended_lsn_not_below_the_commits_it_skips() {
		// A start lsn below the boot frontier makes a first reload ask for records gc may drop, ending every
		// subscription.
		let t = TestEngine::new();
		t.admin("CREATE NAMESPACE app");
		t.admin("CREATE TABLE app::a { id: int4 }");
		t.command("INSERT app::a [{ id: 1 }, { id: 2 }]");
		let wal = t.inner().multi().wal().clone();
		let appended = wal.cursors().expect("commit log cursors").appended;
		assert!(appended.is_some_and(|lsn| lsn.as_u64() > 1), "precondition: the log holds several records");

		let start = frontier_lsn(&wal);

		assert_eq!(
			Some(start),
			appended,
			"the reader starts at the frontier, so its lsn must be the last appended one"
		);
	}
}
