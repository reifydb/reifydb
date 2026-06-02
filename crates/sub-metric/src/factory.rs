// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	actors::metric::MetricMessage,
	event::{
		EventBus,
		metric::{
			CdcEvictedEvent, CdcWrittenEvent, MultiCommittedEvent, ProfilerSnapshotEvent,
			RequestExecutedEvent,
		},
	},
	interface::catalog::{config::GetConfig, id::NamespaceId},
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_metric::{
	accumulator::StatementStatsAccumulator,
	registry::{MetricRegistry, StaticMetricRegistry},
};
use reifydb_runtime::actor::{mailbox::ActorRef, system::ActorSpawner};
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_value::Result;
use tracing::warn;

use crate::{
	actor::MetricCollectorActor,
	listener::{
		CdcEvictedListener, CdcWrittenListener, MultiCommittedListener, ProfilerSnapshotListener,
		RequestMetricsEventListener,
	},
	profiler_vtable::MetricsProfilerCategoriesVTable,
	subsystem::MetricSubsystem,
};

type ResolvedDeps = (ActorSpawner, EventBus, SingleStore, MultiStore, Result<StandardEngine>);

pub struct MetricSubsystemFactory {
	registry: Arc<MetricRegistry>,
	static_registry: Arc<StaticMetricRegistry>,
	accumulator: Arc<StatementStatsAccumulator>,
}

impl MetricSubsystemFactory {
	pub fn new(
		registry: Arc<MetricRegistry>,
		static_registry: Arc<StaticMetricRegistry>,
		accumulator: Arc<StatementStatsAccumulator>,
	) -> Self {
		Self {
			registry,
			static_registry,
			accumulator,
		}
	}
}

impl SubsystemFactory for MetricSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let (spawner, event_bus, single_store, multi_store, engine) = Self::resolve_deps(ioc)?;

		let actor_ref = self.spawn_collector(&spawner, event_bus.clone(), single_store, multi_store, &engine);

		Self::register_listeners(&event_bus, actor_ref);
		Self::register_vtable(engine)?;

		Ok(Box::new(MetricSubsystem::new()))
	}
}

impl MetricSubsystemFactory {
	#[inline]
	fn resolve_deps(ioc: &IocContainer) -> Result<ResolvedDeps> {
		let spawner = ioc.resolve::<ActorSpawner>()?;
		let event_bus = ioc.resolve::<EventBus>()?;
		let single_store = ioc.resolve::<SingleStore>()?;
		let multi_store = ioc.resolve::<MultiStore>()?;
		let engine = ioc.resolve::<StandardEngine>();
		Ok((spawner, event_bus, single_store, multi_store, engine))
	}

	#[inline]
	#[allow(clippy::boxed_local)]
	fn spawn_collector(
		self: Box<Self>,
		spawner: &ActorSpawner,
		event_bus: EventBus,
		single_store: SingleStore,
		multi_store: MultiStore,
		engine: &Result<StandardEngine>,
	) -> ActorRef<MetricMessage> {
		let mut actor = MetricCollectorActor::new(
			self.registry,
			self.static_registry,
			self.accumulator,
			event_bus,
			single_store,
			multi_store,
		);
		if let Ok(engine) = engine {
			actor = actor.with_config(Arc::new(engine.catalog()) as Arc<dyn GetConfig>);
		}
		let handle = spawner.spawn_background("metric-collector", actor);
		handle.actor_ref().clone()
	}

	#[inline]
	fn register_listeners(event_bus: &EventBus, actor_ref: ActorRef<MetricMessage>) {
		event_bus.register::<RequestExecutedEvent, _>(RequestMetricsEventListener::new(actor_ref.clone()));
		event_bus.register::<MultiCommittedEvent, _>(MultiCommittedListener::new(actor_ref.clone()));
		event_bus.register::<CdcWrittenEvent, _>(CdcWrittenListener::new(actor_ref.clone()));
		event_bus.register::<CdcEvictedEvent, _>(CdcEvictedListener::new(actor_ref.clone()));
		event_bus.register::<ProfilerSnapshotEvent, _>(ProfilerSnapshotListener::new(actor_ref));
	}

	#[inline]
	fn register_vtable(engine: Result<StandardEngine>) -> Result<()> {
		match engine {
			Ok(engine) => {
				engine.register_virtual_table(
					NamespaceId::SYSTEM_METRICS_PROFILER,
					"categories",
					MetricsProfilerCategoriesVTable::new(),
				)?;
			}
			Err(e) => {
				warn!(
					"StandardEngine not available in IoC; profiler categories vtable not registered: {e}"
				);
			}
		}
		Ok(())
	}
}
