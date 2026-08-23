// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	actors::metrics::MetricsMessage,
	event::{
		EventBus,
		lifecycle::VersionEpochSampledEvent,
		metric::{
			CdcEvictedEvent, CdcWrittenEvent, MultiCommittedEvent, MultiSweptEvent, RequestExecutedEvent,
		},
	},
	interface::catalog::config::{ConfigKey, GetConfig},
	internal,
	lifecycle::metrics::RetentionMetrics,
	metrics::registry::MetricsRegistry,
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::{mailbox::ActorRef, system::ActorSpawner},
	context::clock::Clock,
};
use reifydb_store_cdc::store::CdcStore;
use reifydb_store_multi::MultiStore;
use reifydb_store_operator::store::OperatorStore;
use reifydb_store_single::SingleStore;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_value::{Result, error};

use crate::{
	accumulator::StatementMetricsAccumulator,
	actor::MetricsFlushActor,
	domains::{
		epoch::EpochGauge,
		runtime::{SampleReader, collect::Collectors},
		store::StoreReader,
	},
	framework::{spec::MetricsDomain, surfaces::MetricsSurfaces},
	listener::{
		CdcEvictedListener, CdcWrittenListener, MultiCommittedListener, MultiSweptListener,
		RequestMetricsEventListener, VersionEpochSampledListener,
	},
	sampler::{MetricsSamplerActor, SamplerMessage},
	subsystem::MetricsSubsystem,
};

pub struct MetricsSubsystemFactory;

impl MetricsSubsystemFactory {
	pub fn new() -> Self {
		Self
	}
}

impl Default for MetricsSubsystemFactory {
	fn default() -> Self {
		Self::new()
	}
}

impl SubsystemFactory for MetricsSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let registry = ioc.resolve::<MetricsRegistry>()?;
		let clock = ioc.resolve::<Clock>()?;
		let spawner = ioc.resolve::<ActorSpawner>()?;
		let multi_store = ioc.resolve::<MultiStore>()?;
		let single_store = ioc.resolve::<SingleStore>()?;
		let operator_store = ioc.resolve::<OperatorStore>()?;
		let cdc_store = ioc.resolve::<CdcStore>()?;
		let retention_metrics = ioc.resolve::<RetentionMetrics>()?;

		registry.register_collectors(cdc_store.metrics_collectors());

		let collectors = Collectors {
			engine: engine.clone(),
			registry,
		};

		let epoch_gauge = Arc::new(EpochGauge::default());

		let sampler = Self::wire_sampler(
			&engine,
			&spawner,
			&clock,
			&collectors,
			&multi_store,
			&single_store,
			&operator_store,
			&cdc_store,
			&retention_metrics,
			epoch_gauge.clone(),
		)?;
		ioc.register_service(sampler.clone());
		Self::wire_accounting(ioc, &engine, &spawner, epoch_gauge, sampler)?;

		let store_reader = StoreReader::new(multi_store, single_store, operator_store, cdc_store);

		Ok(Box::new(MetricsSubsystem::new(SampleReader::new(collectors), store_reader)))
	}
}

impl MetricsSubsystemFactory {
	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn wire_sampler(
		engine: &StandardEngine,
		spawner: &ActorSpawner,
		clock: &Clock,
		collectors: &Collectors,
		multi_store: &MultiStore,
		single_store: &SingleStore,
		operator_store: &OperatorStore,
		cdc_store: &CdcStore,
		retention_metrics: &RetentionMetrics,
		epoch_gauge: Arc<EpochGauge>,
	) -> Result<ActorRef<SamplerMessage>> {
		let surfaces = Arc::new(MetricsSurfaces::build(MetricsDomain::ALL.map(MetricsDomain::spec)));
		surfaces.register_all(engine)?;
		let interval = engine.catalog().get_config_duration(ConfigKey::MetricsSampleInterval);
		let snapshot_interval = engine.catalog().get_config_duration_opt(ConfigKey::MetricsSnapshotInterval);
		if let Some(snapshot) = snapshot_interval
			&& snapshot < interval
		{
			return Err(error!(internal!(
				"METRICS_SNAPSHOT_INTERVAL ({:?}) must not be shorter than METRICS_SAMPLE_INTERVAL ({:?}); a shorter snapshot cadence would append duplicate readings between rolls",
				snapshot,
				interval
			)));
		}
		let actor = MetricsSamplerActor::new(
			collectors.clone(),
			multi_store.clone(),
			single_store.clone(),
			operator_store.clone(),
			cdc_store.clone(),
			retention_metrics.clone(),
			epoch_gauge,
			surfaces,
			clock.clone(),
			interval,
			snapshot_interval,
		);
		let handle = spawner.spawn_coordination("metrics-sampler", actor);
		Ok(handle.actor_ref().clone())
	}

	#[inline]
	fn wire_accounting(
		ioc: &IocContainer,
		engine: &StandardEngine,
		spawner: &ActorSpawner,
		epoch_gauge: Arc<EpochGauge>,
		sampler: ActorRef<SamplerMessage>,
	) -> Result<()> {
		let accumulator = ioc
			.try_resolve::<Arc<StatementMetricsAccumulator>>()
			.unwrap_or_else(|| Arc::new(StatementMetricsAccumulator::new()));

		let event_bus = ioc.resolve::<EventBus>()?;
		let single_store = ioc.resolve::<SingleStore>()?;
		let multi_store = ioc.resolve::<MultiStore>()?;

		let clock = ioc.resolve::<Clock>()?;
		let actor = MetricsFlushActor::new(accumulator, event_bus.clone(), single_store, multi_store)
			.with_drain(engine.clone(), clock)
			.with_config(Arc::new(engine.catalog()) as Arc<dyn GetConfig>)
			.with_epoch_gauge(epoch_gauge)
			.with_sampler(sampler);

		let handle = spawner.spawn_coordination("metrics-flush", actor);
		ioc.register_service(handle.actor_ref().clone());
		Self::register_listeners(&event_bus, handle.actor_ref().clone());

		Ok(())
	}

	#[inline]
	fn register_listeners(event_bus: &EventBus, actor_ref: ActorRef<MetricsMessage>) {
		event_bus.register::<RequestExecutedEvent, _>(RequestMetricsEventListener::new(actor_ref.clone()));
		event_bus.register::<MultiCommittedEvent, _>(MultiCommittedListener::new(actor_ref.clone()));
		event_bus.register::<MultiSweptEvent, _>(MultiSweptListener::new(actor_ref.clone()));
		event_bus.register::<CdcWrittenEvent, _>(CdcWrittenListener::new(actor_ref.clone()));
		event_bus.register::<CdcEvictedEvent, _>(CdcEvictedListener::new(actor_ref.clone()));
		event_bus.register::<VersionEpochSampledEvent, _>(VersionEpochSampledListener::new(actor_ref));
	}
}
