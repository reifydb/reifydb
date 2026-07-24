// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_cdc::storage::CdcStore;
use reifydb_core::{
	event::EventBus,
	interface::catalog::config::{ConfigKey, GetConfig},
	lifecycle::{
		gate::{Gated, RetentionStartupGate},
		registry::LifecycleRegistry,
	},
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{actor::system::ActorSpawner, version_epoch::EpochRetention};
use reifydb_store_multi::MultiStore;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_value::Result;
use tracing::{info, warn};

use crate::{
	actor::LifecycleActor,
	cdc::ttl::CdcTtlTask,
	gc::{
		epoch::durable::{EpochLogTask, hydrate},
		historical::actor::HistoricalGcTask,
		operator::actor::OperatorTtlTask,
	},
	plane::{RetentionPlane, horizon::max_retention_horizon, measured::Measured},
	retention::evictor::RetentionEvictTask,
	store::{
		compaction::CompactionReclaimTask, flush::PersistentFlushTask, tombstone::TombstoneReapTask,
		vacuum::VacuumBudgetTask,
	},
	subsystem::LifecycleSubsystem,
};

pub struct LifecycleSubsystemFactory;

impl Default for LifecycleSubsystemFactory {
	fn default() -> Self {
		Self
	}
}

impl SubsystemFactory for LifecycleSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let spawner = ioc.resolve::<ActorSpawner>()?;
		let registry = ioc.resolve::<LifecycleRegistry>()?;

		let store = match engine.multi_owned().store() {
			MultiStore::Standard(s) => s.clone(),
		};
		let catalog = engine.catalog();

		let plane = RetentionPlane::for_engine(&engine);
		store.set_eviction_watermark(plane.eviction_watermark(engine.clock().clone()));

		let gate = RetentionStartupGate::arm(
			engine.clock().clone(),
			catalog.get_config_duration(ConfigKey::RetentionStartupGrace),
		);

		let bucket = catalog.get_config_duration(ConfigKey::EpochBucketInterval);
		let retention = EpochRetention {
			coarse_bucket_nanos: bucket.to_std().as_nanos() as u64,
			..EpochRetention::default()
		};
		engine.version_epoch().set_retention(retention);
		info!(
			bucket = %bucket,
			coverage_days = retention.guaranteed_coverage_nanos() / (24 * 60 * 60 * 1_000_000_000),
			"version epoch retention applied"
		);

		let horizon = max_retention_horizon(&catalog);
		match hydrate(&engine, horizon) {
			Ok(samples) => info!(samples, horizon = %horizon, "version epoch hydrated"),
			Err(e) => {
				warn!(error = %e, "version epoch hydration failed; ttls resolve only from this uptime")
			}
		}

		registry.register(Box::new(Measured::new(
			EpochLogTask::new(engine.clone(), gate.clone()),
			plane.clone(),
		)));
		registry.register(Box::new(Measured::new(
			Gated::new(RetentionEvictTask::silent(engine.clone(), plane.clone()), gate.clone()),
			plane.clone(),
		)));
		registry.register(Box::new(Measured::new(
			Gated::new(RetentionEvictTask::announced(engine.clone(), plane.clone()), gate.clone()),
			plane.clone(),
		)));
		registry.register(Box::new(Measured::new(
			Gated::new(
				OperatorTtlTask::new(
					store.clone(),
					catalog.clone(),
					plane.clone(),
					engine.clock().clone(),
				),
				gate.clone(),
			),
			plane.clone(),
		)));

		let config: Arc<dyn GetConfig> = Arc::new(catalog.clone());

		if let Some(flush_engine) = store.flush_engine() {
			registry.register(Box::new(Measured::new(
				PersistentFlushTask::new(
					flush_engine,
					config.clone(),
					catalog.get_config_duration(ConfigKey::MultiFlushInterval),
				),
				plane.clone(),
			)));
		}

		let compaction_engine = store.compaction_engine();
		let interval = compaction_engine.flush_interval();
		registry.register(Box::new(Measured::new(
			CompactionReclaimTask::new(compaction_engine, interval),
			plane.clone(),
		)));

		if store.persistent().is_some() {
			registry.register(Box::new(Measured::new(
				TombstoneReapTask::new(
					store.clone(),
					plane.clone(),
					engine.clock().clone(),
					config.clone(),
				),
				plane.clone(),
			)));
			registry.register(Box::new(Measured::new(
				VacuumBudgetTask::new(store.clone(), plane.clone(), config.clone()),
				plane.clone(),
			)));
		}

		registry.register(Box::new(Measured::new(
			HistoricalGcTask::new(store, plane.clone(), engine.clock().clone(), config),
			plane.clone(),
		)));

		if let Some(cdc_store) = ioc.try_resolve::<CdcStore>() {
			let event_bus = ioc.resolve::<EventBus>()?;
			registry.register(Box::new(Measured::new(
				Gated::new(
					CdcTtlTask::new(cdc_store, engine.clone(), event_bus, engine.clock().clone()),
					gate.clone(),
				),
				plane.clone(),
			)));
		}

		let tasks = registry.take();
		let task_names = tasks.iter().map(|task| task.name()).collect();
		let covered = tasks.iter().flat_map(|task| task.classes()).copied().collect();
		let actor_ref = LifecycleActor::spawn(&spawner, tasks);

		Ok(Box::new(LifecycleSubsystem::new(actor_ref, task_names, covered, plane)))
	}
}
