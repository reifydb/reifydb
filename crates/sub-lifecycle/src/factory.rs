// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	event::EventBus,
	interface::catalog::config::{ConfigKey, GetConfig},
	lifecycle::{
		class::RetentionClass,
		coverage::RetentionCoverage,
		gate::{Gated, RetentionStartupGate},
		metrics::RetentionMetrics,
		registry::LifecycleRegistry,
	},
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::system::ActorSpawner,
	version_epoch::{BUCKET_WIDTH, EpochRetention, EpochSpan},
};
use reifydb_store_cdc::store::CdcStore;
use reifydb_store_multi::MultiStore;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_value::Result;
use tracing::{info, warn};

use crate::{
	actor::LifecycleActor,
	cdc::ttl::task::CdcTtlTask,
	gc::{
		epoch::durable::{EpochLogTask, hydrate},
		historical::actor::HistoricalGcTask,
	},
	plane::{RetentionPlane, horizon::max_retention_horizon, measured::Measured},
	queue::{reap::QueueLeaseReapTask, retention::QueueRetentionTask},
	retention::evictor::RetentionEvictTask,
	store::{flush::PersistentFlushTask, tombstone::TombstoneReapTask},
	subsystem::LifecycleSubsystem,
};

const NO_FLUSH_ENGINE: &str = "store has no flush engine";
const NO_PERSISTENT_TIER: &str = "store has no persistent tier";
const NO_CDC_STORE: &str = "no cdc store registered";

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
		let coverage = ioc.try_resolve::<RetentionCoverage>().unwrap_or_default();

		let store = match engine.multi_owned().store() {
			MultiStore::Standard(s) => s.clone(),
		};
		let catalog = engine.catalog();

		let plane = RetentionPlane::for_engine(&engine, ioc.resolve::<RetentionMetrics>()?);
		store.set_eviction_watermark(plane.eviction_watermark(engine.clock().clone()));

		let gate = RetentionStartupGate::arm(
			engine.clock().clone(),
			catalog.get_config_duration(ConfigKey::RetentionStartupGrace),
		);

		let bucket = catalog.get_config_duration(ConfigKey::EpochBucketInterval);
		let bucket_seconds = bucket.to_std().as_secs();
		assert!(
			bucket_seconds >= BUCKET_WIDTH.seconds(),
			"EpochBucketInterval is {bucket}, below the {}s epoch resolution: it would truncate to zero \
			 and silently disable coarse compaction, so the sample map would bound by count instead of \
			 by the retention horizon",
			BUCKET_WIDTH.seconds()
		);
		let retention = EpochRetention {
			coarse_bucket: EpochSpan::new(bucket_seconds),
			..EpochRetention::default()
		};
		engine.version_epoch().set_retention(retention);
		info!(
			bucket = %bucket,
			coverage_days = retention.guaranteed_coverage().seconds() / (24 * 60 * 60),
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
		let config: Arc<dyn GetConfig> = Arc::new(catalog.clone());

		if let Some(flush_engine) = store.flush_engine() {
			registry.register(Box::new(Measured::new(
				PersistentFlushTask::new(
					flush_engine,
					config.clone(),
					plane.clone(),
					engine.clock().clone(),
					catalog.get_config_duration(ConfigKey::MultiFlushInterval),
				),
				plane.clone(),
			)));
		} else {
			coverage.absent(RetentionClass::PersistentFlush, NO_FLUSH_ENGINE);
		}

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
		} else {
			coverage.absent(RetentionClass::TombstoneReap, NO_PERSISTENT_TIER);
		}

		registry.register(Box::new(Measured::new(
			QueueLeaseReapTask::new(engine.clone(), plane.clone(), engine.clock().clone(), config.clone()),
			plane.clone(),
		)));

		registry.register(Box::new(Measured::new(
			Gated::new(
				QueueRetentionTask::new(
					engine.clone(),
					plane.clone(),
					engine.clock().clone(),
					config.clone(),
				),
				gate.clone(),
			),
			plane.clone(),
		)));

		registry.register(Box::new(Measured::new(
			HistoricalGcTask::new(store, plane.clone(), engine.clock().clone(), config),
			plane.clone(),
		)));

		if let Some(cdc_store) = ioc.try_resolve::<CdcStore>() {
			let event_bus = ioc.resolve::<EventBus>()?;
			registry.register(Box::new(Measured::new(
				Gated::new(
					CdcTtlTask::new(
						cdc_store,
						engine.clone(),
						event_bus,
						engine.clock().clone(),
						Some(engine.checkpoint_floor()),
					),
					gate.clone(),
				),
				plane.clone(),
			)));
		} else {
			coverage.absent(RetentionClass::CdcTruncate, NO_CDC_STORE);
		}

		let tasks = registry.take();
		let task_names = tasks.iter().map(|task| task.name()).collect();
		for task in &tasks {
			for class in task.classes() {
				coverage.cover(*class, task.name());
			}
		}
		let actor_ref = LifecycleActor::spawn(&spawner, tasks);

		Ok(Box::new(LifecycleSubsystem::new(actor_ref, task_names, coverage, plane)))
	}
}
