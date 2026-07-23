// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_cdc::storage::CdcStore;
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::catalog::config::{ConfigKey, GetConfig},
	lifecycle::{
		epoch::EpochSource,
		gate::{Gated, RetentionStartupGate},
		registry::LifecycleRegistry,
	},
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::actor::system::ActorSpawner;
use reifydb_store_multi::MultiStore;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_value::Result;
use tracing::{info, warn};

use crate::{
	actor::LifecycleActor,
	cdc::ttl::CdcTtlTask,
	gc::{
		epoch::{
			actor::spawn_version_epoch_sampler,
			durable::{EpochLogTask, hydrate},
		},
		historical::actor::HistoricalGcTask,
		operator::actor::OperatorTtlTask,
	},
	plane::horizon::max_retention_horizon,
	retention::evictor::RetentionEvictTask,
	store::{drop::DropReclaimTask, flush::PersistentFlushTask},
	subsystem::LifecycleSubsystem,
};

struct EngineEpochSource {
	engine: StandardEngine,
}

impl EpochSource for EngineEpochSource {
	fn now_nanos(&self) -> u64 {
		self.engine.clock().now_nanos()
	}

	fn current_version(&self) -> Option<CommitVersion> {
		self.engine.current_version().ok()
	}
}

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

		let epoch = engine.version_epoch().clone();
		let epoch_config: Arc<dyn GetConfig> = Arc::new(catalog.clone());
		let _epoch_sampler = spawn_version_epoch_sampler(
			epoch.clone(),
			spawner.clone(),
			EngineEpochSource {
				engine: engine.clone(),
			},
			epoch_config,
		);

		store.set_eviction_watermark(Arc::new(engine.clone()));

		let gate = RetentionStartupGate::arm(
			engine.clock().clone(),
			catalog.get_config_duration(ConfigKey::RetentionStartupGrace),
		);

		let horizon = max_retention_horizon(&catalog);
		match hydrate(&engine, horizon) {
			Ok(samples) => info!(samples, horizon = %horizon, "version epoch hydrated"),
			Err(e) => warn!(error = %e, "version epoch hydration failed; ttls resolve only from this uptime"),
		}

		registry.register(Box::new(EpochLogTask::new(engine.clone(), gate.clone())));
		registry.register(Box::new(Gated::new(RetentionEvictTask::new(engine.clone()), gate.clone())));
		registry.register(Box::new(Gated::new(
			OperatorTtlTask::new(store.clone(), catalog.clone(), epoch, engine.clock().clone()),
			gate.clone(),
		)));

		if let Some(flush_engine) = store.flush_engine() {
			registry.register(Box::new(PersistentFlushTask::new(
				flush_engine,
				catalog.get_config_duration(ConfigKey::MultiFlushInterval),
			)));
		}

		if let Some(drop_engine) = store.drop_engine() {
			let interval = drop_engine.flush_interval();
			registry.register(Box::new(DropReclaimTask::new(drop_engine, interval)));
		}

		let config: Arc<dyn GetConfig> = Arc::new(catalog.clone());
		registry.register(Box::new(HistoricalGcTask::new(store, engine.clone(), config)));

		if let Some(cdc_store) = ioc.try_resolve::<CdcStore>() {
			let event_bus = ioc.resolve::<EventBus>()?;
			registry.register(Box::new(Gated::new(
				CdcTtlTask::new(cdc_store, engine.clone(), event_bus, engine.clock().clone()),
				gate.clone(),
			)));
		}

		let tasks = registry.take();
		let task_names = tasks.iter().map(|task| task.name()).collect();
		let actor_ref = LifecycleActor::spawn(&spawner, tasks);

		Ok(Box::new(LifecycleSubsystem::new(actor_ref, task_names)))
	}
}
