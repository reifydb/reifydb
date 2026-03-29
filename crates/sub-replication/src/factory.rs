// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_cdc::storage::CdcStore;
use reifydb_core::{event::EventBus, util::ioc::IocContainer};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_type::Result;

use crate::{
	builder::{ReplicationConfig, ReplicationConfigurator},
	subsystem::ReplicationSubsystem,
};

pub struct ReplicationSubsystemFactory {
	config_fn: Box<dyn FnOnce() -> ReplicationConfig + Send>,
}

impl ReplicationSubsystemFactory {
	pub fn new<F, C>(configurator: F) -> Self
	where
		F: FnOnce(ReplicationConfigurator) -> C + Send + 'static,
		C: Into<ReplicationConfig>,
	{
		Self {
			config_fn: Box::new(move || configurator(ReplicationConfigurator).into()),
		}
	}
}

impl SubsystemFactory for ReplicationSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let runtime = ioc.resolve::<SharedRuntime>()?;
		let engine = ioc.resolve::<StandardEngine>()?;

		let config = (self.config_fn)();

		let subsystem = match config {
			ReplicationConfig::Primary(config) => {
				let cdc_store = ioc.resolve::<CdcStore>()?;
				let event_bus = ioc.resolve::<EventBus>()?;
				ReplicationSubsystem::primary(config, cdc_store, event_bus, runtime)
			}
			ReplicationConfig::Replica(config) => ReplicationSubsystem::replica(config, engine, runtime),
		};

		Ok(Box::new(subsystem))
	}
}
