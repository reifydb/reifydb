// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod consumer;
mod factory;
pub mod intercept;

use std::{any::Any, time::Duration};

pub use factory::FlowSubsystemFactory;
use reifydb_cdc::{CdcConsumer, PollConsumer, PollConsumerConfig};
use reifydb_core::{
	Result,
	interface::{
		CdcConsumerId,
		version::{ComponentType, HasVersion, SystemVersion},
	},
	ioc::IocContainer,
};
use reifydb_engine::StandardEngine;
use reifydb_sub_api::{HealthStatus, Priority, Subsystem};

use self::consumer::FlowConsumer;
use crate::builder::OperatorFactory;

pub struct FlowSubsystemConfig {
	/// Unique identifier for this consumer
	pub consumer_id: CdcConsumerId,
	/// How often to poll for new CDC events
	pub poll_interval: Duration,
	/// Priority for the polling task in the worker pool
	pub priority: Priority,
	/// Custom operator factories
	pub operators: Vec<(String, OperatorFactory)>,
	/// Maximum batch size for CDC polling (None = unbounded)
	pub max_batch_size: Option<u64>,
}

pub struct FlowSubsystem {
	consumer: PollConsumer<FlowConsumer>,
	running: bool,
}

impl FlowSubsystem {
	pub fn new(cfg: FlowSubsystemConfig, ioc: &IocContainer) -> Result<Self> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let consumer = FlowConsumer::new(engine.clone(), cfg.operators);

		Ok(Self {
			consumer: PollConsumer::new(
				PollConsumerConfig::new(cfg.consumer_id.clone(), cfg.poll_interval, cfg.max_batch_size),
				engine,
				consumer,
			),
			running: false,
		})
	}
}

impl Drop for FlowSubsystem {
	fn drop(&mut self) {
		let _ = self.shutdown();
	}
}

impl Subsystem for FlowSubsystem {
	fn name(&self) -> &'static str {
		"Flow"
	}

	fn start(&mut self) -> Result<()> {
		if self.running {
			return Ok(());
		}

		self.consumer.start()?;
		self.running = true;

		Ok(())
	}

	fn shutdown(&mut self) -> Result<()> {
		if !self.running {
			return Ok(());
		}

		self.consumer.stop()?;
		self.running = false;
		Ok(())
	}

	fn is_running(&self) -> bool {
		self.running
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

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}

impl HasVersion for FlowSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "sub-flow".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Data flow and stream processing subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}
