// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod consumer;
mod factory;
pub mod intercept;

use std::{any::Any, sync::Arc, time::Duration};

pub use factory::FlowSubsystemFactory;
use reifydb_cdc::{PollConsumer, PollConsumerConfig};
use reifydb_core::{
	Result,
	interface::{
		CdcConsumer, ConsumerId, Transaction,
		subsystem::{HealthStatus, Subsystem, workerpool::Priority},
	},
	ioc::IocContainer,
};
use reifydb_engine::{StandardEngine, StandardEvaluator};

use self::consumer::FlowConsumer;
use crate::engine::FlowEngine;

pub struct FlowSubsystemConfig {
	/// Unique identifier for this consumer
	pub consumer_id: ConsumerId,
	/// How often to poll for new CDC events
	pub poll_interval: Duration,
	/// Priority for the polling task in the worker pool
	pub priority: Priority,
}

pub struct FlowSubsystem<T: Transaction> {
	consumer: PollConsumer<T, FlowConsumer>,
	running: bool,
}

impl<T: Transaction> FlowSubsystem<T> {
	pub fn new(
		cfg: FlowSubsystemConfig,
		ioc: &IocContainer,
	) -> crate::Result<Self> {
		let engine = ioc.resolve::<StandardEngine<T>>()?;

		let flow_engine =
			Arc::new(FlowEngine::new(StandardEvaluator::default()));

		let consumer = FlowConsumer::new(flow_engine);

		Ok(Self {
			consumer: PollConsumer::new(
				PollConsumerConfig::new(
					cfg.consumer_id.clone(),
					cfg.poll_interval,
				),
				engine,
				consumer,
			),
			running: false,
		})
	}
}

impl<T: Transaction> Drop for FlowSubsystem<T> {
	fn drop(&mut self) {
		let _ = self.shutdown();
	}
}

impl<T: Transaction> Subsystem for FlowSubsystem<T> {
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
