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
		subsystem::{HealthStatus, Subsystem},
	},
};
use reifydb_engine::{StandardEngine, StandardEvaluator};

use self::consumer::FlowConsumer;
use crate::engine::FlowEngine;

pub struct FlowSubsystem<T: Transaction> {
	engine: StandardEngine<T>,
	flow_engine: Arc<FlowEngine<StandardEvaluator>>,
	poll_consumer: Option<PollConsumer<T, FlowConsumer>>,
	#[allow(dead_code)]
	consumer_id: ConsumerId,
	poll_config: PollConsumerConfig,
	running: bool,
}

impl<T: Transaction> FlowSubsystem<T> {
	pub fn new(engine: StandardEngine<T>) -> Self {
		Self::with_config(
			engine,
			ConsumerId::flow_consumer(),
			PollConsumerConfig::new(
				ConsumerId::flow_consumer(),
				Duration::from_millis(100),
			),
		)
	}

	pub fn with_config(
		engine: StandardEngine<T>,
		consumer_id: ConsumerId,
		poll_config: PollConsumerConfig,
	) -> Self {
		let flow_engine =
			Arc::new(FlowEngine::new(StandardEvaluator::default()));

		Self {
			engine,
			flow_engine,
			poll_consumer: None,
			consumer_id,
			poll_config,
			running: false,
		}
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

		// Create and start the CDC poll consumer with flow engine
		let consumer = FlowConsumer::new(Arc::clone(&self.flow_engine));
		let mut poll_consumer = PollConsumer::new(
			self.poll_config.clone(),
			self.engine.clone(),
			consumer,
		);

		poll_consumer.start()?;
		self.poll_consumer = Some(poll_consumer);
		self.running = true;

		Ok(())
	}

	fn shutdown(&mut self) -> Result<()> {
		if !self.running {
			return Ok(());
		}

		// Stop the poll consumer
		if let Some(mut consumer) = self.poll_consumer.take() {
			consumer.stop()?;
		}

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
