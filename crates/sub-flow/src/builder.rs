// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Builder pattern for configuring the flow subsystem

use std::{marker::PhantomData, sync::Arc, time::Duration};

use reifydb_core::interface::{ConsumerId, FlowNodeId, Transaction, expression::Expression};
use reifydb_sub_api::Priority;

use crate::{operator::Operator, subsystem::FlowSubsystemConfig};

/// Type alias for operator factory functions
pub type OperatorFactory<T> =
	Arc<dyn Fn(FlowNodeId, &[Expression<'static>]) -> crate::Result<Box<dyn Operator<T>>> + Send + Sync>;

pub struct FlowBuilder<T: Transaction> {
	consumer_id: ConsumerId,
	poll_interval: Duration,
	priority: Priority,
	operators: Vec<(String, OperatorFactory<T>)>,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> Default for FlowBuilder<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T: Transaction> FlowBuilder<T> {
	/// Create a new FlowBuilder with default settings
	pub fn new() -> Self {
		Self {
			consumer_id: ConsumerId::flow_consumer(),
			poll_interval: Duration::from_millis(1),
			priority: Priority::Normal,
			operators: Vec::new(),
			_phantom: PhantomData,
		}
	}

	/// Set the consumer ID for the flow subsystem
	pub fn consumer_id(mut self, id: ConsumerId) -> Self {
		self.consumer_id = id;
		self
	}

	/// Set the poll interval for checking new CDC events
	pub fn poll_interval(mut self, interval: Duration) -> Self {
		self.poll_interval = interval;
		self
	}

	/// Set the priority for the polling task in the worker pool
	pub fn priority(mut self, priority: Priority) -> Self {
		self.priority = priority;
		self
	}

	/// Register a custom operator factory
	pub fn register_operator<F>(mut self, name: impl Into<String>, factory: F) -> Self
	where
		F: Fn(FlowNodeId, &[Expression<'static>]) -> crate::Result<Box<dyn Operator<T>>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.operators.push((name.into(), Arc::new(factory)));
		self
	}

	/// Build the configuration
	pub(crate) fn build_config(self) -> FlowSubsystemConfig<T> {
		FlowSubsystemConfig {
			consumer_id: self.consumer_id,
			poll_interval: self.poll_interval,
			priority: self.priority,
			operators: self.operators,
		}
	}
}
