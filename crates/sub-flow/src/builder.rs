// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Builder pattern for configuring the flow subsystem

use std::{path::PathBuf, sync::Arc, time::Duration};

use reifydb_core::interface::{CdcConsumerId, FlowNodeId};
use reifydb_rql::expression::Expression;
use reifydb_sub_api::Priority;

use crate::{operator::BoxedOperator, subsystem::FlowSubsystemConfig};

/// Type alias for operator factory functions
pub type OperatorFactory =
	Arc<dyn Fn(FlowNodeId, &[Expression<'static>]) -> crate::Result<BoxedOperator> + Send + Sync>;

pub struct FlowBuilder {
	consumer_id: CdcConsumerId,
	poll_interval: Duration,
	priority: Priority,
	operators: Vec<(String, OperatorFactory)>,
	max_batch_size: Option<u64>,
	operators_dir: Option<PathBuf>,
}

impl Default for FlowBuilder {
	fn default() -> Self {
		Self::new()
	}
}

impl FlowBuilder {
	/// Create a new FlowBuilder with default settings
	pub fn new() -> Self {
		Self {
			consumer_id: CdcConsumerId::flow_consumer(),
			poll_interval: Duration::from_millis(1),
			priority: Priority::Normal,
			operators: Vec::new(),
			max_batch_size: Some(10),
			operators_dir: None,
		}
	}

	/// Set the consumer ID for the flow subsystem
	pub fn consumer_id(mut self, id: CdcConsumerId) -> Self {
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

	/// Set the maximum batch size for CDC polling
	pub fn max_batch_size(mut self, size: u64) -> Self {
		self.max_batch_size = Some(size);
		self
	}

	/// Set the directory to scan for FFI operator shared libraries
	pub fn operators_dir(mut self, path: PathBuf) -> Self {
		self.operators_dir = Some(path);
		self
	}

	/// Register a custom operator factory
	pub fn register_operator<F>(mut self, name: impl Into<String>, factory: F) -> Self
	where
		F: Fn(FlowNodeId, &[Expression<'static>]) -> crate::Result<BoxedOperator> + Send + Sync + 'static,
	{
		self.operators.push((name.into(), Arc::new(factory)));
		self
	}

	/// Build the configuration
	pub(crate) fn build_config(self) -> FlowSubsystemConfig {
		FlowSubsystemConfig {
			consumer_id: self.consumer_id,
			poll_interval: self.poll_interval,
			priority: self.priority,
			operators: self.operators,
			max_batch_size: self.max_batch_size,
			operators_dir: self.operators_dir,
		}
	}
}
