// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Builder pattern for configuring the flow subsystem

use std::time::Duration;

use reifydb_core::interface::{ConsumerId, subsystem::workerpool::Priority};

use crate::subsystem::FlowSubsystemConfig;

pub struct FlowBuilder {
	consumer_id: ConsumerId,
	poll_interval: Duration,
	priority: Priority,
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
			consumer_id: ConsumerId::flow_consumer(),
			poll_interval: Duration::from_millis(1),
			priority: Priority::Normal,
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

	/// Build the configuration
	pub(crate) fn build_config(self) -> FlowSubsystemConfig {
		FlowSubsystemConfig {
			consumer_id: self.consumer_id,
			poll_interval: self.poll_interval,
			priority: self.priority,
		}
	}
}
