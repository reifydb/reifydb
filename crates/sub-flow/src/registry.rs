// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! In-memory registry for tracking spawned flow consumers.

use std::{
	collections::{HashMap, HashSet},
	time::Duration,
};

use parking_lot::{RwLock, RwLockReadGuard};
use reifydb_core::interface::{FlowId, PrimitiveId};
use tracing::debug;

use crate::flow::FlowConsumer;

/// Registry for tracking active flow consumers.
pub struct FlowConsumerRegistry {
	consumers: RwLock<HashMap<FlowId, FlowConsumerHandle>>,
}

pub(crate) struct FlowConsumerHandle {
	pub(crate) flow_consumer: FlowConsumer,
}

impl FlowConsumerRegistry {
	/// Create a new empty registry.
	pub fn new() -> Self {
		Self {
			consumers: RwLock::new(HashMap::new()),
		}
	}

	/// Register a flow consumer.
	pub fn register(&self, flow_id: FlowId, flow_consumer: FlowConsumer) {
		let handle = FlowConsumerHandle {
			flow_consumer,
		};

		let mut consumers = self.consumers.write();
		consumers.insert(flow_id, handle);
		debug!(flow_id = flow_id.0, "flow consumer registered");
	}

	/// Check if a flow is already registered.
	pub fn contains(&self, flow_id: FlowId) -> bool {
		let consumers = self.consumers.read();
		consumers.contains_key(&flow_id)
	}

	/// Deregister a flow consumer and return it for shutdown.
	pub fn deregister(&self, flow_id: FlowId) -> Option<FlowConsumer> {
		let mut consumers = self.consumers.write();
		let handle = consumers.remove(&flow_id)?;
		debug!(flow_id = flow_id.0, "flow consumer deregistered");
		Some(handle.flow_consumer)
	}

	/// Get all registered flow IDs.
	pub fn flow_ids(&self) -> Vec<FlowId> {
		let consumers = self.consumers.read();
		consumers.keys().copied().collect()
	}

	/// Get data for all registered flows: (flow_id, sources).
	///
	/// This returns flow IDs and their source sets. Versions are retrieved
	/// separately by querying each consumer's CDC checkpoint.
	pub fn all_flow_info(&self) -> Vec<(FlowId, HashSet<PrimitiveId>)> {
		let consumers = self.consumers.read();
		consumers
			.values()
			.map(|handle| {
				let flow_id = handle.flow_consumer.flow_id();
				let sources = handle.flow_consumer.sources().clone();
				(flow_id, sources)
			})
			.collect()
	}

	/// Get a reference to the consumers map for internal use.
	///
	/// Returns a read lock guard that allows read-only access to the consumers map.
	pub(crate) fn consumers_read(&self) -> RwLockReadGuard<'_, HashMap<FlowId, FlowConsumerHandle>> {
		self.consumers.read()
	}

	/// Shutdown all flow consumers with a timeout.
	pub fn shutdown_all(&self, _drain_timeout: Duration) {
		let flow_ids = self.flow_ids();

		debug!(count = flow_ids.len(), "shutting down all flow consumers");

		for flow_id in flow_ids {
			if let Some(consumer) = self.deregister(flow_id) {
				consumer.shutdown().unwrap()
			}
		}

		debug!("all flow consumers shutdown complete");
	}
}

impl Default for FlowConsumerRegistry {
	fn default() -> Self {
		Self::new()
	}
}
