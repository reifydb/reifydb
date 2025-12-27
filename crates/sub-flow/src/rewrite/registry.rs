// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! In-memory registry for tracking spawned flow consumers.

use std::{
	collections::{HashMap, HashSet},
	time::Duration,
};

use reifydb_core::interface::{FlowId, PrimitiveId};
use tokio::{
	sync::{RwLock, RwLockReadGuard},
	time::{Instant, timeout},
};
use tracing::{debug, error, warn};

use super::flow::FlowConsumer;

/// Registry for tracking active flow consumers.
pub struct FlowConsumerRegistry {
	consumers: RwLock<HashMap<FlowId, FlowConsumerHandle>>,
}

pub(crate) struct FlowConsumerHandle {
	pub(crate) flow_consumer: FlowConsumer,
	#[allow(dead_code)]
	spawned_at: Instant,
}

impl FlowConsumerRegistry {
	/// Create a new empty registry.
	pub fn new() -> Self {
		Self {
			consumers: RwLock::new(HashMap::new()),
		}
	}

	/// Register a flow consumer.
	pub async fn register(&self, flow_id: FlowId, flow_consumer: FlowConsumer) {
		let handle = FlowConsumerHandle {
			flow_consumer,
			spawned_at: Instant::now(),
		};

		let mut consumers = self.consumers.write().await;
		consumers.insert(flow_id, handle);
		debug!(flow_id = flow_id.0, "flow consumer registered");
	}

	/// Check if a flow is already registered.
	pub async fn contains(&self, flow_id: FlowId) -> bool {
		let consumers = self.consumers.read().await;
		consumers.contains_key(&flow_id)
	}

	/// Deregister a flow consumer and return it for shutdown.
	pub async fn deregister(&self, flow_id: FlowId) -> Option<FlowConsumer> {
		let mut consumers = self.consumers.write().await;
		let handle = consumers.remove(&flow_id)?;
		debug!(flow_id = flow_id.0, "flow consumer deregistered");
		Some(handle.flow_consumer)
	}

	/// Get all registered flow IDs.
	pub async fn flow_ids(&self) -> Vec<FlowId> {
		let consumers = self.consumers.read().await;
		consumers.keys().copied().collect()
	}

	/// Get data for all registered flows: (flow_id, sources).
	///
	/// This returns flow IDs and their source sets. Versions are retrieved
	/// separately by querying each consumer's CDC checkpoint.
	pub async fn all_flow_info(&self) -> Vec<(FlowId, HashSet<PrimitiveId>)> {
		let consumers = self.consumers.read().await;
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
	pub(crate) async fn consumers_read(&self) -> RwLockReadGuard<'_, HashMap<FlowId, FlowConsumerHandle>> {
		self.consumers.read().await
	}

	/// Shutdown all flow consumers with a timeout.
	pub async fn shutdown_all(&self, drain_timeout: Duration) {
		let flow_ids = self.flow_ids().await;
		let drain_deadline = Instant::now() + drain_timeout;

		debug!(count = flow_ids.len(), "shutting down all flow consumers");

		for flow_id in flow_ids {
			if let Some(consumer) = self.deregister(flow_id).await {
				let remaining = drain_deadline.saturating_duration_since(Instant::now());

				match timeout(remaining, consumer.shutdown()).await {
					Ok(Ok(())) => {
						debug!(flow_id = flow_id.0, "flow consumer shutdown successfully");
					}
					Ok(Err(e)) => {
						error!(flow_id = flow_id.0, error = %e, "flow consumer shutdown failed");
					}
					Err(_) => {
						warn!(flow_id = flow_id.0, "flow consumer shutdown timed out");
					}
				}
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
