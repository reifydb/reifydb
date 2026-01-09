// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Subscription registry for tracking active WebSocket subscriptions.
//!
//! The registry maintains mappings between subscription IDs, connections,
//! and push channels to enable server-initiated message delivery.

use dashmap::DashMap;
use reifydb_sub_server::ResponseFrame;
use reifydb_type::Uuid7;
use tokio::sync::mpsc;

/// Unique identifier for a subscription.
pub type SubscriptionId = Uuid7;

/// Unique identifier for a WebSocket connection.
pub type ConnectionId = Uuid7;

/// Message sent to a connection for push delivery.
#[derive(Debug, Clone)]
pub enum PushMessage {
	/// A change notification for a subscription.
	Change {
		subscription_id: SubscriptionId,
		frame: ResponseFrame,
	},
}

/// Internal state for a subscription.
struct SubscriptionState {
	connection_id: ConnectionId,
	push_tx: mpsc::Sender<PushMessage>,
	#[allow(dead_code)]
	query: String,
}

/// Registry tracking subscriptions across all WebSocket connections.
///
/// The registry is thread-safe and can be shared across connection handlers
/// and the push broadcast thread.
pub struct SubscriptionRegistry {
	/// subscription_id → subscription state
	subscriptions: DashMap<SubscriptionId, SubscriptionState>,
	/// connection_id → list of subscription_ids (for cleanup on disconnect)
	connections: DashMap<ConnectionId, Vec<SubscriptionId>>,
}

impl SubscriptionRegistry {
	/// Create a new empty registry.
	pub fn new() -> Self {
		Self {
			subscriptions: DashMap::new(),
			connections: DashMap::new(),
		}
	}

	/// Register a new subscription for a connection.
	///
	/// Returns the generated subscription ID.
	pub fn subscribe(
		&self,
		connection_id: ConnectionId,
		query: String,
		push_tx: mpsc::Sender<PushMessage>,
	) -> SubscriptionId {
		let subscription_id = Uuid7::generate();

		// Store subscription state
		self.subscriptions.insert(
			subscription_id,
			SubscriptionState {
				connection_id,
				push_tx,
				query,
			},
		);

		// Track subscription for connection cleanup
		self.connections.entry(connection_id).or_default().push(subscription_id);

		tracing::debug!("Registered subscription {} for connection {}", subscription_id, connection_id);

		subscription_id
	}

	/// Unsubscribe a specific subscription.
	///
	/// Returns true if the subscription existed and was removed.
	pub fn unsubscribe(&self, subscription_id: SubscriptionId) -> bool {
		if let Some((_, state)) = self.subscriptions.remove(&subscription_id) {
			// Remove from connection's subscription list
			if let Some(mut subs) = self.connections.get_mut(&state.connection_id) {
				subs.retain(|id| *id != subscription_id);
			}

			tracing::debug!("Unsubscribed subscription {}", subscription_id);
			true
		} else {
			false
		}
	}

	/// Cleanup all subscriptions for a connection.
	///
	/// Called when a WebSocket connection is closed.
	pub fn cleanup_connection(&self, connection_id: ConnectionId) {
		if let Some((_, subscription_ids)) = self.connections.remove(&connection_id) {
			for sub_id in subscription_ids {
				self.subscriptions.remove(&sub_id);
			}
			tracing::debug!("Cleaned up subscriptions for disconnected connection {}", connection_id);
		}
	}

	/// Broadcast a message to all active subscriptions.
	///
	/// Used by the test push thread to send periodic updates.
	pub async fn broadcast(&self, frame: ResponseFrame) {
		for entry in self.subscriptions.iter() {
			let subscription_id = *entry.key();
			let state = entry.value();

			let msg = PushMessage::Change {
				subscription_id,
				frame: frame.clone(),
			};

			// Try to send, ignore if channel is full or closed
			if let Err(e) = state.push_tx.try_send(msg) {
				tracing::warn!("Failed to push to subscription {}: {}", subscription_id, e);
			}
		}
	}

	/// Get the number of active subscriptions.
	#[allow(dead_code)]
	pub fn subscription_count(&self) -> usize {
		self.subscriptions.len()
	}

	/// Get the number of connections with subscriptions.
	#[allow(dead_code)]
	pub fn connection_count(&self) -> usize {
		self.connections.len()
	}
}

impl Default for SubscriptionRegistry {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_sub_server::ResponseColumn;
	use reifydb_type::Type;

	use super::*;

	#[tokio::test]
	async fn test_subscribe_unsubscribe() {
		let registry = SubscriptionRegistry::new();
		let connection_id = Uuid7::generate();
		let (tx, mut rx) = mpsc::channel(10);

		// Subscribe
		let sub_id = registry.subscribe(connection_id, "FROM test".to_string(), tx);
		assert_eq!(registry.subscription_count(), 1);

		// Broadcast with a ResponseFrame
		let frame = ResponseFrame {
			row_numbers: vec![0],
			columns: vec![ResponseColumn {
				name: "answer".to_string(),
				r#type: Type::Int8,
				data: vec!["42".to_string()],
			}],
		};
		registry.broadcast(frame).await;

		// Should receive message
		let msg = rx.try_recv().unwrap();
		match msg {
			PushMessage::Change {
				subscription_id,
				frame,
			} => {
				assert_eq!(subscription_id, sub_id);
				assert_eq!(frame.columns[0].name, "answer");
				assert_eq!(frame.columns[0].data[0], "42");
			}
		}

		// Unsubscribe
		assert!(registry.unsubscribe(sub_id));
		assert_eq!(registry.subscription_count(), 0);

		// Unsubscribe again should return false
		assert!(!registry.unsubscribe(sub_id));
	}

	#[tokio::test]
	async fn test_cleanup_connection() {
		let registry = SubscriptionRegistry::new();
		let connection_id = Uuid7::generate();
		let (tx1, _rx1) = mpsc::channel(10);
		let (tx2, _rx2) = mpsc::channel(10);

		// Subscribe twice
		let _sub1 = registry.subscribe(connection_id, "FROM test1".to_string(), tx1);
		let _sub2 = registry.subscribe(connection_id, "FROM test2".to_string(), tx2);
		assert_eq!(registry.subscription_count(), 2);

		// Cleanup connection
		registry.cleanup_connection(connection_id);
		assert_eq!(registry.subscription_count(), 0);
		assert_eq!(registry.connection_count(), 0);
	}
}
