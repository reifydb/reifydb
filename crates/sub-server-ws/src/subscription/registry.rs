// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Subscription registry for tracking active WebSocket subscriptions.
//!
//! The registry maintains mappings between subscription IDs, connections,
//! and push channels to enable server-initiated message delivery.

use dashmap::DashMap;
use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_sub_server::{format::WireFormat, response::resolve_response_json};
use reifydb_subscription::delivery::{DeliveryResult, SubscriptionDelivery};
use reifydb_type::value::{frame::frame::Frame, uuid::Uuid7};
use reifydb_wire_format::{
	encode::encode_frames,
	json::types::{ResponseColumn, ResponseFrame},
	options::EncodeOptions,
};
use serde_json::{Value as JsonValue, from_str, json};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::handler::{BinaryKind, encode_rbcf_envelope};

/// Unique identifier for a WebSocket connection.
pub type ConnectionId = Uuid7;

/// Message sent to a connection for push delivery.
#[derive(Debug, Clone)]
pub enum PushMessage {
	/// A JSON-encoded change notification for a subscription.
	ChangeJson {
		subscription_id: SubscriptionId,
		content_type: String,
		body: JsonValue,
	},
	/// A pre-encoded RBCF binary envelope ready to be sent as Message::Binary.
	ChangeRbcf {
		subscription_id: SubscriptionId,
		envelope: Vec<u8>,
	},
	/// The remote subscription has closed (upstream stream ended).
	Closed {
		subscription_id: SubscriptionId,
	},
}

/// Internal state for a subscription.
struct SubscriptionState {
	connection_id: ConnectionId,
	push_tx: mpsc::UnboundedSender<PushMessage>,
	format: WireFormat,
	#[allow(dead_code)]
	query: String,
}

/// Registry tracking subscriptions across all WebSocket connections.
///
/// The registry is thread-safe and can be shared across connection handler
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

	/// Register a new subscription for a connection using the provided subscription ID.
	///
	/// The subscription ID should be the database subscription ID returned from CREATE SUBSCRIPTION.
	pub fn subscribe(
		&self,
		subscription_id: SubscriptionId,
		connection_id: ConnectionId,
		query: String,
		push_tx: mpsc::UnboundedSender<PushMessage>,
		format: WireFormat,
	) {
		// Store subscription state
		self.subscriptions.insert(
			subscription_id,
			SubscriptionState {
				connection_id,
				push_tx,
				format,
				query,
			},
		);

		// Track subscription for connection cleanup
		self.connections.entry(connection_id).or_default().push(subscription_id);

		debug!("Registered subscription {} for connection {}", subscription_id, connection_id);
	}

	/// Get the push channel for a subscription.
	///
	/// Returns None if the subscription doesn't exist.
	pub fn get_push_channel(&self, subscription_id: &SubscriptionId) -> Option<mpsc::UnboundedSender<PushMessage>> {
		self.subscriptions.get(subscription_id).map(|state| state.push_tx.clone())
	}

	/// Get the push channel and chosen wire format for a subscription.
	pub fn get_push_target(
		&self,
		subscription_id: &SubscriptionId,
	) -> Option<(mpsc::UnboundedSender<PushMessage>, WireFormat)> {
		self.subscriptions.get(subscription_id).map(|state| (state.push_tx.clone(), state.format))
	}

	/// Unsubscribe a specific subscription.
	///
	/// Returns true if the subscription existed and was removed.
	pub fn unsubscribe(&self, subscription_id: SubscriptionId) -> bool {
		if let Some((_, state)) = self.subscriptions.remove(&subscription_id) {
			let connection_id = state.connection_id;

			// Remove from connection's subscription list and check if empty
			let should_remove_connection = {
				if let Some(mut subs) = self.connections.get_mut(&connection_id) {
					subs.retain(|id| *id != subscription_id);
					subs.is_empty()
				} else {
					false
				}
			};

			// If the connection has no more subscriptions, remove the entry entirely
			if should_remove_connection {
				self.connections.remove(&connection_id);
			}

			debug!("Unsubscribed subscription {}", subscription_id);
			true
		} else {
			false
		}
	}

	/// Cleanup all subscriptions for a connection.
	///
	/// Called when a WebSocket connection is closed.
	/// Returns the list of subscription IDs that were cleaned up.
	pub fn cleanup_connection(&self, connection_id: ConnectionId) -> Vec<SubscriptionId> {
		if let Some((_, subscription_ids)) = self.connections.remove(&connection_id) {
			for sub_id in &subscription_ids {
				self.subscriptions.remove(sub_id);
			}
			debug!("Cleaned up subscriptions for disconnected connection {}", connection_id);
			subscription_ids
		} else {
			Vec::new()
		}
	}

	/// Broadcast a message to all active subscriptions.
	///
	/// Used by the test push thread to send periodic updates. Subscriptions that
	/// requested RBCF format still receive JSON here because this helper is only
	/// used by tests that supply a ready-made JSON body.
	pub async fn broadcast(&self, content_type: String, body: JsonValue) {
		for entry in self.subscriptions.iter() {
			let subscription_id = *entry.key();
			let state = entry.value();

			let msg = PushMessage::ChangeJson {
				subscription_id,
				content_type: content_type.clone(),
				body: body.clone(),
			};

			// Try to send, ignore if channel is closed
			if let Err(e) = state.push_tx.send(msg) {
				warn!("Failed to push to subscription {}: {}", subscription_id, e);
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

	/// Log registry stats for debugging resource usage.
	#[allow(dead_code)]
	pub fn log_stats(&self) {
		info!(
			"Registry stats: {} subscriptions, {} connections",
			self.subscriptions.len(),
			self.connections.len()
		);
	}
}

impl Default for SubscriptionRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl SubscriptionDelivery for SubscriptionRegistry {
	fn try_deliver(&self, subscription_id: &SubscriptionId, columns: Columns) -> DeliveryResult {
		let (push_tx, format) = match self.get_push_target(subscription_id) {
			Some(t) => t,
			None => return DeliveryResult::Disconnected,
		};

		let msg = match format {
			WireFormat::Rbcf => {
				let frames = vec![Frame::from(columns)];
				let rbcf_bytes = match encode_frames(&frames, &EncodeOptions::fast()) {
					Ok(b) => b,
					Err(e) => {
						warn!("Failed to RBCF-encode change for {}: {}", subscription_id, e);
						return DeliveryResult::Disconnected;
					}
				};
				let envelope = encode_rbcf_envelope(
					BinaryKind::Change,
					&subscription_id.to_string(),
					&rbcf_bytes,
					None,
				);
				PushMessage::ChangeRbcf {
					subscription_id: *subscription_id,
					envelope,
				}
			}
			WireFormat::Frames => {
				// Convert Columns to ResponseFrame
				let row_numbers: Vec<u64> = columns.row_numbers.iter().map(|r| r.0).collect();
				let created_at: Vec<String> =
					columns.created_at.iter().map(|dt| dt.to_string()).collect();
				let updated_at: Vec<String> =
					columns.updated_at.iter().map(|dt| dt.to_string()).collect();
				let row_count = columns.row_count();

				let response_columns: Vec<ResponseColumn> = columns
					.columns
					.iter()
					.map(|col| {
						let data: Vec<String> = (0..row_count)
							.map(|idx| {
								let value = col.data().get_value(idx);
								value.to_string()
							})
							.collect();

						ResponseColumn {
							name: col.name.to_string(),
							r#type: col.data().get_type(),
							payload: data,
						}
					})
					.collect();

				let frame = ResponseFrame {
					row_numbers,
					created_at,
					updated_at,
					columns: response_columns,
				};

				let body = json!({ "frames": [frame] });

				PushMessage::ChangeJson {
					subscription_id: *subscription_id,
					content_type: "application/vnd.reifydb.json".to_string(),
					body,
				}
			}
			WireFormat::Json => {
				let frames = vec![Frame::from(columns)];
				let resolved = match resolve_response_json(frames, false) {
					Ok(r) => r,
					Err(e) => {
						warn!("Failed to JSON-encode change for {}: {}", subscription_id, e);
						return DeliveryResult::Disconnected;
					}
				};
				let body = from_str(&resolved.body).unwrap_or(JsonValue::String(resolved.body));
				PushMessage::ChangeJson {
					subscription_id: *subscription_id,
					content_type: "application/json".to_string(),
					body,
				}
			}
		};

		match push_tx.send(msg) {
			Ok(_) => DeliveryResult::Delivered,
			Err(_) => DeliveryResult::Disconnected,
		}
	}

	fn active_subscriptions(&self) -> Vec<SubscriptionId> {
		self.subscriptions.iter().map(|entry| *entry.key()).collect()
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};

	use super::*;

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	#[tokio::test]
	async fn test_subscribe_unsubscribe() {
		let (_, clock, rng) = test_clock_and_rng();
		let registry = SubscriptionRegistry::new();
		let connection_id = Uuid7::generate(&clock, &rng);
		let (tx, mut rx) = mpsc::unbounded_channel();

		let sub_id = SubscriptionId(12345);
		registry.subscribe(sub_id, connection_id, "FROM test".to_string(), tx, WireFormat::Frames);
		assert_eq!(registry.subscription_count(), 1);

		// Broadcast with a content_type + body
		let body = json!({
			"frames": [{
				"row_numbers": [0],
				"columns": [{
					"name": "answer",
					"type": "Int8",
					"payload": ["42"]
				}]
			}]
		});
		registry.broadcast("application/vnd.reifydb.json".to_string(), body.clone()).await;

		// Should receive message
		let msg = rx.try_recv().unwrap();
		match msg {
			PushMessage::ChangeJson {
				subscription_id,
				body: received_body,
				..
			} => {
				assert_eq!(subscription_id, sub_id);
				assert_eq!(received_body, body);
			}
			PushMessage::ChangeRbcf {
				..
			} => panic!("Unexpected ChangeRbcf message"),
			PushMessage::Closed {
				..
			} => panic!("Unexpected Closed message"),
		}

		// Unsubscribe
		assert!(registry.unsubscribe(sub_id));
		assert_eq!(registry.subscription_count(), 0);
		// Connection entry should be removed when last subscription is unsubscribed
		assert_eq!(registry.connection_count(), 0);

		// Unsubscribe again should return false
		assert!(!registry.unsubscribe(sub_id));
	}

	#[tokio::test]
	async fn test_cleanup_connection() {
		let (_, clock, rng) = test_clock_and_rng();
		let registry = SubscriptionRegistry::new();
		let connection_id = Uuid7::generate(&clock, &rng);
		let (tx1, _rx1) = mpsc::unbounded_channel();
		let (tx2, _rx2) = mpsc::unbounded_channel();

		let sub1 = SubscriptionId(12345);
		let sub2 = SubscriptionId(12346);
		registry.subscribe(sub1, connection_id, "FROM test1".to_string(), tx1, WireFormat::Json);
		registry.subscribe(sub2, connection_id, "FROM test2".to_string(), tx2, WireFormat::Json);
		assert_eq!(registry.subscription_count(), 2);

		registry.cleanup_connection(connection_id);
		assert_eq!(registry.subscription_count(), 0);
		assert_eq!(registry.connection_count(), 0);
	}

	#[tokio::test]
	async fn test_partial_unsubscribe() {
		let (_, clock, rng) = test_clock_and_rng();
		let registry = SubscriptionRegistry::new();
		let connection_id = Uuid7::generate(&clock, &rng);
		let (tx1, _rx1) = mpsc::unbounded_channel();
		let (tx2, _rx2) = mpsc::unbounded_channel();

		let sub1 = SubscriptionId(12345);
		let sub2 = SubscriptionId(12346);
		registry.subscribe(sub1, connection_id, "FROM test1".to_string(), tx1, WireFormat::Json);
		registry.subscribe(sub2, connection_id, "FROM test2".to_string(), tx2, WireFormat::Json);
		assert_eq!(registry.subscription_count(), 2);
		assert_eq!(registry.connection_count(), 1);

		assert!(registry.unsubscribe(sub1));
		assert_eq!(registry.subscription_count(), 1);
		assert_eq!(registry.connection_count(), 1);

		assert!(registry.unsubscribe(sub2));
		assert_eq!(registry.subscription_count(), 0);
		assert_eq!(registry.connection_count(), 0);
	}
}
