// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	mem,
};

use dashmap::DashMap;
use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_runtime::context::{clock::Clock, rng::Rng};
use reifydb_sub_server::{
	format::WireFormat,
	response::{CONTENT_TYPE_FRAMES, CONTENT_TYPE_JSON, resolve_response_json},
};
use reifydb_subscription::{
	batch::BatchId,
	delivery::{DeliveryResult, SubscriptionDelivery},
};
use reifydb_type::value::{frame::frame::Frame, uuid::Uuid7};
use reifydb_wire_format::{encode::encode_frames, json::to::convert_frames, options::EncodeOptions};
use serde_json::{Value as JsonValue, from_str, json};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::handler::{BinaryKind, encode_rbcf_envelope};

pub type ConnectionId = Uuid7;

#[derive(Debug, Clone)]
pub struct BatchChangeEntryPush {
	pub subscription_id: SubscriptionId,
	pub content_type: String,
	pub body: JsonValue,
}

#[derive(Debug, Clone)]
pub enum PushMessage {
	ChangeJson {
		subscription_id: SubscriptionId,
		content_type: String,
		body: JsonValue,
	},

	ChangeRbcf {
		subscription_id: SubscriptionId,
		envelope: Vec<u8>,
	},

	Closed {
		subscription_id: SubscriptionId,
	},

	BatchChangeJson {
		batch_id: BatchId,
		entries: Vec<BatchChangeEntryPush>,
	},

	BatchChangeRbcf {
		batch_id: BatchId,
		envelope: Vec<u8>,
	},

	BatchMemberClosed {
		batch_id: BatchId,
		subscription_id: SubscriptionId,
	},

	BatchClosed {
		batch_id: BatchId,
	},
}

struct SubscriptionState {
	connection_id: ConnectionId,
	push_tx: mpsc::UnboundedSender<PushMessage>,
	format: WireFormat,
	#[allow(dead_code)]
	query: String,

	batch_id: Option<BatchId>,
}

struct BatchState {
	connection_id: ConnectionId,
	push_tx: mpsc::UnboundedSender<PushMessage>,
	format: WireFormat,

	member_ids: Vec<SubscriptionId>,

	pending: DashMap<SubscriptionId, Vec<Frame>>,
}

pub struct SubscriptionRegistry {
	subscriptions: DashMap<SubscriptionId, SubscriptionState>,

	connections: DashMap<ConnectionId, Vec<SubscriptionId>>,

	batches: DashMap<BatchId, BatchState>,

	connection_batches: DashMap<ConnectionId, Vec<BatchId>>,
}

impl SubscriptionRegistry {
	pub fn new() -> Self {
		Self {
			subscriptions: DashMap::new(),
			connections: DashMap::new(),
			batches: DashMap::new(),
			connection_batches: DashMap::new(),
		}
	}

	pub fn subscribe(
		&self,
		subscription_id: SubscriptionId,
		connection_id: ConnectionId,
		query: String,
		push_tx: mpsc::UnboundedSender<PushMessage>,
		format: WireFormat,
	) {
		self.subscriptions.insert(
			subscription_id,
			SubscriptionState {
				connection_id,
				push_tx,
				format,
				query,
				batch_id: None,
			},
		);

		self.connections.entry(connection_id).or_default().push(subscription_id);

		debug!("Registered subscription {} for connection {}", subscription_id, connection_id);
	}

	pub fn register_batch(
		&self,
		connection_id: ConnectionId,
		member_ids: Vec<SubscriptionId>,
		push_tx: mpsc::UnboundedSender<PushMessage>,
		format: WireFormat,
		clock: &Clock,
		rng: &Rng,
	) -> BatchId {
		let batch_id = BatchId(Uuid7::generate(clock, rng));

		for member_id in &member_ids {
			if let Some(mut state) = self.subscriptions.get_mut(member_id) {
				state.batch_id = Some(batch_id);
			}
		}

		self.batches.insert(
			batch_id,
			BatchState {
				connection_id,
				push_tx,
				format,
				member_ids: member_ids.clone(),
				pending: DashMap::new(),
			},
		);

		self.connection_batches.entry(connection_id).or_default().push(batch_id);

		debug!(
			"Registered batch {} with {} members for connection {}",
			batch_id,
			member_ids.len(),
			connection_id
		);
		batch_id
	}

	pub fn unsubscribe_batch(&self, batch_id: BatchId) -> Option<Vec<SubscriptionId>> {
		let (_, state) = self.batches.remove(&batch_id)?;

		let connection_id = state.connection_id;
		let members = state.member_ids.clone();

		let mut removed_by_conn: HashMap<ConnectionId, HashSet<SubscriptionId>> = HashMap::new();
		for member_id in &members {
			if let Some((_, sub_state)) = self.subscriptions.remove(member_id) {
				removed_by_conn.entry(sub_state.connection_id).or_default().insert(*member_id);
			}
		}
		for (conn_id, removed) in removed_by_conn {
			if let Some(mut subs) = self.connections.get_mut(&conn_id) {
				subs.retain(|id| !removed.contains(id));
			}
		}

		let remove_connection =
			self.connections.get(&connection_id).map(|subs| subs.is_empty()).unwrap_or(false);
		if remove_connection {
			self.connections.remove(&connection_id);
		}

		let batches_empty = {
			if let Some(mut batches) = self.connection_batches.get_mut(&connection_id) {
				batches.retain(|id| *id != batch_id);
				batches.is_empty()
			} else {
				false
			}
		};
		if batches_empty {
			self.connection_batches.remove(&connection_id);
		}

		debug!("Unsubscribed batch {} ({} members)", batch_id, members.len());
		Some(members)
	}

	pub fn batch_for(&self, subscription_id: &SubscriptionId) -> Option<BatchId> {
		self.subscriptions.get(subscription_id).and_then(|state| state.batch_id)
	}

	pub fn batch_count(&self) -> usize {
		self.batches.len()
	}

	pub fn push_batch_frames(
		&self,
		batch_id: BatchId,
		subscription_id: SubscriptionId,
		frames: Vec<Frame>,
	) -> bool {
		let Some(batch) = self.batches.get(&batch_id) else {
			return false;
		};
		let mut entry = batch.pending.entry(subscription_id).or_default();
		for frame in frames {
			entry.push(frame);
		}
		true
	}

	pub fn emit_batch_member_closed(&self, batch_id: BatchId, subscription_id: SubscriptionId) -> bool {
		let Some(batch) = self.batches.get(&batch_id) else {
			return false;
		};
		batch.push_tx
			.send(PushMessage::BatchMemberClosed {
				batch_id,
				subscription_id,
			})
			.is_ok()
	}

	pub fn get_push_channel(&self, subscription_id: &SubscriptionId) -> Option<mpsc::UnboundedSender<PushMessage>> {
		self.subscriptions.get(subscription_id).map(|state| state.push_tx.clone())
	}

	pub fn get_push_target(
		&self,
		subscription_id: &SubscriptionId,
	) -> Option<(mpsc::UnboundedSender<PushMessage>, WireFormat)> {
		self.subscriptions.get(subscription_id).map(|state| (state.push_tx.clone(), state.format))
	}

	pub fn unsubscribe(&self, subscription_id: SubscriptionId) -> bool {
		if let Some((_, state)) = self.subscriptions.remove(&subscription_id) {
			let connection_id = state.connection_id;

			let should_remove_connection = {
				if let Some(mut subs) = self.connections.get_mut(&connection_id) {
					subs.retain(|id| *id != subscription_id);
					subs.is_empty()
				} else {
					false
				}
			};

			if should_remove_connection {
				self.connections.remove(&connection_id);
			}

			debug!("Unsubscribed subscription {}", subscription_id);
			true
		} else {
			false
		}
	}

	pub fn cleanup_connection(&self, connection_id: ConnectionId) -> Vec<SubscriptionId> {
		if let Some((_, batch_ids)) = self.connection_batches.remove(&connection_id) {
			for batch_id in &batch_ids {
				self.batches.remove(batch_id);
			}
		}

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

	pub async fn broadcast(&self, content_type: String, body: JsonValue) {
		for entry in self.subscriptions.iter() {
			let subscription_id = *entry.key();
			let state = entry.value();

			let msg = PushMessage::ChangeJson {
				subscription_id,
				content_type: content_type.clone(),
				body: body.clone(),
			};

			if let Err(e) = state.push_tx.send(msg) {
				warn!("Failed to push to subscription {}: {}", subscription_id, e);
			}
		}
	}

	#[allow(dead_code)]
	pub fn subscription_count(&self) -> usize {
		self.subscriptions.len()
	}

	#[allow(dead_code)]
	pub fn connection_count(&self) -> usize {
		self.connections.len()
	}

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
		if let Some(batch_id) = self.batch_for(subscription_id) {
			if let Some(batch) = self.batches.get(&batch_id) {
				batch.pending.entry(*subscription_id).or_default().push(Frame::from(columns));
				return DeliveryResult::Delivered;
			}

			return DeliveryResult::Disconnected;
		}

		let (push_tx, format) = match self.get_push_target(subscription_id) {
			Some(t) => t,
			None => return DeliveryResult::Disconnected,
		};

		let msg = match encode_change(*subscription_id, columns, format) {
			Some(msg) => msg,
			None => return DeliveryResult::Disconnected,
		};

		match push_tx.send(msg) {
			Ok(_) => DeliveryResult::Delivered,
			Err(_) => DeliveryResult::Disconnected,
		}
	}

	fn active_subscriptions(&self) -> Vec<SubscriptionId> {
		self.subscriptions.iter().map(|entry| *entry.key()).collect()
	}

	fn flush(&self) {
		let mut dead_batches: Vec<BatchId> = Vec::new();

		for entry in self.batches.iter() {
			let batch_id = *entry.key();
			let batch = entry.value();

			let taken: Vec<(SubscriptionId, Vec<Frame>)> = batch
				.pending
				.iter_mut()
				.filter_map(|mut e| {
					let v = mem::take(e.value_mut());
					if v.is_empty() {
						None
					} else {
						Some((*e.key(), v))
					}
				})
				.collect();
			if taken.is_empty() {
				continue;
			}

			let msg = match batch.format {
				WireFormat::Rbcf => {
					let mut entries: Vec<(String, Vec<u8>)> = Vec::with_capacity(taken.len());
					for (sub_id, frames) in taken {
						let rbcf_bytes = match encode_frames(&frames, &EncodeOptions::fast()) {
							Ok(b) => b,
							Err(e) => {
								warn!(
									"Failed to RBCF-encode batch entry for {}/{}: {}",
									batch_id, sub_id, e
								);
								continue;
							}
						};
						entries.push((sub_id.to_string(), rbcf_bytes));
					}
					if entries.is_empty() {
						continue;
					}
					let envelope = encode_rbcf_batch_envelope(&batch_id.to_string(), &entries);
					PushMessage::BatchChangeRbcf {
						batch_id,
						envelope,
					}
				}
				WireFormat::Frames => {
					let entries = taken
						.into_iter()
						.map(|(sub_id, frames)| {
							let body = json!({ "frames": convert_frames(&frames) });
							BatchChangeEntryPush {
								subscription_id: sub_id,
								content_type: CONTENT_TYPE_FRAMES.to_string(),
								body,
							}
						})
						.collect();
					PushMessage::BatchChangeJson {
						batch_id,
						entries,
					}
				}
				WireFormat::Json => {
					let entries = taken
						.into_iter()
						.filter_map(|(sub_id, frames)| {
							let resolved = match resolve_response_json(frames, false) {
								Ok(r) => r,
								Err(e) => {
									warn!(
										"Failed to JSON-encode batch entry for {}/{}: {}",
										batch_id, sub_id, e
									);
									return None;
								}
							};
							let body = from_str(&resolved.body)
								.unwrap_or(JsonValue::String(resolved.body));
							Some(BatchChangeEntryPush {
								subscription_id: sub_id,
								content_type: CONTENT_TYPE_JSON.to_string(),
								body,
							})
						})
						.collect::<Vec<_>>();
					if entries.is_empty() {
						continue;
					}
					PushMessage::BatchChangeJson {
						batch_id,
						entries,
					}
				}
			};

			if batch.push_tx.send(msg).is_err() {
				dead_batches.push(batch_id);
			}
		}

		for batch_id in dead_batches {
			if let Some(members) = self.unsubscribe_batch(batch_id) {
				debug!("Batch {} push channel closed; cascaded {} members", batch_id, members.len());
			}
		}
	}
}

fn encode_change(subscription_id: SubscriptionId, columns: Columns, format: WireFormat) -> Option<PushMessage> {
	match format {
		WireFormat::Rbcf => {
			let frames = vec![Frame::from(columns)];
			let rbcf_bytes = match encode_frames(&frames, &EncodeOptions::fast()) {
				Ok(b) => b,
				Err(e) => {
					warn!("Failed to RBCF-encode change for {}: {}", subscription_id, e);
					return None;
				}
			};
			let envelope = encode_rbcf_envelope(
				BinaryKind::Change,
				&subscription_id.to_string(),
				&rbcf_bytes,
				None,
			);
			Some(PushMessage::ChangeRbcf {
				subscription_id,
				envelope,
			})
		}
		WireFormat::Frames => {
			let body = json!({ "frames": convert_frames(&[Frame::from(columns)]) });
			Some(PushMessage::ChangeJson {
				subscription_id,
				content_type: CONTENT_TYPE_FRAMES.to_string(),
				body,
			})
		}
		WireFormat::Json => {
			let frames = vec![Frame::from(columns)];
			let resolved = match resolve_response_json(frames, false) {
				Ok(r) => r,
				Err(e) => {
					warn!("Failed to JSON-encode change for {}: {}", subscription_id, e);
					return None;
				}
			};
			let body = from_str(&resolved.body).unwrap_or(JsonValue::String(resolved.body));
			Some(PushMessage::ChangeJson {
				subscription_id,
				content_type: CONTENT_TYPE_JSON.to_string(),
				body,
			})
		}
	}
}

fn encode_rbcf_batch_envelope(batch_id: &str, entries: &[(String, Vec<u8>)]) -> Vec<u8> {
	let batch_id_bytes = batch_id.as_bytes();
	let mut total_entries_bytes = 0usize;
	for (sub_id, rbcf) in entries {
		total_entries_bytes += 4 + sub_id.len() + 4 + rbcf.len();
	}

	let mut envelope = Vec::with_capacity(1 + 4 + batch_id_bytes.len() + 4 + total_entries_bytes);
	envelope.push(BinaryKind::BatchChange as u8);
	envelope.extend_from_slice(&(batch_id_bytes.len() as u32).to_le_bytes());
	envelope.extend_from_slice(batch_id_bytes);
	envelope.extend_from_slice(&(entries.len() as u32).to_le_bytes());

	for (sub_id, rbcf) in entries {
		let sub_id_bytes = sub_id.as_bytes();
		envelope.extend_from_slice(&(sub_id_bytes.len() as u32).to_le_bytes());
		envelope.extend_from_slice(sub_id_bytes);
		envelope.extend_from_slice(&(rbcf.len() as u32).to_le_bytes());
		envelope.extend_from_slice(rbcf);
	}
	envelope
}

#[cfg(test)]
pub mod tests {
	use std::collections::HashSet;

	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};
	use reifydb_type::value::Value;

	use super::*;

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	fn single_int_columns(name: &str, value: i64) -> Columns {
		Columns::single_row([(name, Value::Int8(value))])
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
		registry.broadcast(CONTENT_TYPE_FRAMES.to_string(), body.clone()).await;

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
			other => panic!("Unexpected message: {:?}", other),
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

	#[tokio::test]
	async fn test_batch_flush_coalesces_two_members() {
		let (_, clock, rng) = test_clock_and_rng();
		let registry = SubscriptionRegistry::new();
		let connection_id = Uuid7::generate(&clock, &rng);
		let (push_tx, mut push_rx) = mpsc::unbounded_channel();

		let sub_a = SubscriptionId(1);
		let sub_b = SubscriptionId(2);

		registry.subscribe(sub_a, connection_id, "FROM a".to_string(), push_tx.clone(), WireFormat::Frames);
		registry.subscribe(sub_b, connection_id, "FROM b".to_string(), push_tx.clone(), WireFormat::Frames);

		let batch_id = registry.register_batch(
			connection_id,
			vec![sub_a, sub_b],
			push_tx.clone(),
			WireFormat::Frames,
			&clock,
			&rng,
		);
		assert_eq!(registry.batch_count(), 1);
		assert_eq!(registry.batch_for(&sub_a), Some(batch_id));
		assert_eq!(registry.batch_for(&sub_b), Some(batch_id));

		// Each try_deliver for batched members should not emit anything yet.
		assert!(matches!(
			registry.try_deliver(&sub_a, single_int_columns("value", 10)),
			DeliveryResult::Delivered
		));
		assert!(matches!(
			registry.try_deliver(&sub_b, single_int_columns("value", 20)),
			DeliveryResult::Delivered
		));

		// No push before flush.
		assert!(push_rx.try_recv().is_err());

		registry.flush();

		let msg = push_rx.try_recv().expect("expected one BatchChange after flush");
		// Exactly one envelope should be emitted.
		assert!(push_rx.try_recv().is_err());
		match msg {
			PushMessage::BatchChangeJson {
				batch_id: bid,
				entries,
			} => {
				assert_eq!(bid, batch_id);
				assert_eq!(entries.len(), 2);
				let ids: HashSet<SubscriptionId> = entries.iter().map(|e| e.subscription_id).collect();
				assert!(ids.contains(&sub_a));
				assert!(ids.contains(&sub_b));
			}
			other => panic!("expected BatchChangeJson, got {:?}", other),
		}
	}

	#[tokio::test]
	async fn test_batch_flush_merges_repeated_member_deliveries() {
		let (_, clock, rng) = test_clock_and_rng();
		let registry = SubscriptionRegistry::new();
		let connection_id = Uuid7::generate(&clock, &rng);
		let (push_tx, mut push_rx) = mpsc::unbounded_channel();

		let sub_a = SubscriptionId(100);
		registry.subscribe(sub_a, connection_id, "FROM a".to_string(), push_tx.clone(), WireFormat::Frames);
		let batch_id =
			registry.register_batch(connection_id, vec![sub_a], push_tx, WireFormat::Frames, &clock, &rng);

		// Two deliveries in one tick - should merge into one envelope entry with two frames.
		registry.try_deliver(&sub_a, single_int_columns("value", 1));
		registry.try_deliver(&sub_a, single_int_columns("value", 2));

		registry.flush();

		let msg = push_rx.try_recv().expect("envelope");
		match msg {
			PushMessage::BatchChangeJson {
				batch_id: bid,
				entries,
			} => {
				assert_eq!(bid, batch_id);
				assert_eq!(entries.len(), 1);
				assert_eq!(entries[0].subscription_id, sub_a);
				// `Frames` format body: object with `frames` array of length 2.
				let frames = entries[0].body.get("frames").expect("frames key").as_array().unwrap();
				assert_eq!(frames.len(), 2);
			}
			other => panic!("expected BatchChangeJson, got {:?}", other),
		}
	}

	#[tokio::test]
	async fn test_batch_flush_empty_tick_is_noop() {
		let (_, clock, rng) = test_clock_and_rng();
		let registry = SubscriptionRegistry::new();
		let connection_id = Uuid7::generate(&clock, &rng);
		let (push_tx, mut push_rx) = mpsc::unbounded_channel();
		let sub_a = SubscriptionId(77);
		registry.subscribe(sub_a, connection_id, "FROM a".to_string(), push_tx.clone(), WireFormat::Frames);
		registry.register_batch(connection_id, vec![sub_a], push_tx, WireFormat::Frames, &clock, &rng);

		registry.flush();
		assert!(push_rx.try_recv().is_err());
	}

	#[tokio::test]
	async fn test_batch_unsubscribe_cascades_members() {
		let (_, clock, rng) = test_clock_and_rng();
		let registry = SubscriptionRegistry::new();
		let connection_id = Uuid7::generate(&clock, &rng);
		let (push_tx, _push_rx) = mpsc::unbounded_channel();
		let sub_a = SubscriptionId(11);
		let sub_b = SubscriptionId(22);
		registry.subscribe(sub_a, connection_id, "FROM a".to_string(), push_tx.clone(), WireFormat::Frames);
		registry.subscribe(sub_b, connection_id, "FROM b".to_string(), push_tx.clone(), WireFormat::Frames);
		let batch_id = registry.register_batch(
			connection_id,
			vec![sub_a, sub_b],
			push_tx,
			WireFormat::Frames,
			&clock,
			&rng,
		);

		let removed = registry.unsubscribe_batch(batch_id).expect("batch existed");
		assert_eq!(removed.len(), 2);
		assert_eq!(registry.subscription_count(), 0);
		assert_eq!(registry.batch_count(), 0);
	}

	#[tokio::test]
	async fn test_batch_cleanup_on_connection_close() {
		let (_, clock, rng) = test_clock_and_rng();
		let registry = SubscriptionRegistry::new();
		let connection_id = Uuid7::generate(&clock, &rng);
		let (push_tx, _push_rx) = mpsc::unbounded_channel();
		let sub_a = SubscriptionId(31);
		registry.subscribe(sub_a, connection_id, "FROM a".to_string(), push_tx.clone(), WireFormat::Frames);
		registry.register_batch(connection_id, vec![sub_a], push_tx, WireFormat::Frames, &clock, &rng);

		let cleaned = registry.cleanup_connection(connection_id);
		assert_eq!(cleaned, vec![sub_a]);
		assert_eq!(registry.batch_count(), 0);
	}

	#[test]
	fn test_batch_id_display_fromstr_roundtrip() {
		let (_, clock, rng) = test_clock_and_rng();
		let id = BatchId(Uuid7::generate(&clock, &rng));
		let rendered = id.to_string();
		let parsed: BatchId = rendered.parse().expect("parse roundtrip");
		assert_eq!(id, parsed);
	}

	#[tokio::test]
	async fn test_batch_flush_cascades_on_dead_channel() {
		let (_, clock, rng) = test_clock_and_rng();
		let registry = SubscriptionRegistry::new();
		let connection_id = Uuid7::generate(&clock, &rng);
		let (push_tx, push_rx) = mpsc::unbounded_channel();
		let sub_a = SubscriptionId(55);
		registry.subscribe(sub_a, connection_id, "FROM a".to_string(), push_tx.clone(), WireFormat::Frames);
		let _batch_id =
			registry.register_batch(connection_id, vec![sub_a], push_tx, WireFormat::Frames, &clock, &rng);

		// Close the receiver side → push_tx.send() will fail during flush.
		drop(push_rx);

		registry.try_deliver(&sub_a, single_int_columns("value", 7));
		registry.flush();

		// Batch should be gone.
		assert_eq!(registry.batch_count(), 0);
	}
}
